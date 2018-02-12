# -*- python -*-
# oVirt or RHV upload nbdkit plugin used by ‘virt-v2v -o rhv-upload’
# Copyright (C) 2018 Red Hat Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import builtins
import json
import logging
import ovirtsdk4 as sdk
import ovirtsdk4.types as types
import ssl
import sys
import time

from http.client import HTTPSConnection
from urllib.parse import urlparse

# Timeout to wait for oVirt disks to change status, or the transfer
# object to finish initializing [seconds].
timeout = 5*60

# Parameters are passed in via a JSON doc from the OCaml code.
# Because this Python code ships embedded inside virt-v2v there
# is no formal API here.
params = None

def config(key, value):
    global params

    if key == "params":
        with builtins.open(value, 'r') as fp:
            params = json.load(fp)
    else:
        raise RuntimeError("unknown configuration key '%s'" % key)

def config_complete():
    if params is None:
        raise RuntimeError("missing configuration parameters")

def open(readonly):
    # Parse out the username from the output_conn URL.
    parsed = urlparse(params['output_conn'])
    username = parsed.username or "admin@internal"

    # Read the password from file.
    with builtins.open(params['output_password'], 'r') as fp:
        password = fp.read()
    password = password.rstrip()

    # Connect to the server.
    connection = sdk.Connection(
        url = params['output_conn'],
        username = username,
        password = password,
        ca_file = params['rhv_cafile'],
        log = logging.getLogger(),
        insecure = params['insecure'],
    )

    system_service = connection.system_service()

    # Create the disk.
    disks_service = system_service.disks_service()
    if params['disk_format'] == "raw":
        disk_format = types.DiskFormat.RAW
    else:
        disk_format = types.DiskFormat.COW
    disk = disks_service.add(
        disk = types.Disk(
            name = params['disk_name'],
            description = "Uploaded by virt-v2v",
            format = disk_format,
            provisioned_size = params['disk_size'],
            sparse = params['output_sparse'],
            storage_domains = [
                types.StorageDomain(
                    name = params['output_storage'],
                )
            ],
        )
    )

    # Wait till the disk is up, as the transfer can't start if the
    # disk is locked:
    disk_service = disks_service.disk_service(disk.id)

    endt = time.time() + timeout
    while True:
        time.sleep(5)
        disk = disk_service.get()
        if disk.status == types.DiskStatus.OK:
            break
        if time.time() > endt:
            raise RuntimeError("timed out waiting for disk to become unlocked")

    # Get a reference to the transfer service.
    transfers_service = system_service.image_transfers_service()

    # Create a new image transfer.
    transfer = transfers_service.add(
        types.ImageTransfer(
            image = types.Image(
                id = disk.id
            )
        )
    )

    # Get a reference to the created transfer service.
    transfer_service = transfers_service.image_transfer_service(transfer.id)

    # After adding a new transfer for the disk, the transfer's status
    # will be INITIALIZING.  Wait until the init phase is over. The
    # actual transfer can start when its status is "Transferring".
    endt = time.time() + timeout
    while True:
        time.sleep(5)
        transfer = transfer_service.get()
        if transfer.phase != types.ImageTransferPhase.INITIALIZING:
            break
        if time.time() > endt:
            raise RuntimeError("timed out waiting for transfer status != INITIALIZING")

    # Now we have permission to start the transfer.
    if params['rhv_direct']:
        if transfer.transfer_url is None:
            raise RuntimeError("direct upload to host not supported, requires ovirt-engine >= 4.2 and only works when virt-v2v is run within the oVirt/RHV environment, eg. on an ovirt node.")
        destination_url = urlparse(transfer.transfer_url)
    else:
        destination_url = urlparse(transfer.proxy_url)

    context = ssl.create_default_context()
    context.load_verify_locations(cafile = params['rhv_cafile'])

    http = HTTPSConnection(
        destination_url.hostname,
        destination_url.port,
        context = context
    )

    # Save everything we need to make requests in the handle.
    return {
        'connection': connection,
        'disk': disk,
        'disk_service': disk_service,
        'failed': False,
        'highestwrite': 0,
        'http': http,
        'path': destination_url.path,
        'transfer': transfer,
        'transfer_service': transfer_service,
    }

def get_size(h):
    return params['disk_size']

# For examples of working code to read/write from the server, see:
# https://github.com/oVirt/ovirt-imageio/blob/master/daemon/test/server_test.py

def pread(h, count, offset):
    http = h['http']
    transfer=h['transfer']
    transfer_service=h['transfer_service']

    http.putrequest("GET", h['path'])
    http.putheader("Authorization", transfer.signed_ticket)
    http.putheader("Range", "bytes=%d-%d" % (offset, offset+count-1))
    http.endheaders()

    r = http.getresponse()
    # 206 = HTTP Partial Content, which is the usual response.
    if r.status != 200 and r.status != 206:
        h['transfer_service'].pause()
        h['failed'] = True
        raise RuntimeError("could not read sector (%d, %d): %d: %s" %
                           (offset, count, r.status, r.reason))
    return r.read()

def pwrite(h, buf, offset):
    count = len(buf)
    h['highestwrite'] = max(h['highestwrite'], offset+count)
    do_pwrite(h, buf, offset, count)

def do_pwrite(h, buf, offset, count):
    http = h['http']
    transfer=h['transfer']
    transfer_service=h['transfer_service']

    http.putrequest("PUT", h['path'])
    http.putheader("Authorization", transfer.signed_ticket)
    # The oVirt server only uses the first part of the range, and the
    # content-length.
    http.putheader("Content-Range", "bytes %d-%d/*" % (offset, offset+count-1))
    http.putheader("Content-Length", str(count))
    http.endheaders()
    http.send(buf)

    r = http.getresponse()
    if r.status != 200:
        transfer_service.pause()
        h['failed'] = True
        raise RuntimeError("could not write sector (%d, %d): %d: %s" %
                           (offset, count, r.status, r.reason))

# qemu-img convert starts by trying to zero/trim the whole device.
# Since we've just created a new disk it's safe to ignore these
# requests as long as they are smaller than the highest write seen.
# After that we must emulate them with writes.
def zero(h, count, offset, may_trim):
    if offset+count < h['highestwrite']:
        # count could be very large, so split into chunks.
        while count > 0:
            n = min(count, 65536)
            buf = bytearray(n)
            do_pwrite(h, buf, offset, n)
            offset += n
            count -= n

def close(h):
    http = h['http']
    connection = h['connection']

    http.close()

    # If we didn't fail, then finalize the transfer.
    if not h['failed']:
        disk = h['disk']
        transfer_service=h['transfer_service']

        transfer_service.finalize()

        # Wait until the transfer disk job is completed since
        # only then we can be sure the disk is unlocked.  As this
        # code is not very clear, what's happening is that we are
        # waiting for the transfer object to cease to exist, which
        # falls through to the exception case and then we can
        # continue.
        endt = time.time() + timeout
        try:
            while True:
                time.sleep(1)
                tmp = transfer_service.get()
                if time.time() > endt:
                    raise RuntimeError("timed out waiting for transfer to finalize")
        except sdk.NotFoundError:
            pass

        # Write the disk ID file.  Only do this on successful completion.
        with builtins.open(params['diskid_file'], 'w') as fp:
            fp.write(disk.id)

    # Otherwise if we did fail then we should delete the disk.
    else:
        disk_service = h['disk_service']
        disk_service.remove()

    connection.close()
