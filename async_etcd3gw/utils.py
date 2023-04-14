#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
#    Derived from:
#    https://opendev.org/openstack/etcd3gw/src/tag/2.1.0/etcd3gw/utils.py

import base64

__all__ = [
    "DEFAULT_TIMEOUT",
    "LOCK_PREFIX",
    "_encode",
    "_decode",
    "_increment_last_byte",
]

DEFAULT_TIMEOUT = 30
LOCK_PREFIX = "/locks/"
bytes_types = (bytes, bytearray)


def _encode(data):
    """Encode the given data using base-64

    :param data:
    :return: base-64 encoded string
    """
    if not isinstance(data, bytes_types):
        data = str(data).encode("latin-1")
    return base64.b64encode(data).decode("utf-8")


def _decode(data):
    """Decode the base-64 encoded string

    :param data:
    :return: decoded data
    """
    if not isinstance(data, bytes_types):
        data = str(data).encode("latin-1")
    return base64.b64decode(data.decode("utf-8"))


def _increment_last_byte(data):
    """Get the last byte in the array and increment it

    :param bytes_string:
    :return:
    """
    if not isinstance(data, bytes_types):
        if isinstance(data, str):
            data = data.encode("utf-8")
        else:
            data = str(data).encode("latin-1")
    s = bytearray(data)
    s[-1] = s[-1] + 1
    return bytes(s)
