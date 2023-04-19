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
    """Encode the given data using base-64.

    This function takes a data as an argument and encodes it using base-64.
    It returns a base-64 encoded string. If the data is not a bytes object,
    it converts it to a bytes object using latin-1 encoding.

    Example:
        >>> _encode("hello")
        'aGVsbG8='

    Args:
        data (bytes or str): The data to encode.

    Returns:
        str: The base-64 encoded string.
    """
    if not isinstance(data, bytes_types):
        data = str(data).encode("latin-1")
    return base64.b64encode(data).decode("utf-8")


def _decode(data):
    """Decode the base-64 encoded string.

    This function takes a data as an argument and decodes it using base-64.
    It returns the decoded data. If the data is not a bytes object,
    it converts it to a bytes object using latin-1 encoding.

    Example:
        >>> _decode('aGVsbG8=')
        b'hello'

    Args:
        data (bytes or str): The base-64 encoded string.

    Returns:
        bytes: The decoded data.
    """
    if not isinstance(data, bytes_types):
        data = str(data).encode("latin-1")
    return base64.b64decode(data.decode("utf-8"))


def _increment_last_byte(data):
    """Get the last byte in the array and increment it.

    This function takes a data as an argument and increments the last byte in the
    array by one. It returns the modified data. If the data is not a bytes object,
    it converts it to a bytes object using utf-8 or latin-1 encoding.

    Example:
        >>> _increment_last_byte(b'foo')
        b'fop'

    Args:
        data (bytes or str): The data to increment.

    Returns:
        bytes: The modified data.
    """
    if not isinstance(data, bytes_types):
        if isinstance(data, str):
            data = data.encode("utf-8")
        else:
            data = str(data).encode("latin-1")
    s = bytearray(data)
    s[-1] = s[-1] + 1
    return bytes(s)
