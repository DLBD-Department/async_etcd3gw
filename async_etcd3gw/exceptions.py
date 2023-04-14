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
#    https://opendev.org/openstack/etcd3gw/src/tag/2.1.0/etcd3gw/exceptions.py

__all__ = [
    "AsyncEtcd3Exception",
    "WatchTimedOut",
    "InternalServerError",
    "ConnectionFailedError",
    "ConnectionTimeoutError",
    "PreconditionFailedError",
    "_EXCEPTIONS_BY_CODE",
]


class AsyncEtcd3Exception(Exception):
    def __init__(self, detail_text=None, *args):
        super().__init__(*args)
        self.detail_text = detail_text


class WatchTimedOut(AsyncEtcd3Exception):
    pass


class InternalServerError(AsyncEtcd3Exception):
    pass


class ConnectionFailedError(AsyncEtcd3Exception):
    pass


class ConnectionTimeoutError(AsyncEtcd3Exception):
    pass


class PreconditionFailedError(AsyncEtcd3Exception):
    pass


_EXCEPTIONS_BY_CODE = {
    500: InternalServerError,
    503: ConnectionFailedError,
    408: ConnectionTimeoutError,
    504: ConnectionTimeoutError,
    412: PreconditionFailedError,
}
