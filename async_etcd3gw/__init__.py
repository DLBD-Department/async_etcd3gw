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
#    https://opendev.org/openstack/etcd3gw/src/tag/2.1.0/etcd3gw/__init__.py

from . import utils
from .async_client import AsyncEtcd3Client, async_client
from .async_lease import AsyncLease
from .async_lock import AsyncLock

__version__ = "0.4"

__all__ = (
    "AsyncEtcd3Client",
    "AsyncLease",
    "AsyncLock",
    "async_client",
    "utils",
)
