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
#    https://opendev.org/openstack/etcd3gw/src/tag/2.1.0/etcd3gw/lock.py

import uuid

from .utils import DEFAULT_TIMEOUT, LOCK_PREFIX, _encode

__all__ = ["AsyncLock"]


class AsyncLock(object):
    def __init__(self, name, ttl=DEFAULT_TIMEOUT, async_client=None):
        """Create a lock using the given name with specified timeout

        This class represents a lock that can be acquired or released on a key in the key-value store.
        A lock has a name and a time to live (TTL) that specifies the expiration time of the lock.

        Args:
            name (str): The name of the lock.
            ttl (int): The timeout for the lock in seconds.
            async_client (AsyncClient): The async client object that communicates with the etcd v3 API.
        """
        self.name = name
        self.ttl = ttl
        self.async_client = async_client
        self.key = LOCK_PREFIX + self.name
        self.lease = None
        self._uuid = str(uuid.uuid1())

    @property
    def uuid(self):
        """The unique id of the lock"""
        return self._uuid

    async def acquire(self):
        """Acquire the lock."""
        self.lease = await self.async_client.lease(self.ttl)

        base64_key = _encode(self.key)
        base64_value = _encode(self._uuid)
        txn = {
            "compare": [
                {
                    "key": base64_key,
                    "result": "EQUAL",
                    "target": "CREATE",
                    "create_revision": 0,
                }
            ],
            "success": [
                {
                    "request_put": {
                        "key": base64_key,
                        "value": base64_value,
                        "lease": self.lease.id,
                    }
                }
            ],
            "failure": [{"request_range": {"key": base64_key}}],
        }
        result = await self.async_client.transaction(txn)
        if "succeeded" in result:
            return result["succeeded"]
        return False

    async def release(self):
        """Release the lock"""
        base64_key = _encode(self.key)
        base64_value = _encode(self._uuid)

        txn = {
            "compare": [
                {
                    "key": base64_key,
                    "result": "EQUAL",
                    "target": "VALUE",
                    "value": base64_value,
                }
            ],
            "success": [{"request_delete_range": {"key": base64_key}}],
        }

        result = await self.async_client.transaction(txn)
        if "succeeded" in result:
            return result["succeeded"]
        return False

    async def refresh(self):
        """Refresh the lease on the lock"""
        return await self.lease.refresh()

    async def is_acquired(self):
        """Check if the lock is acquired"""
        values = await self.async_client.get(self.key)
        return self._uuid.encode("latin-1") in values

    async def __aenter__(self):
        """Use the lock as a contextmanager"""
        await self.acquire()
        return self

    async def __aexit__(self, exception_type, exception_value, traceback):
        await self.release()
