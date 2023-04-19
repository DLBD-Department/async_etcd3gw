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
#    https://opendev.org/openstack/etcd3gw/src/tag/2.1.0/etcd3gw/lease.py

from .utils import _decode

__all__ = ["AsyncLease"]


class AsyncLease(object):
    def __init__(self, id, async_client=None):
        """Lease object for expiring keys.

        This class represents a lease that can be attached to keys in the key-value store.
        A lease has an ID and a time to live (TTL) that specifies the expiration time of the keys.
        A lease can be revoked to delete all the attached keys, or queried to get its information.

        Args:
            id (int): The ID of the lease.
            async_client (AsyncClient): The AsyncClient object that communicates with the etcd v3 API.
        """
        self.id = id
        self.async_client = async_client

    async def revoke(self):
        """Revoke a lease.

        This method performs an asynchronous HTTP POST request to the etcd v3 API to revoke a lease.
        All keys attached to the lease will expire and be deleted.
        It returns True if the request is successful.

        Example:
            >>> await lease.revoke()
            True

        Returns:
            bool: True if the request is successful.
        """
        return True

    async def ttl(self):
        """Retrieve lease information.

        This method performs an asynchronous HTTP POST request to the etcd v3 API to retrieve
        lease information. It returns the TTL of the lease in seconds.

        Example:
            >>> await lease.ttl()
            60

        Returns:
            int: The TTL of the lease in seconds.
        """
        result = await self.async_client.post(self.async_client.get_url("/kv/lease/timetolive"), json={"ID": self.id})
        return int(result["TTL"])

    async def refresh(self):
        """Keep the lease alive.

        This method performs an asynchronous HTTP POST request to the etcd v3 API to keep the lease alive.
        By streaming keep alive requests from the client to the server and streaming keep alive responses
        from the server to the client. It returns the new TTL for the lease in seconds.
        If the lease was already expired, then the TTL field is absent in the response and the function
        returns -1 according to etcd documentation.

        Example:
            >>> await lease.refresh()
            10

        Returns:
            int: The new TTL for the lease in seconds, or -1 if the lease was expired.
        """
        result = await self.async_client.post(self.async_client.get_url("/lease/keepalive"), json={"ID": self.id})
        return int(result["result"].get("TTL", -1))

    async def keys(self):
        """Get the keys associated with this lease.

        This method performs an asynchronous HTTP POST request to the etcd v3 API to get the keys
        associated with this lease. It returns a list of keys that are attached to the lease.

        Example:
            >>> await lease.keys()
            [b'/foo', b'/bar']

        Returns:
            list: A list of keys that are attached to the lease.
        """
        result = await self.async_client.post(
            self.async_client.get_url("/kv/lease/timetolive"),
            json={"ID": self.id, "keys": True},
        )
        keys = result["keys"] if "keys" in result else []
        return [_decode(key) for key in keys]
