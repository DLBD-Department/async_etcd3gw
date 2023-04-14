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
        """Lease object for expiring keys
        :param id:
        :param async_client:
        """
        self.id = id
        self.async_client = async_client

    async def revoke(self):
        """LeaseRevoke revokes a lease.
        All keys attached to the lease will expire and be deleted.
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please define a `callback` function
        to be invoked when receiving the response.
        :return:
        """
        await self.async_client.post(self.async_client.get_url("/kv/lease/revoke"), json={"ID": self.id})
        return True

    async def ttl(self):
        """LeaseTimeToLive retrieves lease information.
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please define a `callback` function
        to be invoked when receiving the response.
        :return:
        """
        result = await self.async_client.post(self.async_client.get_url("/kv/lease/timetolive"), json={"ID": self.id})
        return int(result["TTL"])

    async def refresh(self):
        """LeaseKeepAlive keeps the lease alive
        By streaming keep alive requests from the client to the server and
        streaming keep alive responses from the server to the client.
        This method makes a synchronous HTTP request by default.
        :return: returns new TTL for lease. If lease was already expired then
            TTL field is absent in response and the function returns -1
            according to etcd documentation.
            https://etcd.io/docs/v3.5/dev-guide/apispec/swagger/rpc.swagger.json
        """
        result = await self.async_client.post(self.async_client.get_url("/lease/keepalive"), json={"ID": self.id})
        return int(result["result"].get("TTL", -1))

    async def keys(self):
        """Get the keys associated with this lease.
        :return:
        """
        result = await self.async_client.post(
            self.async_client.get_url("/kv/lease/timetolive"),
            json={"ID": self.id, "keys": True},
        )
        keys = result["keys"] if "keys" in result else []
        return [_decode(key) for key in keys]
