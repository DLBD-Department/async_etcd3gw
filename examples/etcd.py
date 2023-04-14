# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import asyncio
import os
from time import perf_counter

from async_etcd3gw.async_client import AsyncEtcd3Client
from async_etcd3gw.async_lock import AsyncLock


async def main():
    etcd_host = os.environ.get("ETCD_HOST", "localhost")
    async_client = AsyncEtcd3Client(host=etcd_host)

    print(">>>> Status")
    result = await async_client.status()
    print("cluster id : %r" % result["header"]["cluster_id"])

    result = await async_client.members()
    print("first member info : %r" % result[0])

    print(">>>> Lease")
    lease = await async_client.lease()
    print("Lease id : %r" % lease.id)
    print("Lease ttl : %r" % lease.ttl())
    print("Lease refresh : %r" % lease.refresh())

    result = await async_client.put("foo2", "bar2", lease)
    print("Key put foo2 : %r" % result)
    result = await async_client.put("foo3", "bar3", lease)
    print("Key put foo3 : %r" % result)
    print("Lease Keys : %r" % lease.keys())

    result = await lease.revoke()
    print("Lease Revoke : %r" % result)

    result = await async_client.get("foox")
    print("Key get foox : %r" % result)

    result = await async_client.put("foo", "bar")
    print("Key put foo : %r" % result)
    result = await async_client.get("foo")
    print("Key get foo : %r" % result)
    result = await async_client.delete("foo")
    print("Key delete foo : %r" % result)
    result = await async_client.delete("foo-unknown")
    print("Key delete foo-unknown : %r" % result)

    print(">>>> Lock")
    lock = AsyncLock("xyz-%s" % perf_counter(), ttl=10000, async_client=async_client)
    result = await lock.acquire()
    print("acquire : %r" % result)
    result = await lock.refresh()
    print("refresh : %r" % result)
    result = await lock.is_acquired()
    print("is_acquired : %r" % result)
    result = await lock.release()
    print("release : %r" % result)
    result = await lock.is_acquired()
    print("is_acquired : %r" % result)


if __name__ == "__main__":
    asyncio.run(main())
