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

from async_etcd3gw.async_client import DEFAULT_API_PATH, AsyncEtcd3Client


async def generator(async_client, n):
    for i in range(0, n):
        print(i)
        await async_client.put("/foo", i)
        await asyncio.sleep(0.1)


async def main():
    etcd_host = os.environ.get("ETCD_HOST", "localhost")
    etcd_port = os.environ.get("ETCD_PORT", "2379")
    api_path = os.environ.get("API_PATH", DEFAULT_API_PATH)
    async with AsyncEtcd3Client(host=etcd_host, port=etcd_port, api_path=api_path) as async_client:
        n = 10 * 10
        await generator(async_client, n)


if __name__ == "__main__":
    asyncio.run(main())
