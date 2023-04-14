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

import pytest

from async_etcd3gw import AsyncEtcd3Client


@pytest.mark.asyncio
async def test_client_default():
    client = AsyncEtcd3Client()
    assert "http://localhost:2379%slease/grant" % client.api_path == client.get_url("/lease/grant")


@pytest.mark.asyncio
async def test_client_ipv4():
    client = AsyncEtcd3Client(host="127.0.0.1")
    assert "http://127.0.0.1:2379%slease/grant" % client.api_path == client.get_url("/lease/grant")


@pytest.mark.asyncio
async def test_client_ipv6():
    client = AsyncEtcd3Client(host="::1")
    assert "http://[::1]:2379%slease/grant" % client.api_path == client.get_url("/lease/grant")


@pytest.mark.asyncio
async def test_client_api_path():
    client = AsyncEtcd3Client(host="127.0.0.1", api_path="/v3/")
    assert "http://127.0.0.1:2379/v3/lease/grant" == client.get_url("/lease/grant")
