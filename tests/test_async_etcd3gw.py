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
import uuid
from time import perf_counter

import pytest

from async_etcd3gw import exceptions, utils, AsyncEtcd3Client

ETCD_HOST = os.environ.get("ETCD_HOST", "localhost")


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_client_status():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    response = await client.status()
    assert response is not None
    assert "version" in response
    assert "header" in response
    assert "cluster_id" in response["header"]


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_client_members():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    response = await client.members()
    assert len(response) > 0
    assert "clientURLs" in response[0]
    assert "peerURLs" in response[0]


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_client_with_keys_and_values():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    assert await client.put("foo0", "bar0")
    assert await client.put("foo1", 2001)
    assert await client.put("foo2", b"bar2")

    assert [b"bar0"] == await client.get("foo0")
    assert [b"2001"] == await client.get("foo1")
    assert [b"bar2"] == await client.get("foo2")

    assert await client.delete("foo0")
    assert [] == await client.get("foo0")

    assert not await client.delete("foo0")
    assert len(await client.get_all()) > 0


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_get_and_delete_prefix():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    for i in range(20):
        await client.put(f"/doot1/range{i}", "i am a range")

    values = list(await client.get_prefix("/doot1/range"))
    assert len(values) == 20
    for value, metadata in values:
        assert b"i am a range" == value
        assert metadata["key"].startswith(b"/doot1/range")

    assert await client.delete_prefix("/doot1/range")
    values = list(await client.get_prefix("/doot1/range"))
    assert len(values) == 0


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_get_prefix_sort_order():
    client = AsyncEtcd3Client(host=ETCD_HOST)

    def remove_prefix(string, prefix):
        return string[len(prefix) :]

    initial_keys = "abcde"
    initial_values = "qwert"

    for k, v in zip(initial_keys, initial_values):
        await client.put("/doot2/{}".format(k), v)

    keys = b""
    for value, meta in await client.get_prefix("/doot2", sort_order="ascend"):
        keys += remove_prefix(meta["key"], "/doot2/")

    assert keys == initial_keys.encode("latin-1")

    reverse_keys = b""
    for value, meta in await client.get_prefix("/doot2", sort_order="descend"):
        reverse_keys += remove_prefix(meta["key"], "/doot2/")

    assert reverse_keys == "".join(reversed(initial_keys)).encode("latin-1")


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_get_prefix_sort_order_explicit_sort_target_key():
    client = AsyncEtcd3Client(host=ETCD_HOST)

    def remove_prefix(string, prefix):
        return string[len(prefix) :]

    initial_keys_ordered = "abcde"
    initial_keys = "aebdc"
    initial_values = "qwert"

    for k, v in zip(initial_keys, initial_values):
        await client.put("/doot2/{}".format(k), v)

    keys = b""
    for value, meta in await client.get_prefix("/doot2", sort_order="ascend", sort_target="key"):
        keys += remove_prefix(meta["key"], "/doot2/")

    assert keys == initial_keys_ordered.encode("latin-1")

    reverse_keys = b""
    for value, meta in await client.get_prefix("/doot2", sort_order="descend", sort_target="key"):
        reverse_keys += remove_prefix(meta["key"], "/doot2/")

    assert reverse_keys == "".join(reversed(initial_keys_ordered)).encode("latin-1")


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_get_prefix_sort_order_explicit_sort_target_rev():
    client = AsyncEtcd3Client(host=ETCD_HOST)

    def remove_prefix(string, prefix):
        return string[len(prefix) :]

    initial_keys = "aebdc"
    initial_values = "qwert"

    for k, v in zip(initial_keys, initial_values):
        await client.put("/expsortmod/{}".format(k), v)

    keys = b""
    for value, meta in await client.get_prefix("/expsortmod", sort_order="ascend", sort_target="mod"):
        keys += remove_prefix(meta["key"], "/expsortmod/")

    assert keys == initial_keys.encode("latin-1")

    reverse_keys = b""
    for value, meta in await client.get_prefix("/expsortmod", sort_order="descend", sort_target="mod"):
        reverse_keys += remove_prefix(meta["key"], "/expsortmod/")

    assert reverse_keys == "".join(reversed(initial_keys)).encode("latin-1")


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_replace_success():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    key = "/doot/thing" + str(uuid.uuid4())
    await client.put(key, "toot")
    status = await client.replace(key, "toot", "doot")
    v = await client.get(key)
    assert [b"doot"] == v
    assert status


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_replace_fail():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    key = "/doot/thing" + str(uuid.uuid4())
    await client.put(key, "boot")
    status = await client.replace(key, "toot", "doot")
    v = await client.get(key)
    assert [b"boot"] == v
    assert not status


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_client_lease():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    lease = await client.lease(ttl=60)
    assert lease is not None

    ttl = await lease.ttl()
    assert 0 <= ttl <= 60

    keys = await lease.keys()
    assert [] == keys

    ttl = await lease.refresh()
    assert 0 <= ttl <= 60

    assert await lease.revoke()


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_client_lease_with_keys():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    lease = await client.lease(ttl=60)
    assert lease is not None

    assert await client.put("foo12", "bar12", lease)
    assert await client.put("foo13", "bar13", lease)

    keys = await lease.keys()
    assert 2 == len(keys)
    assert b"foo12" in keys
    assert b"foo13" in keys

    assert [b"bar12"] == await client.get("foo12")
    assert [b"bar13"] == await client.get("foo13")

    assert await lease.revoke()


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_watch_key():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    key = "/%s-watch_key/watch" % str(uuid.uuid4())

    async def update_etcd(v):
        await client.put(key, v)
        out = await client.get(key)
        assert [v.encode("latin-1")] == out

    async def update_key():
        # sleep to make watch can get the event
        await asyncio.sleep(0.5)
        await update_etcd("0")
        await asyncio.sleep(0.1)
        await update_etcd("1")
        await asyncio.sleep(0.1)
        await update_etcd("2")
        await asyncio.sleep(0.1)
        await update_etcd("3")
        await asyncio.sleep(0.1)

    t = asyncio.create_task(update_key())

    change_count = 0
    events_iterator, cancel = await client.watch(key)
    async for event in events_iterator:
        assert event["kv"]["key"] == key.encode("latin-1")
        assert event["kv"]["value"] == str(change_count).encode("latin-1")

        # if cancel worked, we should not receive event 3
        assert event["kv"]["value"] != b"3"

        change_count += 1
        if change_count > 2:
            # if cancel not work, we will block in this for-loop forever
            await cancel()

    await t


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_watch_prefix():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    key = "/%s-watch_prefix/watch/prefix/" % str(uuid.uuid4())

    async def update_etcd(v):
        await client.put(key + v, v)
        out = await client.get(key + v)
        assert [v.encode("latin-1")] == out

    async def update_key():
        # sleep to make watch can get the event
        await asyncio.sleep(0.5)
        await update_etcd("0")
        await asyncio.sleep(0.1)
        await update_etcd("1")
        await asyncio.sleep(0.1)
        await update_etcd("2")
        await asyncio.sleep(0.1)
        await update_etcd("3")
        await asyncio.sleep(0.1)

    t = asyncio.create_task(update_key())

    change_count = 0
    events_iterator, cancel = await client.watch_prefix(key)
    async for event in events_iterator:
        if not event["kv"]["key"].startswith(key.encode("latin-1")):
            continue
        assert event["kv"]["key"] == ("%s%s" % (key, change_count)).encode("latin-1")
        assert event["kv"]["value"] == str(change_count).encode("latin-1")

        # if cancel worked, we should not receive event 3
        assert event["kv"]["value"] != b"3"

        change_count += 1
        if change_count > 2:
            # if cancel not work, we will block in this for-loop forever
            await cancel()

    await t


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_sequential_watch_prefix_once():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    try:
        await client.watch_prefix_once("/doot/", 1)
    except exceptions.WatchTimedOut:
        pass
    try:
        await client.watch_prefix_once("/doot/", 1)
    except exceptions.WatchTimedOut:
        pass
    try:
        await client.watch_prefix_once("/doot/", 1)
    except exceptions.WatchTimedOut:
        pass


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_client_lock_acquire_release():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    async with await client.lock(ttl=60) as lock:
        ttl = await lock.refresh()
        assert 0 <= ttl <= 60
    assert not await lock.is_acquired()

    async with await client.lock(ttl=60) as lock:
        assert not await lock.acquire()


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_client_locks():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    lock = await client.lock(id="xyz-%s" % perf_counter(), ttl=60)
    assert lock is not None

    assert await lock.acquire()
    assert lock.uuid is not None

    ttl = await lock.refresh()
    assert 0 <= ttl <= 60

    assert await lock.is_acquired()
    assert await lock.release()
    assert not await lock.release()
    assert not await lock.is_acquired()


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_create_success():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    key = "/foo/unique" + str(uuid.uuid4())
    # Verify that key is empty
    assert [] == await client.get(key)

    status = await client.create(key, "bar")
    # Verify that key is 'bar'
    assert [b"bar"] == await client.get(key)
    assert status


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_create_fail():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    key = "/foo/" + str(uuid.uuid4())
    # Assign value to the key
    await client.put(key, "bar")
    assert [b"bar"] == await client.get(key)

    status = await client.create(key, "goo")
    # Verify that key is still 'bar'
    assert [b"bar"] == await client.get(key)
    assert not status


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_create_with_lease_success():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    key = "/foo/unique" + str(uuid.uuid4())
    # Verify that key is empty
    assert [] == await client.get(key)
    lease = await client.lease()

    status = await client.create(key, "bar", lease=lease)
    # Verify that key is 'bar'
    assert [b"bar"] == await client.get(key)
    assert status
    keys = await lease.keys()
    assert 1 == len(keys)
    assert key.encode("latin-1") in keys


async def _post_key(client, key_name, provide_value=True):
    payload = {"key": utils._encode(key_name)}
    if provide_value:
        payload["value"] = utils._encode(key_name)
    await client.post(client.get_url("/kv/put"), json=payload)


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_client_keys_with_metadata_and_value():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    test_key_value = b"some_key"
    await _post_key(client, test_key_value)
    result = await client.get(test_key_value, metadata=True)
    assert len(result) > 0
    value, metadata = result[0]
    assert value == test_key_value


@pytest.mark.etcd
@pytest.mark.asyncio
async def test_client_keys_with_metadata_and_no_value():
    client = AsyncEtcd3Client(host=ETCD_HOST)
    value_is_not_set_default = b""
    test_key = b"some_key"
    await _post_key(client, test_key, provide_value=False)
    result = await client.get(test_key, metadata=True)
    assert len(result) > 0
    value, metadata = result[0]
    assert value == value_is_not_set_default
