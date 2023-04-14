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
#    https://opendev.org/openstack/etcd3gw/src/tag/2.1.0/etcd3gw/client.py

import asyncio
import json
import ssl
import threading
import uuid

import aiohttp

from .async_lease import AsyncLease
from .async_lock import AsyncLock
from .async_watch import AsyncWatcher
from .exceptions import _EXCEPTIONS_BY_CODE, AsyncEtcd3Exception, ConnectionFailedError, ConnectionTimeoutError, WatchTimedOut
from .utils import DEFAULT_TIMEOUT, _decode, _encode, _increment_last_byte

__all__ = [
    "AsyncEtcd3Client",
    "async_client",
    "DEFAULT_API_PATH",
    "DEFAULT_PORT",
    "DEFAULT_PROTOCOL",
]

_SORT_ORDER = ["none", "ascend", "descend"]
_SORT_TARGET = ["key", "version", "create", "mod", "value"]
DEFAULT_API_PATH = "/v3alpha/"
DEFAULT_PORT = 2379
DEFAULT_PROTOCOL = "http"

# gRPC gateway endpoints:
#
# - etcd v3.2 or before uses only [CLIENT-URL]/v3alpha/*
# - etcd v3.3 uses [CLIENT-URL]/v3beta/* while keeping [CLIENT-URL]/v3alpha/*
# - etcd v3.4 uses [CLIENT-URL]/v3/* while keeping [CLIENT-URL]/v3beta/*
#                  [CLIENT-URL]/v3alpha/* is deprecated
# - etcd v3.5 or later uses only [CLIENT-URL]/v3/*
#                  [CLIENT-URL]/v3beta/* is deprecated


class AsyncEtcd3Client(object):
    def __init__(
        self,
        host="localhost",
        port=DEFAULT_PORT,
        protocol=DEFAULT_PROTOCOL,
        ca_cert=None,
        cert_key=None,
        cert_cert=None,
        timeout=None,
        api_path=DEFAULT_API_PATH,
    ):
        """Construct an client to talk to etcd3's grpc-gateway's /v3 HTTP API

        :param host:
        :param port:
        :param protocol:
        """
        self.host = host
        self.port = port
        self.protocol = protocol
        self.api_path = api_path

        if timeout is not None:
            self.timeout = aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout)
        else:
            self.timeout = None

        if ca_cert is not None:
            ssl_ctx = ssl.create_default_context(cafile=ca_cert)
            if cert_cert is not None and cert_key is not None:
                ssl_ctx.load_cert_chain(cert_cert, cert_key)
            self.connector = aiohttp.TCPConnector(ssl_context=ssl_ctx)
        elif cert_cert is not None and cert_key is not None:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.load_cert_chain(cert_cert, cert_key)
            self.connector = aiohttp.TCPConnector(ssl_context=ssl_ctx)
        else:
            self.connector = None

        self.client_session_kwargs = {}
        if self.timeout is not None:
            self.client_session_kwargs["timeout"] = self.timeout
        if self.connector is not None:
            self.client_session_kwargs["connector"] = self.connector

    def get_url(self, path):
        """Construct a full url to the v3 API given a specific path

        :param path:
        :return: url
        """
        host = f"[{ self.host }]" if ":" in self.host else self.host
        base_url = f"{ self.protocol }://{ host }:{ self.port }"
        return base_url + self.api_path + path.lstrip("/")

    async def post(self, *args, **kwargs):
        """helper method for HTTP POST

        :param args:
        :param kwargs:
        :return: json response
        """
        try:
            async with aiohttp.ClientSession(**self.client_session_kwargs) as session:
                async with session.post(*args, **kwargs) as resp:
                    if resp.status in _EXCEPTIONS_BY_CODE:
                        raise _EXCEPTIONS_BY_CODE[resp.status](resp.text, resp.reason)
                    if resp.status != 200:
                        raise AsyncEtcd3Exception(resp.text, resp.reason)
                    return await resp.json()
        except asyncio.TimeoutError as ex:
            raise ConnectionTimeoutError(str(ex))
        except aiohttp.ClientConnectionError as ex:
            raise ConnectionFailedError(str(ex))

    async def status(self):
        """Status gets the status of the etcd cluster member.

        :return: json response
        """
        return await self.post(self.get_url("/maintenance/status"), json={})

    async def members(self):
        """Lists all the members in the cluster.

        :return: json response
        """
        result = await self.post(self.get_url("/cluster/member/list"), json={})
        return result["members"]

    async def lease(self, ttl=DEFAULT_TIMEOUT):
        """Create a AsyncLease object given a timeout

        :param ttl: timeout
        :return: AsyncLease object
        """
        result = await self.post(self.get_url("/lease/grant"), json={"TTL": ttl, "ID": 0})
        return AsyncLease(int(result["ID"]), async_client=self)

    async def lock(self, id=None, ttl=DEFAULT_TIMEOUT):
        """Create a Lock object given an ID and timeout

        :param id: ID for the lock, creates a new uuid if not provided
        :param ttl: timeout
        :return: Lock object
        """
        if id is None:
            id = str(uuid.uuid4())
        return AsyncLock(id, ttl=ttl, async_client=self)

    async def create(self, key, value, lease=None):
        """Atomically create the given key only if the key doesn't exist.

        This verifies that the create_revision of a key equales to 0, then
        creates the key with the value.
        This operation takes place in a transaction.

        :param key: key in etcd to create
        :param value: value of the key
        :type value: bytes or string
        :param lease: lease to connect with, optional
        :returns: status of transaction, ``True`` if the create was
                  successful, ``False`` otherwise
        :rtype: bool
        """
        base64_key = _encode(key)
        base64_value = _encode(value)
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
                    }
                }
            ],
            "failure": [],
        }
        if lease:
            txn["success"][0]["request_put"]["lease"] = lease.id
        result = await self.transaction(txn)
        if "succeeded" in result:
            return result["succeeded"]
        return False

    async def put(self, key, value, lease=None):
        """Put puts the given key into the key-value store.

        A put request increments the revision of the key-value store
        and generates one event in the event history.

        :param key:
        :param value:
        :param lease:
        :return: boolean
        """
        payload = {"key": _encode(key), "value": _encode(value)}
        if lease:
            payload["lease"] = lease.id
        await self.post(self.get_url("/kv/put"), json=payload)
        return True

    async def get(self, key, metadata=False, sort_order=None, sort_target=None, **kwargs):
        """Range gets the keys in the range from the key-value store.

        :param key:
        :param metadata:
        :param sort_order: 'ascend' or 'descend' or None
        :param sort_target: 'key' or 'version' or 'create' or 'mod' or 'value'
        :param kwargs:
        :return:
        """
        try:
            order = 0
            if sort_order:
                order = _SORT_ORDER.index(sort_order)
        except ValueError:
            raise ValueError('sort_order must be one of "ascend" or "descend"')

        try:
            target = 0
            if sort_target:
                target = _SORT_TARGET.index(sort_target)
        except ValueError:
            raise ValueError('sort_target must be one of "key", ' '"version", "create", "mod" or "value"')

        payload = {
            "key": _encode(key),
            "sort_order": order,
            "sort_target": target,
        }
        payload.update(kwargs)
        result = await self.post(self.get_url("/kv/range"), json=payload)
        if "kvs" not in result:
            return []

        if metadata:

            def value_with_metadata(item):
                item["key"] = _decode(item["key"])
                value = _decode(item.pop("value", ""))
                return value, item

            return [value_with_metadata(item) for item in result["kvs"]]

        return [_decode(item.get("value", "")) for item in result["kvs"]]

    async def get_all(self, sort_order=None, sort_target="key"):
        """Get all keys currently stored in etcd.

        :returns: sequence of (value, metadata) tuples
        """
        return await self.get(
            key=_encode(b"\0"),
            metadata=True,
            sort_order=sort_order,
            sort_target=sort_target,
            range_end=_encode(b"\0"),
        )

    async def get_prefix(self, key_prefix, sort_order=None, sort_target=None):
        """Get a range of keys with a prefix.

        :param sort_order: 'ascend' or 'descend' or None
        :param key_prefix: first key in range

        :returns: sequence of (value, metadata) tuples
        """
        return await self.get(
            key_prefix,
            metadata=True,
            range_end=_encode(_increment_last_byte(key_prefix)),
            sort_order=sort_order,
            sort_target=sort_target,
        )

    async def replace(self, key, initial_value, new_value):
        """Atomically replace the value of a key with a new value.

        This compares the current value of a key, then replaces it with a new
        value if it is equal to a specified value. This operation takes place
        in a transaction.

        :param key: key in etcd to replace
        :param initial_value: old value to replace
        :type initial_value: bytes or string
        :param new_value: new value of the key
        :type new_value: bytes or string
        :returns: status of transaction, ``True`` if the replace was
                  successful, ``False`` otherwise
        :rtype: bool
        """
        base64_key = _encode(key)
        base64_initial_value = _encode(initial_value)
        base64_new_value = _encode(new_value)
        txn = {
            "compare": [
                {
                    "key": base64_key,
                    "result": "EQUAL",
                    "target": "VALUE",
                    "value": base64_initial_value,
                }
            ],
            "success": [
                {
                    "request_put": {
                        "key": base64_key,
                        "value": base64_new_value,
                    }
                }
            ],
            "failure": [],
        }
        result = await self.transaction(txn)
        if "succeeded" in result:
            return result["succeeded"]
        return False

    async def delete(self, key, **kwargs):
        """DeleteRange deletes the given range from the key-value store.

        A delete request increments the revision of the key-value store and
        generates a delete event in the event history for every deleted key.

        :param key:
        :param kwargs:
        :return:
        """
        payload = {
            "key": _encode(key),
        }
        payload.update(kwargs)

        result = await self.post(self.get_url("/kv/deleterange"), json=payload)
        if "deleted" in result:
            return True
        return False

    async def delete_prefix(self, key_prefix):
        """Delete a range of keys with a prefix in etcd."""
        return await self.delete(key_prefix, range_end=_encode(_increment_last_byte(key_prefix)))

    async def transaction(self, txn):
        """Txn processes multiple requests in a single transaction.

        A txn request increments the revision of the key-value store and
        generates events with the same revision for every completed request.
        It is not allowed to modify the same key several times within one txn.

        :param txn:
        :return:
        """
        return await self.post(self.get_url("/kv/txn"), data=json.dumps(txn))

    async def watch(self, key, **kwargs):
        """Watch a key.

        :param key: key to watch

        :returns: tuple of ``events_iterator`` and ``cancel``.
                  Use ``events_iterator`` to get the events of key changes
                  and ``cancel`` to cancel the watch request
        """
        event_queue = asyncio.Queue()

        async def callback(event):
            await event_queue.put(event)

        w = AsyncWatcher(async_client=self, key=key, callback=callback, **kwargs)
        await w.start()
        canceled = threading.Event()

        async def cancel():
            canceled.set()
            await event_queue.put(None)
            await w.stop()

        async def iterator():
            while not canceled.is_set():
                event = await event_queue.get()
                if event is None:
                    canceled.set()
                if not canceled.is_set():
                    yield event

        return iterator(), cancel

    async def watch_prefix(self, key_prefix, **kwargs):
        """The same as ``watch``, but watches a range of keys with a prefix."""
        kwargs["range_end"] = _increment_last_byte(key_prefix)
        return await self.watch(key_prefix, **kwargs)

    async def watch_once(self, key, timeout=None, **kwargs):
        """Watch a key and stops after the first event.

        :param key: key to watch
        :param timeout: (optional) timeout in seconds.
        :returns: event
        """
        event_queue = asyncio.Queue()

        async def callback(event):
            await event_queue.put(event)

        w = AsyncWatcher(async_client=self, key=key, callback=callback, **kwargs)
        await w.start()
        try:
            return await asyncio.wait_for(event_queue.get(), timeout=timeout)
        except asyncio.exceptions.TimeoutError:
            raise WatchTimedOut()
        finally:
            await w.stop()

    async def watch_prefix_once(self, key_prefix, timeout=None, **kwargs):
        """Watches a range of keys with a prefix, similar to watch_once"""
        kwargs["range_end"] = _increment_last_byte(key_prefix)
        return await self.watch_once(key_prefix, timeout=timeout, **kwargs)


def async_client(
    host="localhost",
    port=2379,
    ca_cert=None,
    cert_key=None,
    cert_cert=None,
    timeout=None,
    protocol="http",
    api_path=DEFAULT_API_PATH,
):
    """Return an instance of AsyncEtcd3Client"""
    return AsyncEtcd3Client(
        host=host,
        port=port,
        ca_cert=ca_cert,
        cert_key=cert_key,
        cert_cert=cert_cert,
        timeout=timeout,
        api_path=api_path,
        protocol=protocol,
    )
