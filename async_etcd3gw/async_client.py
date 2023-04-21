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
DEFAULT_API_PATH = "/v3/"
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

        Args:
            host (str): The etcd hostname.
            port (int): The etcd port.
            protocol (str): The etcd protocol (http/https).
            timeout (int): The default timeout.
            api_path (str): The api path.
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

        This method takes a path as an argument and returns a url that points to the v3 API of the host.

        Args:
            path (str): The path to append to the base url.

        Returns:
            str: The full url to the v3 API with the given path.
        """
        host = f"[{ self.host }]" if ":" in self.host else self.host
        base_url = f"{ self.protocol }://{ host }:{ self.port }"
        return base_url + self.api_path + path.lstrip("/")

    async def post(self, *args, **kwargs):
        """Helper method for HTTP POST

        This method performs an asynchronous HTTP POST request using aiohttp.
        It takes arbitrary arguments and keyword arguments that are passed to the session.post method.
        It returns a json response if the request is successful, or raises an exception if the request
        fails or times out.

        Example:
            >>> self.client_session_kwargs = {"timeout": 10}
            >>> await self.post("https://example.com/api", data={"key": "value"})
            {'result': 'ok'}

        Args:
            *args: Positional arguments for the session.post method.
            **kwargs: Keyword arguments for the session.post method.

        Returns:
            dict: The json response from the server.

        Raises:
            AsyncEtcd3Exception: If the status code is not 200 and not in the predefined exceptions.
            ConnectionTimeoutError: If the request times out.
            ConnectionFailedError: If the connection fails.
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

        Returns:
            dict: The json response from the server.
        """
        return await self.post(self.get_url("/maintenance/status"), json={})

    async def members(self):
        """Lists all the members in the cluster.

        Returns:
            dict: The json response from the server.
        """
        result = await self.post(self.get_url("/cluster/member/list"), json={})
        return result["members"]

    async def lease(self, ttl=DEFAULT_TIMEOUT):
        """Create a AsyncLease object given a timeout

        This method performs an post request to the etcd v3 API to create a lease
        with a specified time to live (TTL). It takes a TTL as an argument,
        and returns an AsyncLease object that represents the lease.
        The lease can be used to attach keys to it, keep it alive, or revoke it.

        Example:
            >>> lease = await self.lease(ttl=10)
            >>> await self.put("/foo", "bar", lease=lease)
            True

        Args:
            ttl (int): The timeout for the lease in seconds.

        Returns:
            AsyncLease: The lease object.

        """
        result = await self.post(self.get_url("/lease/grant"), json={"TTL": ttl, "ID": 0})
        return AsyncLease(int(result["ID"]), async_client=self)

    async def lock(self, id=None, ttl=DEFAULT_TIMEOUT):
        """Create a Lock object given an ID and timeout

        This method creates a lock with a specified ID and TTL.
        It takes an ID and a TTL as arguments, and returns an AsyncLock object that represents the lock.
        The lock can be used to acquire or release the lock on a key.

        Example:
            >>> lock = await self.lock(id="mylock", ttl=10)
            >>> await lock.acquire("foo")
            True

        Args:
            id (str): The ID for the lock. Creates a new UUID if not provided.
            ttl (int): The timeout for the lock in seconds.

        Returns:
            AsyncLock: The lock object.
        """
        if id is None:
            id = str(uuid.uuid4())
        return AsyncLock(id, ttl=ttl, async_client=self)

    async def create(self, key, value, lease=None):
        """Atomically create the given key only if the key doesn't exist.

        This verifies that the create_revision of a key equales to 0, then
        creates the key with the value.
        This operation takes place in a transaction.

        Example:
            >>> await self.create("/foo", "bar")
            True
            >>> await self.create("/foo", "baz")
            False

        Args:
            key (str): The key to create.
            value (str): The value to store.
            lease (Lease): The lease object that specifies the expiration time of the key.

        Returns:
            bool: True if the create was successful, False otherwise.
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

    async def put(self, key, value, lease=None, metadata=False):
        """Put puts the given key into the key-value store.

        This method performs an asynchronous HTTP POST request to the etcd v3 API to store
        a key-value pair. It takes a key and a value as arguments, and optionally a lease object
        that specifies the expiration time of the key.
        A put request increments the revision of the key-value store
        and generates one event in the event history.

        Example:
            >>> await self.put("/foo", "bar")
            True

        Args:
            key (str): The key to store.
            value (str): The value to store.
            lease (Lease): The lease object that specifies the expiration time of the key.
            metadata (bool): Whether to return the metadata.

        Returns:
            bool, dict: If metadata is True, the json response from the server else True if the put was successful.
        """
        payload = {"key": _encode(key), "value": _encode(value)}
        if lease:
            payload["lease"] = lease.id
        result = await self.post(self.get_url("/kv/put"), json=payload)
        if metadata:
            return result
        else:
            return True

    async def get(self, key, metadata=False, sort_order=None, sort_target=None, **kwargs):
        """Range gets the keys in the range from the key-value store.

         This method performs an asynchronous HTTP POST request to the etcd v3 API to retrieve
         a range of keys from the key-value store.

        Example:
            >>> await self.get("/foo")
            [b'bar']
            >>> await self.get("/foo", metadata=True)
            [(b'bar', {'key': 'foo', 'create_revision': 1, 'mod_revision': 1, 'version': 1})]
            >>> await self.get("/foo", sort_order="descend", sort_target="version")
            [b'baz', b'bar']

        Args:
            key (str): The key to query.
            metadata (bool): Whether to return the metadata of the keys.
            sort_order (str): The order of the keys. One of "ascend" or "descend" or None.
            sort_target (str): The criteria for sorting. One of "key", "version", "create", "mod" or "value".
            **kwargs: Other keyword arguments for the request payload.

        Returns:
            list: A list of values or a list of tuples of value and metadata.
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

        Returns:
            list: A sequence of (value, metadata) tuples
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

        Args:
            key_prefix (str): The prefix to query.
            sort_order (str): The order of the keys. One of "ascend" or "descend" or None.
            sort_target (str): The criteria for sorting. One of "key", "version", "create", "mod" or "value".

        Returns:
            list: A sequence of (value, metadata) tuples
        """
        return await self.get(
            key_prefix,
            metadata=True,
            range_end=_encode(_increment_last_byte(key_prefix)),
            sort_order=sort_order,
            sort_target=sort_target,
        )

    async def replace(self, key, initial_value, new_value, metadata=False):
        """Atomically replace the value of a key with a new value.

        This compares the current value of a key, then replaces it with a new
        value if it is equal to a specified value. This operation takes place
        in a transaction.

        Example:
            >>> await self.replace("/foo", "bar", "baz")
            True
            >>> await self.replace("/foo", "bar", "qux")
            False

        Args:
            key (str): The key to replace.
            initial_value (str): The old value to compare.
            new_value (str): The new value to swap.
            metadata (bool): Whether to return the metadata.

        Returns:
            bool, dict: If metadata is True, the json response from the server else True if the replace was successful.
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
        if metadata:
            return result
        elif "succeeded" in result:
            return result["succeeded"]
        return False

    async def delete(self, key, metadata=False, **kwargs):
        """DeleteRange deletes the given range from the key-value store.

        A delete request increments the revision of the key-value store and
        generates a delete event in the event history for every deleted key.

        Args:
            key (str): The key to delete.
            metadata (bool): Whether to return the metadata.

        Returns:
            bool, dict: If metadata is True, the json response from the server else True if the delete was successful.
        """
        payload = {
            "key": _encode(key),
        }
        payload.update(kwargs)

        result = await self.post(self.get_url("/kv/deleterange"), json=payload)
        if metadata:
            return result
        else:
            return "deleted" in result

    async def delete_prefix(self, key_prefix, metadata=False):
        """Delete a range of keys with a prefix in etcd."""
        return await self.delete(key_prefix, range_end=_encode(_increment_last_byte(key_prefix)), metadata=metadata)

    async def transaction(self, txn):
        """Txn processes multiple requests in a single transaction.

        A txn request increments the revision of the key-value store and
        generates events with the same revision for every completed request.
        It is not allowed to modify the same key several times within one txn.

        Args:
            txn: The json request payload.
        Returns:
            dict: The json response from the server.
        """
        return await self.post(self.get_url("/kv/txn"), data=json.dumps(txn))

    async def watch(self, key, **kwargs):
        """Watch a key.

        This method creates a watch request to the etcd v3 API to monitor a key for changes.
        It returns a tuple of events_iterator and cancel.
        Use events_iterator to get the events of key changes and cancel to cancel the watch request.

        Args:
            key (str): The key to watch.
            **kwargs: Other keyword arguments for the watch request.

        Returns:
            tuple: A tuple of events_iterator and cancel.
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

        Args:
            key (str): The key to watch.
            timeout (int): The timeout in seconds. Optional.
            **kwargs: Other keyword arguments for the watch request.

        Returns:
            dict: The first event of the key change.

        Raises:
            WatchTimedOut: If the timeout is reached.
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
