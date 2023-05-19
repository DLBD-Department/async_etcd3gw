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
#    https://opendev.org/openstack/etcd3gw/src/tag/2.1.0/etcd3gw/watch.py

import asyncio
import json

import aiohttp

from .exceptions import get_exception
from .utils import _decode, _encode

__all__ = ["AsyncWatcher"]


class AsyncWatcher(object):
    KW_ARGS = ["start_revision", "progress_notify", "filters", "prev_kv"]
    KW_ENCODED_ARGS = ["range_end"]

    def __init__(self, async_client, key, callback, **kwargs):
        self.async_client = async_client
        self.callback = callback
        create_watch = {"key": _encode(key)}
        for arg in kwargs:
            if arg in self.KW_ARGS:
                create_watch[arg] = kwargs[arg]
            elif arg in self.KW_ENCODED_ARGS:
                create_watch[arg] = _encode(kwargs[arg])
        self.create_request = {"create_request": create_watch}

    async def start(self):
        "Start the watch task"
        self.event = asyncio.Event()
        self.exception = None
        self.watch_task = asyncio.create_task(self.watch())
        await self.event.wait()
        if self.exception is not None:
            raise self.exception

    async def stop(self):
        "Request the watch task to be cancelled"
        self.watch_task.cancel()
        try:
            await self.watch_task
        except asyncio.CancelledError:
            pass

    async def process_chunk(self, line):
        "Process a chuck"
        decoded_line = line.decode("utf-8")
        try:
            payload = json.loads(decoded_line)
        except json.JSONDecodeError as ex:
            raise ex
        if "error" in payload:
            raise get_exception(
                payload["error"].get("http_code", 500), payload["error"].get("http_status", ""), payload["error"].get("message", "")
            )
        if "created" in payload["result"]:
            if not payload["result"]["created"]:
                raise Exception("Unable to create watch")
        if "events" in payload["result"]:
            for event in payload["result"]["events"]:
                event["kv"]["key"] = _decode(event["kv"]["key"])
                if "value" in event["kv"]:
                    event["kv"]["value"] = _decode(event["kv"]["value"])
                await self.callback(event)

    async def read_and_process_chunk(self, content):
        while True:
            if content._exception is not None:
                raise content._exception

            if not content._buffer and not content._eof:
                await content._wait("readany")

            (data, end_of_http_chunk) = await content.readchunk()
            while not end_of_http_chunk:
                (more_data, end_of_http_chunk) = await content.readchunk()
                data += more_data

            await self.process_chunk(data)

    async def watch(self):
        try:
            timeout = aiohttp.ClientTimeout(total=None, sock_read=None)
            async with self.async_client.session.post(
                self.async_client.get_url("/watch"), json=self.create_request, timeout=timeout
            ) as resp:
                ex = get_exception(resp.status, resp.text, resp.reason)
                if ex:
                    raise ex
                self.event.set()
                await self.read_and_process_chunk(resp.content)
        except Exception as ex:
            self.exception = ex
            self.event.set()
            await self.callback(ex)
