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
        self.watch_task = asyncio.create_task(self.watch())

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
        except Exception as ex:
            raise ex
        if "created" in payload["result"]:
            if not payload["result"]["created"]:
                raise Exception("Unable to create watch")
        if "events" in payload["result"]:
            for event in payload["result"]["events"]:
                event["kv"]["key"] = _decode(event["kv"]["key"])
                if "value" in event["kv"]:
                    event["kv"]["value"] = _decode(event["kv"]["value"])
                await self.callback(event)

    async def watch(self):
        async with aiohttp.ClientSession(**self.async_client.client_session_kwargs) as session:
            async with session.post(self.async_client.get_url("/watch"), json=self.create_request) as resp:
                async for data in resp.content.iter_any():
                    await self.process_chunk(data)
