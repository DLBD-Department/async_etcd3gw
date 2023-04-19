# async-etcd3gw
An async etcd3 grpc-gateway v3 API Python client, derived from [etcd3gw](https://opendev.org/openstack/etcd3gw).

[![Build Status](https://github.com//DLBD-Department/async_etcd3gw/workflows/Tests/badge.svg)](https://github.com//DLBD-Department/async_etcd3gw/actions)
[![PyPI version](https://badge.fury.io/py/async-etcd3gw.svg)](https://badge.fury.io/py/async-etcd3gw)
[![PyPI](https://img.shields.io/pypi/pyversions/async-etcd3gw.svg)](https://pypi.org/project/async-etcd3gw)
[![License: Apache 2](https://img.shields.io/pypi/l/async-etcd3gw)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

### Library Installation

```
$ pip install async-etcd3gw
```

### Usage

You can find examples in examples folder.

Basic usage example:

```python
import asyncio
from async_etcd3gw import AsyncEtcd3Client

async def main():
    client = AsyncEtcd3Client(host='etcd', port=2379)

    # Put key
    await client.put(key='foo', value='bar')

    # Get key
    print("get key foo", await client.get(key='foo'))

    # Get all keys
    print("get all keys", await client.get_all())

    # Create lease and use it
    lease = await client.lease(ttl=100)
    await client.put(key='foo', value='bar', lease=lease)

    # Get lease keys
    print("get lease keys", await lease.keys())

    # Refresh lease
    await lease.refresh()

if __name__ == "__main__":
    asyncio.run(main())
```

### Links

* [etcd](https://etcd.io)
* [etcd gRPC API](https://etcd.io/docs/v3.5/learning/api/)
* [etcd3gw, An etcd3 grpc-gateway v3 API Python client](https://opendev.org/openstack/etcd3gw)
* [etcd gRPC gateway](https://etcd.io/docs/v3.5/dev-guide/api_grpc_gateway/)
* [AIOHTTP, Asynchronous HTTP Client/Server for asyncio and Python.](https://docs.aiohttp.org/en/stable/)
