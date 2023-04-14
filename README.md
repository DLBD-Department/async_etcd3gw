# async_etcd3gw
An async etcd3 grpc-gateway v3 API Python client, derived from [etcd3gw](https://opendev.org/openstack/etcd3gw).

### Library Installation

```
$ pip install async_etcd3gw
```

### Usage

You can find examples in examples folder.

Basic usage example:

```python
import asyncio
from async_etcd3gw.async_client import AsyncEtcd3Client

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

* [etcd3gw, An etcd3 grpc-gateway v3 API Python client](https://opendev.org/openstack/etcd3gw)
* [etcd gRPC gateway](https://etcd.io/docs/v3.5/dev-guide/api_grpc_gateway/)
* [AIOHTTP, Asynchronous HTTP Client/Server for asyncio and Python.](https://docs.aiohttp.org/en/stable/)
