import asyncio
async def main():
    loop=asyncio.get_event_loop()
    tr,p=await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=('127.0.0.1',0))
    print('stock isinstance:', isinstance(tr, asyncio.DatagramTransport))
    tr.abort(); print('stock abort ok')
    try:
        tr2,_=await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=('127.0.0.1',0), reuse_address=True)
        print('stock accepted reuse_address=True'); tr2.close()
    except ValueError as e:
        print('stock reuse_address=True ->', repr(e))
asyncio.run(main())
