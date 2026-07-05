import asyncio, socket
import runloom.aio as aio
async def main():
    loop=asyncio.get_event_loop()
    tr,p=await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=('127.0.0.1',0))
    print('isinstance DatagramTransport:', isinstance(tr, asyncio.DatagramTransport))
    print('isinstance BaseTransport:', isinstance(tr, asyncio.BaseTransport))
    try:
        tr.abort()
        print('abort ok')
    except AttributeError as e:
        print('abort ->', repr(e)); tr.close()
    tr2,_=await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=('127.0.0.1',0), reuse_address=True)
    s=tr2.get_extra_info('socket')
    print('reuse_address=True accepted; SO_REUSEADDR =', s.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR))
    tr2.close()
aio.run(main())
