def sendall(socket, msg, objs=b""):
    totalsent = 0
    MSGLEN = len(msg)
    while totalsent < MSGLEN:
        sent = socket.send(msg[totalsent:].encode('utf-8'))
        if sent == 0:
            raise RuntimeError("socket connection broken")
        totalsent = totalsent + sent

    if objs != b"":
        for obj in objs:
            # print(obj)
            sent = socket.send(obj)
            totalsent = totalsent + sent
    return totalsent


_rcv_buffer_size = 32768


def recvall(socket, n, bufferList):
    socket.setblocking(1)
    buffer = bufferList[0]
    if len(buffer) >= n:
        ret = buffer[:n]
        bufferList[0] = buffer[n:]
        return ret
    while len(buffer) < n:
        packet = socket.recv(_rcv_buffer_size)
        if not packet:
            break
        buffer += packet

    if len(buffer) >= n:
        ret = buffer[:n]
        bufferList[0] = buffer[n:]
        return ret
    else:
        ret = buffer
        bufferList[0] = b''
        return ret


def recvallhex(socket, n, bufferList):
    socket.setblocking(1)
    buffer = bufferList[0]
    if len(buffer) >= n:
        ret = buffer[:n].hex()
        bufferList[0] = buffer[n:]
        return ret

    while len(buffer) < n:
        packet = socket.recv(_rcv_buffer_size)
        if not packet:
            break
        buffer += packet

    if len(buffer) >= n:
        ret = buffer[:n]
        bufferList[0] = buffer[n:]
        return ret
    else:
        ret = buffer
        bufferList[0] = b''
        return ret


def readline(socket, bufferList):
    start = 0
    buffer = bufferList[0]
    while True:
        pos = buffer.find(b'\n', start)
        if pos != -1:
            ret = buffer[:pos]
            bufferList[0] = buffer[pos+1:]
            return ret.decode('utf-8')
        start = len(buffer)
        packet = socket.recv(_rcv_buffer_size)
        if packet == b'':
            raise IOError("read empty byte")
        buffer += packet


def read_string(socket, bufferList):
    start = 0
    buffer = bufferList[0]
    while True:
        pos = buffer.find(b'\x00', start)
        if pos != -1:
            ret = buffer[:pos]
            bufferList[0] = buffer[pos+1:]
            return ret.decode('utf-8')
        start = len(buffer)
        packet = socket.recv(_rcv_buffer_size)
        if packet == b'':
            raise IOError("read empty byte")
        buffer += packet

