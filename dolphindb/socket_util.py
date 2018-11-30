import select


def sendall(socket, msg, objs=b""):
    totalsent = 0
    MSGLEN = len(msg)
    while totalsent < MSGLEN:
        sent = socket.send(msg[totalsent:].encode())
        if sent == 0:
            raise RuntimeError("socket connection broken")
        totalsent = totalsent + sent

    if objs != b"":
        for obj in objs:
            # print(obj)
            sent = socket.send(obj)
            totalsent = totalsent + sent
    return totalsent

def recvall(socket, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = b''
    socket.setblocking(1)
    while len((data)) < n:
        # ready = select.select([socket], [], [])
        # if not ready[0]:
        #     return data
        packet = socket.recv(n - len((data)))
        if not packet:
            # print(2)
            break

        data += packet
    # print(len((data)))
    return data

def recvallhex(socket, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = ''
    socket.setblocking(1)
    while len(bytes.fromhex(data)) < n:
        # ready = select.select([socket], [], [], 1)
        # if not ready[0]:
        #     return data
        packet = socket.recv(n - len(bytes.fromhex(data)))
        if not packet:

            break

        data += packet.hex()
    return data


def readline(socket):
    data = ''
    while True:
        packet = socket.recv(1)
        if packet == b"":
            raise IOError("read empty byte")
        if b'\n' == packet:
            return data
        data += packet.decode('ascii')


def read_string(socket):
    # print("called")
    data = ''
    while True:
        packet = socket.recv(1)
        # print(packet)
        if '\x00' == packet.decode('ascii'):
            return data
        data += packet.decode('ascii')


