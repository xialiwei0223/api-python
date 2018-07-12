import select


def sendall(socket, msg):
    totalsent = 0
    MSGLEN = len(msg)
    while totalsent < MSGLEN:
        sent = socket.send(msg[totalsent:])
        if sent == 0:
            raise RuntimeError("socket connection broken")
        totalsent = totalsent + sent
    return totalsent

def recvall(socket, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = ''
    while len(data) < n:
        ready = select.select([socket], [], [], 1)
        if not ready[0]:
            return data
        packet = socket.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data


def readline(socket):
    data = ''
    while True:
        packet = socket.recv(1)
        if packet == "":
            raise IOError("read empty byte")
        if '\n' == packet:
            return data
        data += packet



def read_string(socket):
    data = ''
    while True:
        packet = socket.recv(1)
        if '\x00' == packet:
            return data
        data += packet

