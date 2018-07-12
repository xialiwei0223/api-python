import re
import socket
import traceback

from dolphindb import data_factory, socket_util
from dolphindb.data_factory import *
from dolphindb.settings import *
from dolphindb.type_util import determine_form_type
from dolphindb.pair import Pair
from dolphindb.table import Table

class session(object):
    """
    xxdb api class
    connect: initiate socket connection
    run: execute xxdb script and return corresponding python objects
    1: Scalar variable returns a python scalar
    2: Vector object returns numpy array
    3: Table object returns  a pandas data frame
    4: Matrix object returns a pandas data frame
    """
    def __init__(self, host=None, port=None):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port
        self.sessionID = None
        self.remoteLittleEndian = None
        if self.host is not None and self.port is not None:
            self.connect(host, port)

    def connect(self, host, port):
        self.host = host
        self.port = port
        body = "connect\n"
        msg = "API 0 "+str(len(body))+'\n'+body
        try:
            self.socket.connect((host, port))
            sent = socket_util.sendall(self.socket, msg)
            if sent:
                header = socket_util.readline(self.socket)

                headers = header.split()
                if len(headers) != 3:
                    raise Exception('Header Length Incorrect', header)
                sid, _ , is_remote = headers
                msg = socket_util.readline(self.socket)
                if msg != 'OK':
                    raise Exception('Connection failed', msg)
                self.sessionID = sid
                self.remoteLittleEndian = False if is_remote == '0' else True
                data_factory.endianness = '>'.__add__ if is_remote == '0' else  '<'.__add__
        except Exception, e:
            traceback.print_exc(e)
            return False
        return True

    def _reconnect(self, message):
        try:
            print ("socket connection was lost; attemping to reconnect to %s : %s\n" % (self.host, self.port))
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            socket_util.sendall(self.socket, message)
            print ("socket is reconnected\n")
        except Exception:
            print ("socket reconnection has failed!\n")
            raise
        return True

    def close(self):
        self.socket.close()
        self.host = None
        self.port = None
        self.sessionID = None
        self.remoteLittleEndian = None

    def upload(self, nameObjectDict):
        if not isinstance(nameObjectDict, dict) or not nameObjectDict:
            print '\n Empty name/object mapping received - no upload\n'
            return None
        if not self.sessionID:
            raise Exception('Connection has not been established yet; please call function connect!')

        """upload variable names """
        body = "variable\n"
        objects = []
        for key in nameObjectDict:
            if not isinstance(key, str):
                raise ValueError("variable name" + key + " is not a string")
            if re.match(r"[^a-zA-Z]", key) or re.search(r"[^_a-zA-Z0-9]", key):
                raise ValueError("variable name" + key + " is not valid")
            body += key + ","
            objects.append(nameObjectDict[key])
        body = body[:-1]

        """upload objects"""
        body += "\n" + str(len(objects)) + "\n"
        body += "1" if self.remoteLittleEndian else "0"
        message = "API " + self.sessionID + " " + str(len(body)) + '\n' + body
        for o in objects:
            message = self.write_python_obj(o, message)
        reconnected = False
        try:
            totalsent = socket_util.sendall(self.socket, message)
        except IOError, e:
            traceback.print_exc(e)
            reconnected = self._reconnect(message)

        """msg receive"""
        header = socket_util.readline(self.socket)
        headers = header.split()
        if len(headers) != 3:
            raise Exception('Header Length Incorrect', header)
        if reconnected:
            if not self.sessionID == headers[0]:
                print ("old sessionID %s is invalid after reconnection; new sessionID is %s\n" % (self.sessionID, headers[0]))
                self.sessionID = headers[0]
        msg = socket_util.readline(self.socket)
        if msg != 'OK':
            raise Exception('Server Exception', msg)
        return None

    def run(self, script, *args):

        if not script or len(script.strip()) == 0:
            raise Exception('Empty Script Received', script)
        if not self.sessionID:
            raise Exception('Connection has not been established yet; please call function connect!')

        """msg send"""
        if len(args):
            """ function with arguments"""
            body = "function\n" + script
            body += "\n" + str(len(args)) + "\n";
            body += "1" if self.remoteLittleEndian else "0"
            message = "API " + self.sessionID + " " + str(len(body)) + '\n' + body
            for arg in args:
                message = self.write_python_obj(arg, message)
        else:
            """pure script"""
            body = "script\n"+script
            message = "API "+self.sessionID + " " + str(len(body)) + '\n' + body

        #print(message)

        reconnected = False
        try:
            socket_util.sendall(self.socket, message)
        except IOError:
            reconnected = self.reconnect(message)

        """msg receive"""
        header = socket_util.readline(self.socket)
        while header == "MSG":
            socket_util.read_string(self.socket) # python API doesn't support progress listener
            header = socket_util.readline(self.socket)

        headers = header.split()
        if len(headers) != 3:
            raise Exception('Header Length Incorrect', header)
        if reconnected:
            if not self.sessionID == headers[0]:
                print ("old sessionID %s is invalid after reconnection; new sessionID is %s\n" % (self.sessionID, headers[0]))
                self.sessionID = headers[0]

        sid, obj_num, _ = headers
        msg = socket_util.readline(self.socket)
        if msg != 'OK':
            raise Exception('Server Exception', msg)
        if int(obj_num) == 0:
            return None
        return self.read_xxdb_obj

    def table(self, data, dbPath=None):
        return Table(data=data, dbPath=dbPath, s=self)

    @property
    def read_xxdb_obj(self):
       return read_xxdb_obj_general(self.socket)

    def write_python_obj(self, obj, message):
        (dbForm, dbType) = determine_form_type(obj)

        #special handling of numpy datetime64[ns]
        #internally we don't have this type
        #we use nanotimestamp for it
        #however, packing value is different from other datatypes
        #so we handle it seperately
        if dbType == 100:
            flag = (dbForm << 8) + DT_NANOTIMESTAMP
        else:
            flag = (dbForm << 8) + dbType
        message += (DATA_PACKER_SCALAR[DT_SHORT](flag))

        if dbType == DT_ANY and isinstance(obj, list): # any vector written
            message += DATA_PACKER_SCALAR[DT_INT](len(obj))
            message += DATA_PACKER_SCALAR[DT_INT](1)
            for val in obj:
                message = self.write_python_obj(val, message)
        elif isinstance(obj, list):  # vector written from list
            message += DATA_PACKER_SCALAR[DT_INT](len(obj))
            message += DATA_PACKER_SCALAR[DT_INT](1)
            for val in obj:
                message += DATA_PACKER_SCALAR[dbType](val)
        elif isinstance(obj, dict):
            message = self.write_python_obj(obj.keys(), message)
            message = self.write_python_obj(obj.values(), message)
        elif isinstance(obj, np.ndarray) and dbForm == DF_VECTOR: # vector written from numpy array
            message += DATA_PACKER_SCALAR[DT_INT](obj.size)
            message += DATA_PACKER_SCALAR[DT_INT](1)
            message += DATA_PACKER[dbType](obj)
        elif isinstance(obj, Pair):
            message += DATA_PACKER_SCALAR[DT_INT](2)
            message += DATA_PACKER_SCALAR[DT_INT](1)
            message += DATA_PACKER_SCALAR[dbType](obj.a)
            message += DATA_PACKER_SCALAR[dbType](obj.b)
        elif dbForm == DF_SET:
            npArray = np.array((list(obj)))
            message = self.write_python_obj(npArray, message)
        elif dbForm == DF_MATRIX:
            message += '\x00'  # no row and colum labels
            message += (DATA_PACKER_SCALAR[DT_SHORT](flag)) #  a weird interface for matrix
            message += DATA_PACKER_SCALAR[DT_INT](obj.shape[0])
            message += DATA_PACKER_SCALAR[DT_INT](obj.shape[1])
            message += DATA_PACKER2D[dbType](obj)
        elif dbForm == DF_TABLE:
            message += DATA_PACKER_SCALAR[DT_INT](obj.values.shape[0])
            message += DATA_PACKER_SCALAR[DT_INT](obj.values.shape[1])
            message += '\x00'
            message += DATA_PACKER[DT_STRING](list(obj.columns))
            for name in obj.columns:
                message = self.write_python_obj(obj[name].values, message)
        else:
            message += DATA_PACKER_SCALAR[dbType](obj)
        return message
