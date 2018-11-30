import re
import socket
import traceback
import uuid
from . import date_util as d
from dolphindb import data_factory, socket_util
from dolphindb.data_factory import *
from dolphindb.settings import *
from dolphindb.type_util import determine_form_type
from dolphindb.pair import Pair
from dolphindb.table import Table
from threading import Thread, Lock

def _generate_tablename():
    return "TMP_TBL_" + uuid.uuid4().hex[:8]

def _generate_dbname():
    return "TMP_DB_" + uuid.uuid4().hex[:8]+"DB"


class session(object):
    """
    dolphindb api class
    connect: initiate socket connection
    run: execute dolphindb script and return corresponding python objects
    1: Scalar variable returns a python scalar
    2: Vector object returns numpy array
    3: Table object returns  a pandas data frame
    4: Matrix object returns a pandas data frame
    """
    def __init__(self, host=None, port=None, userid="", password=""):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.host = host
        self.port = port
        self.userid = userid
        self.password=password
        self.sessionID = None
        self.remoteLittleEndian = None
        self.encrypted = False
        self.mutex=Lock()
        if self.host is not None and self.port is not None:
            self.connect(host, port)

    def connect(self, host, port, userid="", password=""):
        self.host = host
        self.port = port
        self.userid = userid
        self.password = password
        self.encrypted = False
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
                    raise Exception('Connection failed', msg, b"")
                self.sessionID = sid
                self.remoteLittleEndian = False if is_remote == '0' else True
                data_factory.endianness = '>'.__add__ if is_remote == '0' else  '<'.__add__
                if userid and password and len(userid) and len(password):
                    self.signon()

        except Exception as e:
            traceback.print_exc()
            return False
        return True

    def _reconnect(self, message):
        try:
            print ("socket connection was lost; attemping to reconnect to %s : %s\n" % (self.host, self.port))
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.signon()
            socket_util.sendall(self.socket, message)
            print ("socket is reconnected\n")
        except Exception:
            print ("socket reconnection has failed!\n")
            raise
        return True

    def login(self,userid, password, enableEncryption):
        self.mutex.acquire()
        try:
            self.userid = userid
            self.password = password
            self.encrypted = enableEncryption
            self.signon()
        finally:
            self.mutex.release()

    def signon(self):
        if len(self.userid) and len(self.password):
            self.run("login('%s','%s')"%(self.userid, self.password))

    def close(self):
        self.socket.close()
        self.host = None
        self.port = None
        self.sessionID = None
        self.remoteLittleEndian = None

    def upload(self, nameObjectDict):
        if not isinstance(nameObjectDict, dict) or not nameObjectDict:
            print ('\n Empty name/object mapping received - no upload\n')
            return None
        if not self.sessionID:
            raise Exception('Connection has not been established yet; please call function connect!')

        """upload variable names """
        body = "variable\n"
        objects = []
        pyObj = []
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
            pyObj.append(self.write_python_obj(o, message))
        reconnected = False
        try:
            totalsent = socket_util.sendall(self.socket, message,pyObj)
        except IOError as e:
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
        sid, obj_num, _ = headers
        msg = socket_util.readline(self.socket)
        if msg != 'OK' and obj_num==0:
            raise Exception('Server Exception', msg)
        return None

    def run(self, script, *args):
        # print("run")

        if not script or len(script.strip()) == 0:
            raise Exception('Empty Script Received', script)
        if not self.sessionID:
            raise Exception('Connection has not been established yet; please call function connect!')
        """msg send"""
        objs = []
        if len(args):
            """ function with arguments"""
            body = "function\n" + script
            body += "\n" + str(len(args)) + "\n";
            body += "1" if self.remoteLittleEndian else "0"
            message = "API " + self.sessionID + " " + str(len(body)) + '\n' + body
            for arg in args:
                objs.append(self.write_python_obj(arg, message))
        else:
            """pure script"""
            body = "script\n" + script
            message = "API " + self.sessionID + " " + str(len(body)) + '\n' + body
        reconnected = False
        try:
            # print(message)
            # print(objs)
            socket_util.sendall(self.socket, message, objs)
        except IOError:
            reconnected = self.reconnect(message)

        """msg receive"""
        header = socket_util.readline(self.socket)
        while header == "MSG":
            socket_util.read_string(self.socket)  # python API doesn't support progress listener
            header = socket_util.readline(self.socket)

        headers = header.split()
        if len(headers) != 3:
            raise Exception('Header Length Incorrect', header)
        if reconnected:
            if not self.sessionID == headers[0]:
                print("old sessionID %s is invalid after reconnection; new sessionID is %s\n" % (
                self.sessionID, headers[0]))
                self.sessionID = headers[0]

        sid, obj_num, _ = headers
        msg = socket_util.readline(self.socket)
        if msg != 'OK':
            raise Exception('Server Exception', msg)
        if int(obj_num) == 0:
            return None
        return self.read_dolphindb_obj

    def table(self, data, dbPath=None):
        return Table(data=data, dbPath=dbPath, s=self)

    @property
    def read_dolphindb_obj(self):
        # print("here")
        return read_dolphindb_obj_general(self.socket)

    def write_python_obj(self, obj, message):
        (dbForm, dbType) = determine_form_type(obj)
        tmp = b""

        # special handling of numpy datetime64[ns]
        # internally we don't have this type
        # we use nanotimestamp for it
        # however, packing value is different from other datatypes
        # so we handle it seperately
        if dbType == DT_DATETIME64:
            flag = (dbForm << 8) + DT_NANOTIMESTAMP
            dbType = DT_NANOTIMESTAMP
            if isinstance(obj, list) or (isinstance(obj, np.ndarray) and dbForm == DF_VECTOR):
                obj = NanoTimestamp.from_vec_datetime64(obj)
            else:
                obj = NanoTimestamp.from_datetime64(obj)
        else:
            flag = (dbForm << 8) + dbType
        tmp += (DATA_PACKER_SCALAR[DT_SHORT](flag))

        if dbType == DT_ANY and isinstance(obj, list):  # any vector written
            tmp += DATA_PACKER_SCALAR[DT_INT](len(obj))
            tmp += DATA_PACKER_SCALAR[DT_INT](1)
            for val in obj:
                tmp += self.write_python_obj(val, message)
        elif isinstance(obj, list):  # vector written from list
            tmp += DATA_PACKER_SCALAR[DT_INT](len(obj))
            tmp += DATA_PACKER_SCALAR[DT_INT](1)
            for val in obj:
                tmp += DATA_PACKER_SCALAR[dbType](val)
        elif isinstance(obj, dict):
            tmp += self.write_python_obj(list(obj.keys()), message)
            tmp += self.write_python_obj(list(obj.values()), message)
        elif isinstance(obj, np.ndarray) and dbForm == DF_VECTOR:  # vector written from numpy array
            tmp += DATA_PACKER_SCALAR[DT_INT](obj.size)
            tmp += DATA_PACKER_SCALAR[DT_INT](1)
            tmp += DATA_PACKER[dbType](obj)
        elif isinstance(obj, Pair):
            tmp += DATA_PACKER_SCALAR[DT_INT](2)
            tmp += DATA_PACKER_SCALAR[DT_INT](1)
            tmp += DATA_PACKER_SCALAR[dbType](obj.a)
            tmp += DATA_PACKER_SCALAR[dbType](obj.b)
        elif dbForm == DF_SET:
            npArray = np.array((list(obj)))
            tmp += self.write_python_obj(npArray, message)
        elif dbForm == DF_MATRIX:
            tmp += b'\x00'  # no row and colum labels
            tmp += (DATA_PACKER_SCALAR[DT_SHORT](flag))  # a weird interface for matrix
            tmp += DATA_PACKER_SCALAR[DT_INT](obj.shape[0])
            tmp += DATA_PACKER_SCALAR[DT_INT](obj.shape[1])
            tmp += DATA_PACKER2D[dbType](obj)
        elif dbForm == DF_TABLE:
            tmp += DATA_PACKER_SCALAR[DT_INT](obj.values.shape[0])
            tmp += DATA_PACKER_SCALAR[DT_INT](obj.values.shape[1])
            tmp += b'\x00'
            tmp += DATA_PACKER[DT_STRING](list(obj.columns))
            for name in obj.columns:
                tmp += self.write_python_obj(obj[name].values, message)
        else:
            tmp += (DATA_PACKER_SCALAR[dbType](obj))
        return tmp

    def rpc(self,function_name, *args):
        """

        :param function_name: remote function call name
        :param args: arguments for remote function
        :return: return remote function call result
        """
        """msg send"""
        # if len(args):
        """ function with arguments"""
        body = "function\n" + function_name
        body += "\n" + str(len(args)) + "\n"
        body += "1" if self.remoteLittleEndian else "0"
        message = "API " + self.sessionID + " " + str(len(body)) + '\n' + body
        for arg in args:
            message = self.write_python_obj(arg, message)
        # else:
        #     """pure script"""
        #     body = "script\n"+function_name
        #     message = "API "+self.sessionID + " " + str(len(body)) + '\n' + body

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
        return self.read_dolphindb_obj

    def table(self, dbPath=None, data=None,  tableAliasName=None, inMem=False, partitions=[], ):
        """

        :param data: pandas dataframe, python dictionary, or DolphinDB table name
        :param dbPath: DolphinDB database path
        :param tableAliasName: DolphinDB table alias name
        :param inMem: load the table in memory or not
        :param partitions: the partition column to be loaded into memory. by default, load all
        :return: a Table object
        """
        return Table(dbPath=dbPath, data=data,  tableAliasName=tableAliasName, inMem=inMem, partitions=partitions, s=self)

    def loadText(self,  filename, delimiter=","):
        tableName = _generate_tablename()
        runstr = tableName + '=loadText("' + filename + '","' + delimiter + '")'
        self.run(runstr)
        return Table(data=tableName, s=self)

    def ploadText(self, filename, delimiter=","):
        tableName = _generate_tablename()
        runstr = tableName + '= ploadText("' + filename + '","' + delimiter + '")'
        self.run(runstr)
        return Table(data=tableName, s=self)

    def loadTable(self,tableName,  dbPath=None, partitions=[], memoryMode=False):
        """
        :param dbPath: DolphinDB table db path
        :param tableName: DolphinDB table name
        :param partitions: partitions to be loaded when specified
        :param memoryMode: loadTable all in ram or not
        :return:a Table object
        """
        if dbPath:
            runstr = '{tableName} = loadTable("{dbPath}", "{data}",{partitions},{inMem})'
            fmtDict = dict()
            tbName = _generate_tablename()
            fmtDict['tableName'] = tbName
            fmtDict['dbPath'] = dbPath
            fmtDict['data'] = tableName
            if type(partitions) is list:
                if len(partitions) and type(partitions[0]) is not str:
                    fmtDict['partitions'] = ('[' + ','.join(str(x) for x in partitions) + ']') if len(partitions) else ""
                else:
                    fmtDict['partitions'] = ('["' + '","'.join(partitions) + '"]') if len(partitions) else ""
            else:
                if type(partitions) is str:
                    fmtDict['partitions'] = '"' + partitions + '"'
                else:
                    fmtDict['partitions'] = partitions
            fmtDict['inMem'] = str(memoryMode).lower()
            runstr = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            self.run(runstr)
            return Table(data=tbName, s=self)
        else:
            return Table(data=tableName, s=self)

    def loadTableBySQL(self, tableName, dbPath, sql):
        """
        :param tableName: DolphinDB table name
        :param dbPath: DolphinDB table db path
        :param sql: sql query to load the data
        :return:a Table object
        """
        # loadTableBySQL
        runstr = 'db=database("' + dbPath + '")'
        # print(runstr)
        self.run(runstr)
        runstr = tableName + '= db.loadTable("%s")' % tableName
        # print(runstr)
        self.run(runstr)
        runstr = tableName + "=loadTableBySQL(<%s>)" % sql
        # runstr =  sql
        # print(runstr)
        self.run(runstr)
        return Table(data=tableName, s=self)

    def database(self,dbName, partitionType=None, partitions=None, dbPath=None):
        """

        :param dbName: database variable Name on DolphinDB Server
        :param partitionType: database Partition Type
        :param partitions: partitions as a python list
        :param dbPath: database path
        :return:
        """
        partition_str = str(partitions)
        if partitionType:
            if dbPath:
                dbstr =  dbName + '=database("'+dbPath+'",' + str(partitionType) + "," + partition_str + ")"
            else:
                dbstr =  dbName +'=database("",' + str(partitionType) + "," + partition_str + ")"
        else:
            if dbPath:
                dbstr =  dbName +'=database("' + dbPath + '")'
            else:
                dbstr =  dbName +'=database("")'
        self.run(dbstr)
        return

    def existsDatabase(self, dbUrl):
        return self.run("existsDatabase('%s')" % dbUrl)

    def existsTable(self, dbUrl, tableName):
        return self.run("existsTable('%s','%s')" % (dbUrl,tableName))

    def dropDatabase(self, dbPath):
        self.run("dropDatabase('" + dbPath + "')")

    def dropPartition(self, dbPath, partitionPaths, tableName=None):
        """

        :param dbPath: a DolphinDB database path
        :param partitionPaths:  a string or a list of strings. It indicates the directory of a single partition or a list of directories for multiple partitions under the database folder. It must start with "/"
        :param tableName:a string indicating a table name.
        :return:
        """
        db = _generate_dbname()
        self.run(db + '=database("' + dbPath + '")')
        if isinstance(partitionPaths, list):
            pths = '"'+'","'.join(partitionPaths)+'"'
        else:
            pths = partitionPaths

        if tableName:
            self.run("dropPartition(%s,[%s],%s)" % (db, pths, tableName))
        else:
            self.run("dropPartition(%s,[%s])" % (db, pths))

    def dropTable(self, dbPath, tableName):
        db = _generate_dbname()
        self.run(db + '=database("' + dbPath + '")')
        self.run("dropTable(%s,'%s')" % (db,tableName))

    def loadTextEx(self, dbPath="", tableName="",  partitionColumns=[], filePath="", delimiter=","):
        """
        :param tableName: loadTextEx table name
        :param dbPath: database path, when dbPath is empty, it is in-memory database
        :param partitionColumns: partition columns as a python list
        :param filePath:the file to load into database
        :param delimiter:
        :return: a Table object
        """
        isDBPath = True
        if "/" in dbPath or "\\" in dbPath or "dfs://" in dbPath:
            dbstr ='db=database("' + dbPath + '")'
        # print(dbstr)
            self.run(dbstr)
            tbl_str = '{tableNameNEW} = loadTextEx(db, "{tableName}", {partitionColumns}, "{filePath}", {delimiter})'
        else:
            isDBPath = False
            tbl_str = '{tableNameNEW} = loadTextEx('+dbPath+', "{tableName}", {partitionColumns}, "{filePath}", {delimiter})'
        fmtDict = dict()
        fmtDict['tableNameNEW'] = _generate_tablename()
        fmtDict['tableName'] = tableName
        fmtDict['partitionColumns'] = str(partitionColumns)
        fmtDict['filePath'] = filePath
        fmtDict['delimiter'] = delimiter
        # tbl_str = tableName+'=loadTextEx(db,"' + tableName + '",'+ str(partitionColumns) +',"'+ filePath+"\",'"+delimiter+"')"
        tbl_str = re.sub(' +', ' ', tbl_str.format(**fmtDict).strip())
        # print(tbl_str)
        self.run(tbl_str)
        if isDBPath:
            return Table(data=fmtDict['tableName'] , dbPath=dbPath, s=self)
        else:
            return Table(data=fmtDict['tableNameNEW'], s=self)

    def undef(self, varName, varType):
        undef_str = 'undef("{varName}", {varType})'
        fmtDict = dict()
        fmtDict['varName'] = varName
        fmtDict['varType'] = varType
        self.run(undef_str.format(**fmtDict).strip())

    def undefAll(self):
        self.run("undef all")

    def clearAllCache(self, dfs=False):
        if dfs:
            self.run("pnodeRun(clearAllCache)")
        else:
            self.run("clearAllCache()")

    # @property
    # def read_dolphindb_obj(self):
    #    return read_dolphindb_obj_general(self.socket)
    #
    # def write_python_obj(self, obj, message):
    #     (dbForm, dbType) = determine_form_type(obj)
    #
    #     #special handling of numpy datetime64[ns]
    #     #internally we don't have this type
    #     #we use nanotimestamp for it
    #     #however, packing value is different from other datatypes
    #     #so we handle it seperately
    #     if dbType == 100:
    #         flag = (dbForm << 8) + DT_NANOTIMESTAMP
    #     else:
    #         flag = (dbForm << 8) + dbType
    #     message += (DATA_PACKER_SCALAR[DT_SHORT](flag))
    #
    #     if dbType == DT_ANY and isinstance(obj, list): # any vector written
    #         message += DATA_PACKER_SCALAR[DT_INT](len(obj))
    #         message += DATA_PACKER_SCALAR[DT_INT](1)
    #         for val in obj:
    #             message = self.write_python_obj(val, message)
    #     elif isinstance(obj, list):  # vector written from list
    #         message += DATA_PACKER_SCALAR[DT_INT](len(obj))
    #         message += DATA_PACKER_SCALAR[DT_INT](1)
    #         for val in obj:
    #             message += DATA_PACKER_SCALAR[dbType](val)
    #     elif isinstance(obj, dict):
    #         message = self.write_python_obj(obj.keys(), message)
    #         message = self.write_python_obj(obj.values(), message)
    #     elif isinstance(obj, np.ndarray) and dbForm == DF_VECTOR: # vector written from numpy array
    #         message += DATA_PACKER_SCALAR[DT_INT](obj.size)
    #         message += DATA_PACKER_SCALAR[DT_INT](1)
    #         message += DATA_PACKER[dbType](obj)
    #     elif isinstance(obj, Pair):
    #         message += DATA_PACKER_SCALAR[DT_INT](2)
    #         message += DATA_PACKER_SCALAR[DT_INT](1)
    #         message += DATA_PACKER_SCALAR[dbType](obj.a)
    #         message += DATA_PACKER_SCALAR[dbType](obj.b)
    #     elif dbForm == DF_SET:
    #         npArray = np.array((list(obj)))
    #         message = self.write_python_obj(npArray, message)
    #     elif dbForm == DF_MATRIX:
    #         message += '\x00'  # no row and colum labels
    #         message += (DATA_PACKER_SCALAR[DT_SHORT](flag)) #  a weird interface for matrix
    #         message += DATA_PACKER_SCALAR[DT_INT](obj.shape[0])
    #         message += DATA_PACKER_SCALAR[DT_INT](obj.shape[1])
    #         message += DATA_PACKER2D[dbType](obj)
    #     elif dbForm == DF_TABLE:
    #         message += DATA_PACKER_SCALAR[DT_INT](obj.values.shape[0])
    #         message += DATA_PACKER_SCALAR[DT_INT](obj.values.shape[1])
    #         message += '\x00'
    #         message += DATA_PACKER[DT_STRING](list(obj.columns))
    #         for name in obj.columns:
    #             message = self.write_python_obj(obj[name].values, message)
    #     else:
    #         message += DATA_PACKER_SCALAR[dbType](obj)
    #     return message
