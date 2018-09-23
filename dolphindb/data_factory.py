from struct import Struct
from .date_util import *
from dolphindb.socket_util import read_string, recvall, recvallhex
from .pair import Pair
from .settings import *
from .type_util import swap
import numpy as np
import pandas as pd


def get_form_type(socket):
    """
    Read the data form and type
    :return:
    """
    # print(socket)
    flag = DATA_UNPACKER_SCALAR[DT_SHORT](socket)

    # if flag == 1305:
    #     flag = 274
    # print("-----",flag)
    data_form = flag >> 8
    data_type = flag & 0xff

    # print("form", data_form)
    # print("type", data_type)
    return data_form, data_type


def table_str_col_generator(socket):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket)
    size = row * col
    vc = np.array([read_string(socket) for i in range(size)])
    return vc


def read_dolphindb_obj_general(socket):
    # print(socket)
    data_form, data_type = get_form_type(socket)
    # print( data_type)
    # print('------')
    if data_form == DF_VECTOR and data_type == DT_ANY:
        return VECTOR_FACTORY[DT_ANY](socket)
    elif data_form in [DF_SCALAR, DF_VECTOR]:
        if data_type in DATA_LOADER[data_form]:
            return DATA_LOADER[data_form][data_type](socket)
        else:
            return None
    elif data_form in [DF_SET, DF_DICTIONARY, DF_TABLE, DF_MATRIX]:
        return DATA_LOADER[data_form](socket)
    elif data_form in [DF_PAIR]:
        return Pair.fromlist(DATA_LOADER[data_form][data_type](socket))
    else:
        return None

def vec_generator(socket, data_type):
    '''
    generate a numpy array from a dolphindb vector
    :param socket: TCP socket
    :param data_type: dolphindb data type
    :return: the python corresponding data type
    '''
    row = DATA_UNPACKER_SCALAR[DT_INT](socket)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket)
    size = row * col
    if data_type in [DT_SYMBOL, DT_STRING]:
        vc = []
        # print("size",size)
        for i in range(size):
            vc.append(read_string(socket))
        """
        while True:
            packet = recvall(socket, 4096)
            if not packet or not len(packet):
                break
            data += packet
        (data.split('\x00\x00')[0].split('\x00')[:size])
        """
        return np.array(vc)
    else:
        return np.array(list(DATA_UNPACKER[data_type](socket, size)))


def vector_factory_any(socket):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket)
    # read one more byte, otherwise fail to generate the vector, not sure why
    # DATA_UNPACKER_SCALAR[DT_BYTE](socket)
    size = row * col
    # print("size",size)
    myList = []
    for i in range(0, size):
        myList.append(read_dolphindb_obj_general(socket))
    return myList


def set_generator(socket):
    data_form, data_type = get_form_type(socket)
    if data_type == DT_VOID:
        return set([])
    if ( data_form != DF_VECTOR):
        raise RuntimeError("The form of set keys must be vector")
    vec = VECTOR_FACTORY[data_type](socket)
    return set(vec)


def dict_generator(socket):
    """
    Generate a python dictionary object from a dolphindb dictionary object
    :param socket:
    :return:
    """
    """ read key array """
    key_form, key_type = get_form_type(socket)
    if key_form != DF_VECTOR:
        raise Exception("The form of dictionary keys must be vector")
    if key_type < 0 or key_type >= TYPE_NUM:
        raise Exception("Invalid key type: " + str(key_type))

    keys = VECTOR_FACTORY[key_type](socket)

    """ read value array """
    val_form, val_type = get_form_type(socket)
    if val_form != DF_VECTOR:
        raise Exception("The form of dictionary keys must be vector")
    if val_type < 0 or val_type >= TYPE_NUM:
        raise Exception("Invalid key type: " + str(key_type))
    vals = VECTOR_FACTORY[val_type](socket)

    if len(keys) != len(vals):
        raise Exception("The keys array size is not equal to the vals array size.")

    tmp = dict()
    for idx in range(len(keys)):
        tmp[keys[idx]] = vals[idx]
    return tmp


def table_generator(socket):
    """
    Generate a pandas data frame from dolphindb table object
    :param socket:
    :return:
    """
    rows = DATA_UNPACKER_SCALAR[DT_INT](socket)
    cols = DATA_UNPACKER_SCALAR[DT_INT](socket)
    tableName = read_string(socket)
    """ read column names """
    colNameDict = dict()
    colNames = []
    for i in range(cols):
        name = read_string(socket)
        colNameDict[name] = len(colNameDict)
        colNames.append(name)
    """ read columns and generate a pandas data frame"""
    df = pd.DataFrame()
    for i in range(len(colNames)):
        data_form, data_type = get_form_type(socket)
        # print(data_type)
        if data_form != DF_VECTOR:
            raise Exception("column " + colNames[i] + "in table " + tableName + " must be a vector!")
        if data_type in [DT_SYMBOL, DT_STRING]:
            col = table_str_col_generator(socket)
        else:
            col = VECTOR_FACTORY[data_type](socket)
        df[colNames[i]] = col
        # print(df)
    return df


def matrix_generator(socket):
    hasLabel = DATA_UNPACKER_SCALAR[DT_BYTE](socket)
    rowLabels = None
    colLabels = None
    if hasLabel & 1 == 1:
        data_form, data_type = get_form_type(socket)
        if data_form != DF_VECTOR:
            raise Exception("The form of matrix row labels must be vector")
        if data_type < 0 or data_type >= TYPE_NUM:
            raise Exception("Invalid data type for matrix row labels: " + data_type)
        rowLabels = VECTOR_FACTORY[data_type](socket)

    if hasLabel & 2 == 2:
        data_form, data_type = get_form_type(socket)
        if data_form != DF_VECTOR:
            raise Exception("The form of matrix row labels must be vector")
        if data_type < 0 or data_type >= TYPE_NUM:
            raise Exception("Invalid data type for matrix row labels: " + data_type)
        colLabels = VECTOR_FACTORY[data_type](socket)

    flag = DATA_UNPACKER_SCALAR[DT_SHORT](socket)
    # print(flag)
    data_type = flag & 0xff
    if data_type < 0 or data_type >= TYPE_NUM:
        raise Exception("Invalid data type for matrix row labels: " + data_type)
    rows = DATA_UNPACKER_SCALAR[DT_INT](socket)
    cols = DATA_UNPACKER_SCALAR[DT_INT](socket)
    size = rows * cols

    # print(data_type)
    # print(type(DATA_UNPACKER[data_type](socket, size)))
    vals = DATA_UNPACKER[data_type](socket, size)
    if vals is not None:
        # print(data_type,socket)
        vals = np.transpose(np.array(list(vals)).reshape(cols,rows))
    if not len(vals):
        vals = None
    return vals, rowLabels, colLabels

"""endiness: the function is reset in dolphindb.connect"""
endianness = lambda x : x;


""" Unpack scalar from dolphindb object """
DATA_UNPACKER_SCALAR = dict()
DATA_UNPACKER_SCALAR[DT_VOID] = lambda x: swap(Struct('b').unpack(recvall(x, DATA_SIZE[DT_BOOL]))[0], DT_BOOL)
DATA_UNPACKER_SCALAR[DT_BOOL] = lambda x: swap(Struct('b').unpack(recvall(x, DATA_SIZE[DT_BOOL]))[0], DT_BOOL)
DATA_UNPACKER_SCALAR[DT_BYTE] = lambda x: swap(Struct('b').unpack((recvall(x, DATA_SIZE[DT_BYTE])))[0], DT_BYTE)
DATA_UNPACKER_SCALAR[DT_SHORT] = lambda x: swap(Struct(endianness('h')).unpack(recvall(x, DATA_SIZE[DT_SHORT]))[0], DT_SHORT)
DATA_UNPACKER_SCALAR[DT_INT] = lambda x: swap(Struct(endianness('i')).unpack((recvall(x, DATA_SIZE[DT_INT])))[0], DT_INT)
DATA_UNPACKER_SCALAR[DT_LONG] = lambda x: swap(Struct(endianness('q')).unpack((recvall(x, DATA_SIZE[DT_LONG])))[0], DT_LONG)
DATA_UNPACKER_SCALAR[DT_DATE] = lambda x: Date(Struct(endianness('i')).unpack((recvall(x, DATA_SIZE[DT_DATE])))[0])
DATA_UNPACKER_SCALAR[DT_MONTH] = lambda x: Month(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_MONTH]))[0])
DATA_UNPACKER_SCALAR[DT_TIME] = lambda x: Time(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_TIME]))[0])
DATA_UNPACKER_SCALAR[DT_MINUTE] = lambda x: Minute(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_MINUTE]))[0])
DATA_UNPACKER_SCALAR[DT_SECOND] = lambda x: Second(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_SECOND]))[0])
DATA_UNPACKER_SCALAR[DT_DATETIME] = lambda x: Datetime(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_DATETIME]))[0])
DATA_UNPACKER_SCALAR[DT_TIMESTAMP] = lambda x: Timestamp(Struct(endianness('q')).unpack((recvall(x, DATA_SIZE[DT_TIMESTAMP])))[0])
DATA_UNPACKER_SCALAR[DT_NANOTIME] = lambda x: NanoTime(Struct(endianness('q')).unpack(recvall(x, DATA_SIZE[DT_NANOTIME]))[0])
DATA_UNPACKER_SCALAR[DT_NANOTIMESTAMP] = lambda x: NanoTimestamp(Struct(endianness('q')).unpack((recvall(x, DATA_SIZE[DT_NANOTIMESTAMP])))[0])
DATA_UNPACKER_SCALAR[DT_DATETIME64] = lambda x: NanoTimestamp(Struct(endianness('q')).unpack(recvall(x, DATA_SIZE[DT_NANOTIMESTAMP]))[0])
DATA_UNPACKER_SCALAR[DT_FLOAT] = lambda x: swap(Struct(endianness('f')).unpack(recvall(x, DATA_SIZE[DT_FLOAT]))[0], DT_FLOAT)
DATA_UNPACKER_SCALAR[DT_DOUBLE] = lambda x: swap(Struct(endianness('d')).unpack((recvall(x, DATA_SIZE[DT_DOUBLE])))[0], DT_DOUBLE)
DATA_UNPACKER_SCALAR[DT_SYMBOL] = lambda x: read_string(x)
DATA_UNPACKER_SCALAR[DT_STRING] = lambda x: read_string(x)
DATA_UNPACKER_SCALAR[DT_ANY] = lambda x: None
DATA_UNPACKER_SCALAR[DT_DICTIONARY] = lambda x: None
DATA_UNPACKER_SCALAR[DT_OBJECT] = lambda x: None


DATA_UNPACKER = dict()
DATA_UNPACKER[DT_VOID] = lambda x, y: map(lambda z: swap(z, DT_BOOL), Struct(str(y)+'b').unpack(recvall(x, DATA_SIZE[DT_BOOL]*y)))
DATA_UNPACKER[DT_BOOL] = lambda x, y: map(lambda z: swap(z, DT_BOOL), Struct(str(y)+'b').unpack(recvall(x, DATA_SIZE[DT_BOOL]*y)))
DATA_UNPACKER[DT_BYTE] = lambda x, y: map(lambda z: swap(z, DT_BYTE), Struct(str(y)+'b').unpack(recvall(x, DATA_SIZE[DT_BYTE]*y)))
DATA_UNPACKER[DT_SHORT] = lambda x, y: map(lambda z: swap(z, DT_SHORT), Struct(endianness(str(y)+'h')).unpack(recvall(x, DATA_SIZE[DT_SHORT]*y)))
DATA_UNPACKER[DT_INT] = lambda x, y: list(map(lambda z: swap(z, DT_INT), Struct(endianness(str(y)+'i')).unpack(recvall(x, DATA_SIZE[DT_INT]*y))))
DATA_UNPACKER[DT_LONG] = lambda x, y: map(lambda z: swap(z, DT_LONG), Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_LONG]*y))))
DATA_UNPACKER[DT_DATE] = lambda x, y: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_DATE]*y)))
DATA_UNPACKER[DT_MONTH] = lambda x, y: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_MONTH]*y)))
DATA_UNPACKER[DT_TIME] = lambda x, y: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_TIME]*y)))
DATA_UNPACKER[DT_MINUTE] = lambda x, y: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_MINUTE]*y)))
DATA_UNPACKER[DT_SECOND] = lambda x, y: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_SECOND]*y)))
DATA_UNPACKER[DT_DATETIME] = lambda x, y: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_DATETIME]*y)))
DATA_UNPACKER[DT_TIMESTAMP] = lambda x, y: Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_TIMESTAMP]*y)))
DATA_UNPACKER[DT_NANOTIME] = lambda x, y: Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_NANOTIME]*y)))
DATA_UNPACKER[DT_NANOTIMESTAMP] = lambda x, y: Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_NANOTIMESTAMP]*y)))
DATA_UNPACKER[DT_DATETIME64] = lambda x, y: Struct(endianness(str(y)+'q')).unpack(recvall(x, DATA_SIZE[DT_NANOTIMESTAMP]*y))
DATA_UNPACKER[DT_FLOAT] = lambda x, y: map(lambda z: swap(z, DT_FLOAT), Struct(endianness(str(y)+'f')).unpack(recvall(x, DATA_SIZE[DT_FLOAT]*y)))
DATA_UNPACKER[DT_DOUBLE] = lambda x, y: list(map(lambda z: swap(z, DT_DOUBLE), Struct(endianness(str(y)+'d')).unpack((recvall(x, DATA_SIZE[DT_DOUBLE]*y)))))
DATA_UNPACKER[DT_SYMBOL] = lambda x, y: None
DATA_UNPACKER[DT_STRING] = lambda x, y: None
DATA_UNPACKER[DT_ANY] = lambda x, y: None
DATA_UNPACKER[DT_DICTIONARY] = lambda x, y: None
DATA_UNPACKER[DT_OBJECT] = lambda x, y: None


""" dictionary of functions for making numpy arrays from dolphindb vectors"""
VECTOR_FACTORY = dict()
VECTOR_FACTORY[DT_VOID] = lambda x:[]
VECTOR_FACTORY[DT_BOOL] = lambda x: vec_generator(x, DT_BOOL)
VECTOR_FACTORY[DT_BYTE] = lambda x: vec_generator(x, DT_BYTE)
VECTOR_FACTORY[DT_SHORT] = lambda x: vec_generator(x, DT_SHORT)
VECTOR_FACTORY[DT_INT] = lambda x: vec_generator(x, DT_INT)
VECTOR_FACTORY[DT_LONG] = lambda x: list(vec_generator(x, DT_LONG))
VECTOR_FACTORY[DT_DATE] = lambda x: list(map(Date, vec_generator(x, DT_DATE)))
VECTOR_FACTORY[DT_MONTH] = lambda x: list(map(Month, vec_generator(x, DT_MONTH)))
VECTOR_FACTORY[DT_TIME] = lambda x: list(map(Time, vec_generator(x, DT_TIME)))
VECTOR_FACTORY[DT_MINUTE] = lambda x: list(map(Minute, vec_generator(x, DT_MINUTE)))
VECTOR_FACTORY[DT_SECOND] = lambda x: list(map(Second, vec_generator(x, DT_SECOND)))
VECTOR_FACTORY[DT_DATETIME] = lambda x: list(map(Datetime, vec_generator(x, DT_DATETIME)))
VECTOR_FACTORY[DT_TIMESTAMP] = lambda x: list(map(Timestamp, vec_generator(x, DT_TIMESTAMP)))
VECTOR_FACTORY[DT_NANOTIME] = lambda x: list(map(NanoTime, vec_generator(x, DT_NANOTIME)))
VECTOR_FACTORY[DT_NANOTIMESTAMP] = lambda x: list(map(NanoTimestamp, vec_generator(x, DT_NANOTIMESTAMP)))
VECTOR_FACTORY[DT_DATETIME64] = lambda x: list(map(NanoTimestamp, vec_generator(x, DT_DATETIME64)))
VECTOR_FACTORY[DT_FLOAT] = lambda x: vec_generator(x, DT_FLOAT)
VECTOR_FACTORY[DT_DOUBLE] = lambda x: vec_generator(x, DT_DOUBLE)
VECTOR_FACTORY[DT_SYMBOL] = lambda x: list(map(lambda z: swap(z, DT_SYMBOL), vec_generator(x, DT_SYMBOL)))
VECTOR_FACTORY[DT_STRING] = lambda x: list(map(lambda z: swap(z, DT_STRING), vec_generator(x, DT_STRING)))


VECTOR_FACTORY[DT_ANY] = vector_factory_any

""" dictionary of functions for loading different forms of data from dolphindb api"""
DATA_LOADER = dict()
DATA_LOADER[DF_SCALAR] = DATA_UNPACKER_SCALAR
DATA_LOADER[DF_VECTOR] = VECTOR_FACTORY
DATA_LOADER[DF_PAIR] = VECTOR_FACTORY
DATA_LOADER[DF_SET] = lambda x: set_generator(x)
DATA_LOADER[DF_DICTIONARY] = lambda x: dict_generator(x)
DATA_LOADER[DF_TABLE] = lambda x: table_generator(x)
DATA_LOADER[DF_MATRIX] = lambda x: matrix_generator(x)


""" pack from python scalar"""
DATA_PACKER_SCALAR = dict()
DATA_PACKER_SCALAR[DT_BOOL] = lambda x: Struct('b').pack(swap(x))
DATA_PACKER_SCALAR[DT_SHORT] = lambda x: Struct(endianness('h')).pack(swap(x))
DATA_PACKER_SCALAR[DT_INT] = lambda x: Struct(endianness('i')).pack(swap(x))
DATA_PACKER_SCALAR[DT_LONG] = lambda x: Struct(endianness('q')).pack(swap(x))
DATA_PACKER_SCALAR[DT_DOUBLE] = lambda x: Struct(endianness('d')).pack(swap(x))
DATA_PACKER_SCALAR[DT_STRING] = lambda x: x.encode() + Struct('b').pack(0)
DATA_PACKER_SCALAR[DT_DATE] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_MONTH] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_TIME] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_MINUTE] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_SECOND] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_DATETIME] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_TIMESTAMP] = lambda x: Struct(endianness('q')).pack(x.value)
DATA_PACKER_SCALAR[DT_NANOTIME] = lambda x: Struct(endianness('q')).pack(x.value)
DATA_PACKER_SCALAR[DT_NANOTIMESTAMP] = lambda x: Struct(endianness('q')).pack(x.value)
DATA_PACKER_SCALAR[DT_DATETIME64] = lambda x: Struct(endianness('q')).pack(x)

""" pack from numpy 1D array """
DATA_PACKER = dict()
DATA_PACKER[DT_BOOL] = lambda x: Struct(endianness("%db" % x.size)).pack(*map(lambda y: swap(y), x))
DATA_PACKER[DT_INT] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: swap(y), x))
DATA_PACKER[DT_LONG] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: swap(y), x))
DATA_PACKER[DT_DOUBLE] = lambda x: Struct(endianness("%dd" % x.size)).pack(*map(lambda y: swap(y), x))
DATA_PACKER[DT_STRING] = lambda x: (''.join(map(lambda y: y+'\x00', x))).encode()
DATA_PACKER[DT_DATE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_MONTH] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_TIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_MINUTE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_SECOND] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_DATETIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_TIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_NANOTIME] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_NANOTIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_DATETIME64] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y, x))

""" pack from numpy multi-dimensional array """
DATA_PACKER2D = dict()
DATA_PACKER2D[DT_BOOL] = lambda x: Struct(endianness("%db" % x.size)).pack(*map(lambda y:swap(y), x.T.flat))
DATA_PACKER2D[DT_INT] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:swap(y), x.T.flat))
DATA_PACKER2D[DT_LONG] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:swap(y), x.T.flat))
DATA_PACKER2D[DT_DOUBLE] = lambda x: Struct(endianness("%dd" % x.size)).pack(*map(lambda y:swap(y), x.T.flat))
DATA_PACKER2D[DT_STRING] = None # dolphindb doesn't support 2-D string matrix
DATA_PACKER2D[DT_DATE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_MONTH] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_TIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_MINUTE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_SECOND] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_DATETIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_TIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_NANOTIME] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_NANOTIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER[DT_DATETIME64] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:y.value, x.T.flat))