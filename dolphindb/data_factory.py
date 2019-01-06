from struct import Struct
from socket_util import read_string, recvall
from type_util import *
import numpy as np
import pandas as pd


def get_form_type(socket, buffer, nullMap):
    flag = DATA_UNPACKER_SCALAR[DT_SHORT](socket, buffer, nullMap)
    data_form = flag >> 8
    data_type = flag & 0xff
    return data_form, data_type


def table_str_col_generator(socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    vc = np.array([read_string(socket, buffer) for i in xrange(size)])
    return vc


def read_xxdb_obj_general(socket, buffer, nullMap):
    data_form, data_type = get_form_type(socket, buffer, nullMap)
    if data_form == DF_VECTOR and data_type == DT_ANY:
        return VECTOR_FACTORY[DT_ANY](socket, buffer, nullMap)
    elif data_form in [DF_SCALAR, DF_VECTOR]:

        if data_type in DATA_LOADER[data_form]:
            obj = DATA_LOADER[data_form][data_type](socket, buffer, nullMap)
            if data_type == DT_BOOL:
                if data_form == DF_SCALAR:
                    if obj is None or np.isnan(obj):
                        return nullMap[data_type]
                    return bool(obj)
                else:
                    obj_new = []
                    for o in obj:
                        if o is None or np.isnan(o):
                            obj_new.append(nullMap[data_type])
                        else:
                            obj_new.append(bool(o))
                    obj = obj_new
            return obj
        else:
            return None
    elif data_form in [DF_SET, DF_DICTIONARY, DF_TABLE, DF_MATRIX]:
        return DATA_LOADER[data_form](socket, buffer, nullMap)
    elif data_form in [DF_PAIR]:
        return Pair.fromlist(DATA_LOADER[data_form][data_type](socket, buffer, nullMap))
    else:
        return None

def vec_generator(socket, data_type, buffer, nullMap):
    '''
    generate a numpy array from a xxdb vector
    :param socket: TCP socket
    :param data_type: xxdb data type
    :return: the python corresponding data type
    '''
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    if data_type in [DT_SYMBOL, DT_STRING]:

        vc = []
        for i in xrange(size):
            vc.append(read_string(socket, buffer))
        """
        while True:
            packet = recvall(socket, 4096)
            if not packet or not len(packet):
                break
            data += packet
        (data.split('\x00\x00')[0].split('\x00')[:size])
        """
        return np.array(vc, dtype=object)
    else:
        return np.array(list(DATA_UNPACKER[data_type](socket, size, buffer, nullMap)))



def vector_factory_any(socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    # read one more byte, otherwise fail to generate the vector, not sure why
    # DATA_UNPACKER_SCALAR[DT_BYTE](socket)
    size = row * col
    myList = []
    for i in range(0, size):
        myList.append(read_xxdb_obj_general(socket, buffer, nullMap))
    return myList


def set_generator(socket, buffer, nullMap):
    data_form, data_type = get_form_type(socket, buffer, nullMap)
    if data_type == DT_VOID:
        return set([])
    if ( data_form != DF_VECTOR):
        raise RuntimeError("The form of set keys must be vector")
    vec = VECTOR_FACTORY[data_type](socket, buffer, nullMap)
    return set(vec)


def dict_generator(socket, buffer, nullMap):

    """ read key array """
    key_form, key_type = get_form_type(socket, buffer, nullMap)
    if key_form != DF_VECTOR:
        raise Exception("The form of dictionary keys must be vector")
    if key_type < 0 or key_type >= TYPE_NUM:
        raise Exception("Invalid key type: " + str(key_type))

    keys = VECTOR_FACTORY[key_type](socket, buffer, nullMap)

    """ read value array """
    val_form, val_type = get_form_type(socket, buffer, nullMap)
    if val_form != DF_VECTOR:
        raise Exception("The form of dictionary keys must be vector")
    if val_type < 0 or val_type >= TYPE_NUM:
        raise Exception("Invalid key type: " + str(key_type))
    vals = VECTOR_FACTORY[val_type](socket, buffer, nullMap)

    if len(keys) != len(vals):
        raise Exception("The keys array size is not equal to the vals array size.")
    tmp = dict()
    for idx in xrange(len(keys)):
        tmp[keys[idx]] = vals[idx]
    return tmp


def _symbol_handler(data_type, socket, buffer, nullMap):
    return table_str_col_generator(socket, buffer, nullMap)


def _int_handler(data_type, socket, buffer, nullMap):
    return VECTOR_FACTORY[DT_INT](socket, buffer, nullMap)


def _bool_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [val is 1 if val == 1 or val == 0 else nullMap[DT_BOOL] for val in DATA_UNPACKER[DT_BOOL](socket, size, buffer, nullMap)]


def _date_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'D') for val in DATA_UNPACKER[DT_DATE](socket, size, buffer, nullMap)]


def _month_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [np.datetime64(val - 1970 * 12 if val >= 0 else None, 'M') for val in DATA_UNPACKER[DT_MONTH](socket, size, buffer, nullMap)]


def _datetime_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 's') for val in DATA_UNPACKER[DT_DATETIME](socket, size, buffer, nullMap)]


def _timestamp_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'ms') for val in DATA_UNPACKER[DT_TIMESTAMP](socket, size, buffer, nullMap)]


def _time_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'ms') for val in DATA_UNPACKER[DT_TIME](socket, size, buffer, nullMap)]


def _second_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 's') for val in DATA_UNPACKER[DT_SECOND](socket, size, buffer, nullMap)]


def _minute_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'm') for val in DATA_UNPACKER[DT_MINUTE](socket, size, buffer, nullMap)]


def _nanotime_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'ns') for val in DATA_UNPACKER[DT_NANOTIME](socket, size, buffer, nullMap)]


def _nanotimestamp_handler(data_type, socket, buffer, nullMap):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'ns') for val in DATA_UNPACKER[DT_NANOTIMESTAMP](socket, size, buffer, nullMap)]


def _default_handler(data_type, socket, buffer, nullMap):
    return VECTOR_FACTORY[data_type](socket, buffer, nullMap)


TABLE_GEN_HANDLER = dict()
TABLE_GEN_HANDLER[DT_INT] = _int_handler
TABLE_GEN_HANDLER[DT_BOOL] = _bool_handler
TABLE_GEN_HANDLER[DT_DATE] = _date_handler
TABLE_GEN_HANDLER[DT_MONTH] = _month_handler
TABLE_GEN_HANDLER[DT_DATETIME] = _datetime_handler
TABLE_GEN_HANDLER[DT_TIMESTAMP] = _timestamp_handler
TABLE_GEN_HANDLER[DT_TIME] = _time_handler
TABLE_GEN_HANDLER[DT_SECOND] = _second_handler
TABLE_GEN_HANDLER[DT_MINUTE] = _minute_handler
TABLE_GEN_HANDLER[DT_NANOTIME] = _nanotime_handler
TABLE_GEN_HANDLER[DT_NANOTIMESTAMP] = _nanotimestamp_handler
TABLE_GEN_HANDLER[DT_SYMBOL] = _symbol_handler
TABLE_GEN_HANDLER[DT_STRING] = _symbol_handler

def table_generator(socket, buffer, nullMap):
    """
    Generate a pandas data frame from xxdb table object
    :param socket:
    :return:
    """
    rows = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    cols = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    tableName = read_string(socket, buffer)
    """ read column names """
    colNameDict = dict()
    colNames = []
    for i in range(cols):
        name = read_string(socket, buffer)
        colNameDict[name] = len(colNameDict)
        colNames.append(name)
    """ read columns and generate a pandas data frame"""
    df = pd.DataFrame()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df.__2xdbColumnTypes__ = dict()
    for col in colNames:
        data_form, data_type = get_form_type(socket, buffer, nullMap)

        # print(data_type)
        if data_form != DF_VECTOR:
            raise Exception("column " + col + "in table " + tableName + " must be a vector!")

        df[col] = TABLE_GEN_HANDLER.get(data_type, _default_handler)(data_type, socket, buffer, nullMap)
        # print(df)

        if data_type in [DT_BYTE, DT_SHORT]:
            data_type = DT_INT
        if data_type in [DT_SYMBOL]:
            data_type = DT_STRING
        if data_type in [DT_FLOAT]:
            data_type = DT_DOUBLE
        if data_type >= 6 and data_type <= 14:
            # TODO: improve datetime transmission
            data_type = DT_DATETIME64
        df.__2xdbColumnTypes__[col] = data_type

    return df


def matrix_generator(socket, buffer, nullMap):
    hasLabel = DATA_UNPACKER_SCALAR[DT_BYTE](socket, buffer, nullMap)
    rowLabels = None
    colLabels = None
    if hasLabel & 1 == 1:
        data_form, data_type = get_form_type(socket, buffer, nullMap)
        if data_form != DF_VECTOR:
            raise Exception("The form of matrix row labels must be vector")
        if data_type < 0 or data_type >= TYPE_NUM:
            raise Exception("Invalid data type for matrix row labels: " + data_type)
        rowLabels = VECTOR_FACTORY[data_type](socket, buffer, nullMap)

    if hasLabel & 2 == 2:
        data_form, data_type = get_form_type(socket, buffer, nullMap)
        if data_form != DF_VECTOR:
            raise Exception("The form of matrix row labels must be vector")
        if data_type < 0 or data_type >= TYPE_NUM:
            raise Exception("Invalid data type for matrix row labels: " + data_type)
        colLabels = VECTOR_FACTORY[data_type](socket, buffer, nullMap)

    flag = DATA_UNPACKER_SCALAR[DT_SHORT](socket, buffer, nullMap)
    # print(flag)
    data_type = flag & 0xff
    if data_type < 0 or data_type >= TYPE_NUM:
        raise Exception("Invalid data type for matrix row labels: " + data_type)
    rows = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    cols = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer, nullMap)
    size = rows * cols
    vals = DATA_UNPACKER[data_type](socket, size, buffer, nullMap)
    if vals is not None:
        # print(data_type,socket)
        vals = np.transpose(np.array(list(vals)).reshape(cols,rows))
    if not len(vals):
        vals = None
    return vals, rowLabels, colLabels

"""endiness: the function is reset in xxdb.connect"""
endianness = lambda x : x

""" Unpack scalar from xxdb object """
DATA_UNPACKER_SCALAR = dict()
DATA_UNPACKER_SCALAR[DT_VOID] = lambda x, y, nullMap: swap_fromxxdb(Struct('b').unpack(recvall(x, DATA_SIZE[DT_BOOL], y))[0], DT_BOOL, nullMap)
DATA_UNPACKER_SCALAR[DT_BOOL] = lambda x, y, nullMap: swap_fromxxdb(Struct('b').unpack(recvall(x, DATA_SIZE[DT_BOOL], y))[0], DT_BOOL, nullMap)
DATA_UNPACKER_SCALAR[DT_BYTE] = lambda x, y, nullMap: swap_fromxxdb(Struct('b').unpack((recvall(x, DATA_SIZE[DT_BYTE], y)))[0], DT_BYTE, nullMap)
DATA_UNPACKER_SCALAR[DT_SHORT] = lambda x, y, nullMap: swap_fromxxdb(Struct(endianness('h')).unpack(recvall(x, DATA_SIZE[DT_SHORT], y))[0], DT_SHORT, nullMap)
DATA_UNPACKER_SCALAR[DT_INT] = lambda x, y, nullMap: swap_fromxxdb(Struct(endianness('i')).unpack((recvall(x, DATA_SIZE[DT_INT], y)))[0], DT_INT, nullMap)
DATA_UNPACKER_SCALAR[DT_LONG] = lambda x, y, nullMap: swap_fromxxdb(Struct(endianness('q')).unpack((recvall(x, DATA_SIZE[DT_LONG], y)))[0], DT_LONG, nullMap)
DATA_UNPACKER_SCALAR[DT_DATE] = lambda x, y, nullMap: Date(Struct(endianness('i')).unpack((recvall(x, DATA_SIZE[DT_DATE], y)))[0])
DATA_UNPACKER_SCALAR[DT_MONTH] = lambda x, y, nullMap: Month(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_MONTH], y))[0])
DATA_UNPACKER_SCALAR[DT_TIME] = lambda x, y, nullMap: Time(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_TIME], y))[0])
DATA_UNPACKER_SCALAR[DT_MINUTE] = lambda x, y, nullMap: Minute(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_MINUTE], y))[0])
DATA_UNPACKER_SCALAR[DT_SECOND] = lambda x, y, nullMap: Second(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_SECOND], y))[0])
DATA_UNPACKER_SCALAR[DT_DATETIME] = lambda x, y, nullMap: Datetime(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_DATETIME], y))[0])
DATA_UNPACKER_SCALAR[DT_TIMESTAMP] = lambda x, y, nullMap: Timestamp(Struct(endianness('q')).unpack((recvall(x, DATA_SIZE[DT_TIMESTAMP], y)))[0])
DATA_UNPACKER_SCALAR[DT_NANOTIME] = lambda x, y, nullMap: NanoTime(Struct(endianness('q')).unpack(recvall(x, DATA_SIZE[DT_NANOTIME], y))[0])
DATA_UNPACKER_SCALAR[DT_NANOTIMESTAMP] = lambda x, y, nullMap: NanoTimestamp(Struct(endianness('q')).unpack((recvall(x, DATA_SIZE[DT_NANOTIMESTAMP], y)))[0])
DATA_UNPACKER_SCALAR[DT_DATETIME64] = lambda x, y, nullMap: NanoTimestamp(Struct(endianness('q')).unpack(recvall(x, DATA_SIZE[DT_NANOTIMESTAMP], y))[0])
DATA_UNPACKER_SCALAR[DT_FLOAT] = lambda x, y, nullMap: swap_fromxxdb(Struct(endianness('f')).unpack(recvall(x, DATA_SIZE[DT_FLOAT], y))[0], DT_FLOAT, nullMap)
DATA_UNPACKER_SCALAR[DT_DOUBLE] = lambda x, y, nullMap: swap_fromxxdb(Struct(endianness('d')).unpack((recvall(x, DATA_SIZE[DT_DOUBLE], y)))[0], DT_DOUBLE, nullMap)
DATA_UNPACKER_SCALAR[DT_SYMBOL] = lambda x, y, nullMap: read_string(x, y)
DATA_UNPACKER_SCALAR[DT_STRING] = lambda x, y, nullMap: read_string(x, y)
DATA_UNPACKER_SCALAR[DT_ANY] = lambda x, y, nullMap: None
DATA_UNPACKER_SCALAR[DT_DICTIONARY] = lambda x, y, nullMap: None
DATA_UNPACKER_SCALAR[DT_OBJECT] = lambda x, y, nullMap: None

DATA_UNPACKER = dict()
DATA_UNPACKER[DT_VOID] = lambda x, y, z, nullMap: map(lambda z: swap_fromxxdb(z, DT_BOOL, nullMap), Struct(str(y)+'b').unpack(recvall(x, DATA_SIZE[DT_BOOL]*y, z)))
DATA_UNPACKER[DT_BOOL] = lambda x, y, z, nullMap: map(lambda z: swap_fromxxdb(z, DT_BOOL, nullMap), Struct(str(y)+'b').unpack(recvall(x, DATA_SIZE[DT_BOOL]*y, z)))
DATA_UNPACKER[DT_BYTE] = lambda x, y, z, nullMap: map(lambda z: swap_fromxxdb(z, DT_BYTE, nullMap), Struct(str(y)+'b').unpack(recvall(x, DATA_SIZE[DT_BYTE]*y, z)))
DATA_UNPACKER[DT_SHORT] = lambda x, y, z, nullMap: map(lambda z: swap_fromxxdb(z, DT_SHORT, nullMap), Struct(endianness(str(y)+'h')).unpack(recvall(x, DATA_SIZE[DT_SHORT]*y, z)))
DATA_UNPACKER[DT_INT] = lambda x, y, z, nullMap: list(map(lambda z: swap_fromxxdb(z, DT_INT, nullMap), Struct(endianness(str(y)+'i')).unpack(recvall(x, DATA_SIZE[DT_INT]*y, z))))
DATA_UNPACKER[DT_LONG] = lambda x, y, z, nullMap: map(lambda z: swap_fromxxdb(z, DT_LONG, nullMap), Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_LONG]*y, z))))
DATA_UNPACKER[DT_DATE] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_DATE]*y, z)))
DATA_UNPACKER[DT_MONTH] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_MONTH]*y, z)))
DATA_UNPACKER[DT_TIME] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_TIME]*y, z)))
DATA_UNPACKER[DT_MINUTE] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_MINUTE]*y, z)))
DATA_UNPACKER[DT_SECOND] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_SECOND]*y, z)))
DATA_UNPACKER[DT_DATETIME] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_DATETIME]*y, z)))
DATA_UNPACKER[DT_TIMESTAMP] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_TIMESTAMP]*y, z)))
DATA_UNPACKER[DT_NANOTIME] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_NANOTIME]*y, z)))
DATA_UNPACKER[DT_NANOTIMESTAMP] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_NANOTIMESTAMP]*y, z)))
DATA_UNPACKER[DT_DATETIME64] = lambda x, y, z, nullMap: Struct(endianness(str(y)+'q')).unpack(recvall(x, DATA_SIZE[DT_NANOTIMESTAMP]*y, z))
DATA_UNPACKER[DT_FLOAT] = lambda x, y, z, nullMap: map(lambda z: swap_fromxxdb(z, DT_FLOAT, nullMap), Struct(endianness(str(y)+'f')).unpack(recvall(x, DATA_SIZE[DT_FLOAT]*y, z)))
DATA_UNPACKER[DT_DOUBLE] = lambda x, y, z, nullMap: list(map(lambda z: swap_fromxxdb(z, DT_DOUBLE, nullMap), Struct(endianness(str(y)+'d')).unpack((recvall(x, DATA_SIZE[DT_DOUBLE]*y, z)))))
DATA_UNPACKER[DT_SYMBOL] = lambda x, y, z, nullMap: None
DATA_UNPACKER[DT_STRING] = lambda x, y, z, nullMap: None
DATA_UNPACKER[DT_ANY] = lambda x, y, z, nullMap: None
DATA_UNPACKER[DT_DICTIONARY] = lambda x, y, z, nullMap: None
DATA_UNPACKER[DT_OBJECT] = lambda x, y, z, nullMap: None

""" dictionary of functions for making numpy arrays from xxdb vectors"""
VECTOR_FACTORY = dict()
VECTOR_FACTORY[DT_VOID] = lambda x, y, nullMap:[]
VECTOR_FACTORY[DT_BOOL] = lambda x, y, nullMap: vec_generator(x, DT_BOOL, y, nullMap)
VECTOR_FACTORY[DT_BYTE] = lambda x, y, nullMap: vec_generator(x, DT_BYTE, y, nullMap)
VECTOR_FACTORY[DT_SHORT] = lambda x, y, nullMap: vec_generator(x, DT_SHORT, y, nullMap)
VECTOR_FACTORY[DT_INT] = lambda x, y, nullMap: vec_generator(x, DT_INT, y, nullMap)
VECTOR_FACTORY[DT_LONG] = lambda x, y, nullMap: vec_generator(x, DT_LONG, y, nullMap)
VECTOR_FACTORY[DT_DATE] = lambda x, y, nullMap: list(map(Date, vec_generator(x, DT_DATE, y, nullMap)))
VECTOR_FACTORY[DT_MONTH] = lambda x, y, nullMap: list(map(Month, vec_generator(x, DT_MONTH, y, nullMap)))
VECTOR_FACTORY[DT_TIME] = lambda x, y, nullMap: list(map(Time, vec_generator(x, DT_TIME, y, nullMap)))
VECTOR_FACTORY[DT_MINUTE] = lambda x, y, nullMap: list(map(Minute, vec_generator(x, DT_MINUTE, y, nullMap)))
VECTOR_FACTORY[DT_SECOND] = lambda x, y, nullMap: list(map(Second, vec_generator(x, DT_SECOND, y, nullMap)))
VECTOR_FACTORY[DT_DATETIME] = lambda x, y, nullMap: list(map(Datetime, vec_generator(x, DT_DATETIME, y, nullMap)))
VECTOR_FACTORY[DT_TIMESTAMP] = lambda x, y, nullMap: list(map(Timestamp, vec_generator(x, DT_TIMESTAMP, y, nullMap)))
VECTOR_FACTORY[DT_NANOTIME] = lambda x, y, nullMap: list(map(NanoTime, vec_generator(x, DT_NANOTIME, y, nullMap)))
VECTOR_FACTORY[DT_NANOTIMESTAMP] = lambda x, y, nullMap: list(map(NanoTimestamp, vec_generator(x, DT_NANOTIMESTAMP, y, nullMap)))
VECTOR_FACTORY[DT_DATETIME64] = lambda x, y, nullMap: list(map(NanoTimestamp, vec_generator(x, DT_DATETIME64, y, nullMap)))
VECTOR_FACTORY[DT_FLOAT] = lambda x, y, nullMap: vec_generator(x, DT_FLOAT, y, nullMap)
VECTOR_FACTORY[DT_DOUBLE] = lambda x, y, nullMap: vec_generator(x, DT_DOUBLE, y, nullMap)
VECTOR_FACTORY[DT_SYMBOL] = lambda x, y, nullMap: list(map(lambda z: swap_fromxxdb(z, DT_SYMBOL, nullMap), vec_generator(x, DT_SYMBOL, y, nullMap)))
VECTOR_FACTORY[DT_STRING] = lambda x, y, nullMap: list(map(lambda z: swap_fromxxdb(z, DT_STRING, nullMap), vec_generator(x, DT_STRING, y, nullMap)))
VECTOR_FACTORY[DT_ANY] = vector_factory_any

""" dictionary of functions for loading different forms of data from xxdb api"""
DATA_LOADER = dict()
DATA_LOADER[DF_SCALAR] = DATA_UNPACKER_SCALAR
DATA_LOADER[DF_VECTOR] = VECTOR_FACTORY
DATA_LOADER[DF_PAIR] = VECTOR_FACTORY
DATA_LOADER[DF_SET] = lambda x, y, nullMap: set_generator(x, y, nullMap)
DATA_LOADER[DF_DICTIONARY] = lambda x, y, nullMap: dict_generator(x, y, nullMap)
DATA_LOADER[DF_TABLE] = lambda x, y, nullMap: table_generator(x, y, nullMap)
DATA_LOADER[DF_MATRIX] = lambda x, y, nullMap: matrix_generator(x, y, nullMap)

""" pack from python scalar"""
DATA_PACKER_SCALAR = dict()
DATA_PACKER_SCALAR[DT_BOOL] = lambda x: Struct('b').pack(swap_toxxdb_int(x, DT_BOOL))
DATA_PACKER_SCALAR[DT_SHORT] = lambda x: Struct(endianness('h')).pack(swap_toxxdb_int(x, DT_SHORT))
DATA_PACKER_SCALAR[DT_INT] = lambda x: Struct(endianness('i')).pack(swap_toxxdb_int(x, DT_INT))
DATA_PACKER_SCALAR[DT_LONG] = lambda x: Struct(endianness('q')).pack(swap_toxxdb_int(x, DT_LONG))
DATA_PACKER_SCALAR[DT_DOUBLE] = lambda x: Struct(endianness('d')).pack(swap_toxxdb(x, DT_DOUBLE))
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
DATA_PACKER_SCALAR[DT_DATETIME64] = lambda x: x.tobytes()
# DATA_PACKER_SCALAR[DT_DATETIME64] = lambda x: Struct(endianness('q')).pack(x.astype(np.int64))
# ATTENTION:
# In DT_DATETIME64 packer, the byte array of a nanotimestamp is directly copied from the underlying memory of datetime64
# datetime64 has a identical memory layout with time types in DolphinDB


""" pack from numpy 1D array """
DATA_PACKER = dict()
DATA_PACKER[DT_BOOL] = lambda x: Struct(endianness("%db" % x.size)).pack(*map(lambda y: swap_toxxdb_int(y, DT_BOOL), x))
DATA_PACKER[DT_INT] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: swap_toxxdb_int(y, DT_INT), x))
DATA_PACKER[DT_LONG] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: swap_toxxdb_int(y, DT_LONG), x))
DATA_PACKER[DT_DOUBLE] = lambda x: Struct(endianness("%dd" % x.size)).pack(*map(lambda y: swap_toxxdb(y, DT_DOUBLE), x))
DATA_PACKER[DT_STRING] = lambda x: b''.join(map(lambda y: bytes(y)+b'\x00', x))
DATA_PACKER[DT_DATE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_MONTH] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_TIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_MINUTE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_SECOND] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_DATETIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_TIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_NANOTIME] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_NANOTIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_DATETIME64] = lambda x: b''.join(list(map(lambda y: y.tobytes(), x)))
# DATA_PACKER[DT_DATETIME64] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.astype(np.int64), x))

""" pack from numpy multi-dimensional array """
DATA_PACKER2D = dict()
DATA_PACKER2D[DT_BOOL] = lambda x: Struct(endianness("%db" % x.size)).pack(*map(lambda y:swap_toxxdb_int(y, DT_BOOL), x.T.flat))
DATA_PACKER2D[DT_INT] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:swap_toxxdb_int(y, DT_INT), x.T.flat))
DATA_PACKER2D[DT_LONG] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:swap_toxxdb_int(y, DT_LONG), x.T.flat))
DATA_PACKER2D[DT_DOUBLE] = lambda x: Struct(endianness("%dd" % x.size)).pack(*map(lambda y: swap_toxxdb(y, DT_DOUBLE), x.T.flat))
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
DATA_PACKER2D[DT_DATETIME64] = lambda x: b''.join(list(map(lambda y: y.tobytes(), x.T.flat)))
# DATA_PACKER2D[DT_DATETIME64] =lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.astype(np.int64), x.T.flat))
