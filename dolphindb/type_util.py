import numpy as np
import pandas as pd
from . import date_util as d
import type_util as t
import dolphindb.date_util as dd
from dolphindb.date_util import *
from dolphindb.pair import Pair

## python data type to xxdb data type mapping
DBTYPE = dict()
DBTYPE[bool] = DT_BOOL
DBTYPE[int] = DT_INT
DBTYPE[long] = DT_LONG
DBTYPE[float] = DT_DOUBLE
DBTYPE[str] = DT_STRING
DBTYPE[Date] = DT_DATE
DBTYPE[Month] = DT_MONTH
DBTYPE[Time] = DT_TIME
DBTYPE[Minute] = DT_MINUTE
DBTYPE[Second] = DT_SECOND
DBTYPE[Datetime] = DT_DATETIME
DBTYPE[Timestamp] = DT_TIMESTAMP
DBTYPE[NanoTime] = DT_NANOTIME
DBTYPE[NanoTimestamp] = DT_NANOTIMESTAMP

## numpy dtype.name to xxdb data type mapping
DBTYPE['bool'] = DT_BOOL
DBTYPE['int32'] = DT_INT
DBTYPE['int64'] = DT_LONG
DBTYPE['float32'] = DT_FLOAT
DBTYPE['float64'] = DT_DOUBLE
DBTYPE['string'] = DT_STRING
DBTYPE['Date'] = DT_DATE
DBTYPE['Month'] = DT_MONTH
DBTYPE['Time'] = DT_TIME
DBTYPE['Minute'] = DT_MINUTE
DBTYPE['Second'] = DT_SECOND
DBTYPE['Datetime'] = DT_DATETIME
DBTYPE['Timestamp'] = DT_TIMESTAMP
DBTYPE['NanoTime'] = DT_NANOTIME
DBTYPE['NanoTimestamp'] = DT_NANOTIMESTAMP
DBTYPE['datetime64[ns]']=DT_DATETIME64
DBTYPE['datetime64[D]']=DT_DATETIME64
class nan(object):
    def __init__(self, type):
        self.__type = type

    @property
    def type(self):
        return self.__type

    def __repr__(self): return 'nan'

# python doesn't have type-dependent Nan, we have to support different nan types here
# for temporal types, nan support is built inside class
byteNan = nan(DT_BYTE)
boolNan = nan(DT_BOOL)
shortNan = nan(DT_SHORT)
intNan = nan(DT_INT)
floatNan = nan(DT_FLOAT)
doubleNan = nan(DT_DOUBLE)


def swap(val, dt_type=None):
    # when write to xxdb, for a null object of a given type, returns xxdb representation
    if isinstance(val, nan): return DBNAN[val.type]
    # when read number types from xxdb, if xxdb object is nan, create a null instance.
    if dt_type and val == DBNAN[dt_type]: return nan(dt_type)  #nan(dt_type) : changed from nan(dt_type) to np.nan as this is used for numpy
    return val


def  is_scalar(obj):
    if (isinstance(obj, list)
        or isinstance(obj, tuple)
        or isinstance(obj, set)
        or isinstance(obj, Pair)
        or isinstance(obj, dict)
        or isinstance(obj, np.ndarray)
        or isinstance(obj, pd.core.frame.DataFrame)):
        return False

    return True


def determine_form_type(obj):
    """
    determine the database form and type for the given python object
    :return: a tuple
    """
    if isinstance(obj, list):
        dbForm = DF_VECTOR

        if len(obj):
            try:
                dbType = obj[0].type if isinstance(obj[0],t.nan) else DBTYPE[type(obj[0])]
                for val in obj:
                    dbType2 = val.type if isinstance(val, t.nan) else DBTYPE[type(val)]
                    if dbType != DT_ANY and dbType2 != dbType:
                        dbType = DT_ANY
                        break
            except KeyError:
                dbType = obj[0].dtype.name if isinstance(obj[0], t.nan) else DBTYPE[obj[0].dtype.name]
                for val in obj:
                    dbType2 = val.dtype.name if isinstance(val, t.nan) else DBTYPE[val.dtype.name]
                    if dbType != DT_ANY and dbType2 != dbType:
                        dbType = DT_ANY
                        break
        else:
            raise RuntimeError("function argument with list type cannot be empty")
    elif isinstance(obj, dict):
        dbForm = DF_DICTIONARY
        if len(obj):
            if is_scalar(list(obj.values())[0]):
                dbType = DBTYPE[type(list(obj.values())[0])]
            else:
                raise RuntimeError("dict can only hold scalars")
        else:
            raise RuntimeError("function argument with dict type cannot be empty")
    elif isinstance(obj, np.ndarray):
        if obj.ndim > 2: # we need further check each element should be just a scalar
            raise RuntimeError("only support rank 1 or 2 numpy array!")
        dbForm = DF_VECTOR if obj.ndim == 1 else DF_MATRIX
        if obj.dtype.name == 'object':
            dbType = DBTYPE[type(obj[0])] if obj.ndim ==1 else DBTYPE[type(obj[0][0])]
        else:
            dbType = DBTYPE[obj.dtype.name] if not obj.dtype.name.startswith('str') else DBTYPE['string']
    elif isinstance(obj, pd.core.frame.DataFrame):
        dbForm = DF_TABLE
        dbType = DT_DICTIONARY
    elif isinstance(obj, set):
        dbForm = DF_SET
        dbType = None
        for key in obj:
            if not dbType:
                dbType = DBTYPE[type(key)]
            else:
                if dbType != DBTYPE[type(key)]:
                    raise RuntimeError("There should be only one data type in set")
    elif isinstance(obj, Pair):
        dbForm = DF_PAIR
        dbType = DBTYPE[obj.type]
    elif (isinstance(obj, bool)
        or isinstance(obj, int)
        or isinstance(obj, long)
        or isinstance(obj, float)
        or isinstance(obj, str)
        or isinstance(obj, d.Date)
        or isinstance(obj, d.Month)
        or isinstance(obj, d.Time)
        or isinstance(obj, d.Minute)
        or isinstance(obj, d.Second)
        or isinstance(obj, d.Datetime)
        or isinstance(obj, d.Timestamp)
        or isinstance(obj, d.NanoTime)
        or isinstance(obj, d.NanoTimestamp)):
        dbForm = DF_SCALAR
        dbType = DBTYPE[type(obj)]
    elif (isinstance(obj, t.nan)):
        dbForm = DF_SCALAR
        dbType = obj.type
    else:
        raise RuntimeError("Sending type " + type(obj).__name__ + " is not supported yet!")
    return (dbForm, dbType)