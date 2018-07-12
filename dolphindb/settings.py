FORM_NUM = 7
# DATA FORM
DF_SCALAR = 0
DF_VECTOR = 1
DF_PAIR = 2
DF_MATRIX = 3
DF_SET = 4
DF_DICTIONARY = 5
DF_TABLE = 6
DF_CHART = 7


TYPE_NUM = 27
# DATA TYPE
DT_VOID = 0
DT_BOOL = 1
DT_BYTE = 2
DT_SHORT = 3
DT_INT = 4
DT_LONG = 5
DT_DATE = 6
DT_MONTH = 7
DT_TIME = 8
DT_MINUTE = 9
DT_SECOND = 10
DT_DATETIME = 11
DT_TIMESTAMP = 12
DT_NANOTIME = 13
DT_NANOTIMESTAMP = 14
DT_FLOAT = 15
DT_DOUBLE = 16
DT_SYMBOL = 17
DT_STRING = 18
DT_FUNCTIONDEF = 19
DT_HANDLE = 20
DT_CODE=21
DT_DATASOURCE=22
DT_RESOURCE=23
DT_ANY = 24
DT_DICTIONARY = 25
DT_OBJECT = 26

DT_DATETIME64=100

# Data type size
DATA_SIZE = dict()
DATA_SIZE[DT_VOID] = 0
DATA_SIZE[DT_BOOL] = 1
DATA_SIZE[DT_BYTE] = 1
DATA_SIZE[DT_SHORT] = 2
DATA_SIZE[DT_INT] = 4
DATA_SIZE[DT_LONG] = 8
DATA_SIZE[DT_DATE] = 4
DATA_SIZE[DT_MONTH] = 4
DATA_SIZE[DT_TIME] = 4
DATA_SIZE[DT_MINUTE] = 4
DATA_SIZE[DT_SECOND] = 4
DATA_SIZE[DT_DATETIME] = 4
DATA_SIZE[DT_TIMESTAMP] = 8
DATA_SIZE[DT_NANOTIME] = 8
DATA_SIZE[DT_NANOTIMESTAMP] = 8
DATA_SIZE[DT_FLOAT] = 4
DATA_SIZE[DT_DOUBLE] = 8
DATA_SIZE[DT_SYMBOL] = 0
DATA_SIZE[DT_STRING] = 0
DATA_SIZE[DT_ANY] = 0
DATA_SIZE[DT_DICTIONARY] = 0
DATA_SIZE[DT_OBJECT] = 0

## xxdb NAN values
DBNAN = dict()
DBNAN[DT_BYTE] = -128
DBNAN[DT_BOOL] = -128
DBNAN[DT_SHORT] = -32768
DBNAN[DT_INT] = -2147483648
DBNAN[DT_LONG] = -9223372036854775808L
DBNAN[DT_FLOAT] = -3.4028234663852886e+38
DBNAN[DT_DOUBLE] = -1.7976931348623157e+308
DBNAN[DT_SYMBOL] = ''
DBNAN[DT_STRING] = ''
DBNAN[DT_DATE] = -2147483648
DBNAN[DT_MONTH] = -2147483648
DBNAN[DT_TIME] = -2147483648
DBNAN[DT_MINUTE] = -2147483648
DBNAN[DT_SECOND] = -2147483648
DBNAN[DT_DATETIME] = -2147483648
DBNAN[DT_TIMESTAMP] = -9223372036854775808L
DBNAN[DT_NANOTIME] = -9223372036854775808L
DBNAN[DT_NANOTIMESTAMP] = -9223372036854775808L

