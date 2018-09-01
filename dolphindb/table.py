from pandas import DataFrame
from dolphindb.vector import Vector
from dolphindb.vector import FilterCond
import uuid
import copy
import __builtin__
import re
import inspect


def _generate_tablename():
    return "T" + uuid.uuid4().hex[:8]


def _getFuncName(f):
    if isinstance(f, str):
        return f
    else: return f.__name__


class Table(object):
    def __init__(self,dbPath=None, data=None,  tableAliasName=None, partitions=[], inMem=False, schemaInited=False, s=None):
        self.__having = None
        self.__top = None
        self.__exec = False
        if s is None:
            raise RuntimeError("session must be provided")
        self.__tableName = _generate_tablename() if not isinstance(data, str) else data
        self.__session = s  # type: session
        self.__schemaInited = schemaInited
        if not isinstance(partitions, list):
            raise RuntimeError(
                'Column names must be passed in as a list')
        if isinstance(data, dict) or isinstance(data, DataFrame):
            df = data if isinstance(data, DataFrame) else DataFrame(data)
            self.__session.upload({self.__tableName : df})
            self.vecs = {}

            # self.__session.run("share %s as S%s" % (self.__tableName, self.__tableName))
            for colName in df.keys():
                self.vecs[colName] = Vector(name=colName, tableName=self.__tableName, s=self.__session)
            self._setSelect(list(self.vecs.keys()))
        elif isinstance(data, str):
            if dbPath:
                if tableAliasName:
                    self.__tableName = tableAliasName
                else:
                    self.__tableName = data
                runstr = '{tableName} = loadTable("{dbPath}", "{data}",{partitions},{inMem})'
                fmtDict = dict()
                fmtDict['tableName'] = self.__tableName
                fmtDict['dbPath'] = dbPath
                fmtDict['data'] = data
                if len(partitions) and type(partitions[0]) is not str:
                    fmtDict['partitions'] = ('[' + ','.join(str(x) for x in partitions) + ']') if len(partitions) else ""
                else:
                    fmtDict['partitions'] = ('["' + '","'.join(partitions) + '"]') if len(partitions) else ""
                fmtDict['inMem'] = str(inMem).lower()
                runstr = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
                # print(runstr)
                self.__session.run(runstr)
                # runstr = '%s = select * from %s' %(self.__tableName, self.__tableName)
                # self.__session.run(runstr)
            else:
                pass
        else:
            raise RuntimeError("data must be a remote dolphindb table name or dict or DataFrame")
        self._init_schema()

    def __deepcopy__(self, memodict={}):
        newTable = Table(data=self.__tableName, schemaInited=True, s=self.__session)
        try:
            newTable.vecs = copy.deepcopy(self.vecs, memodict)
        except AttributeError:
            pass

        try:
            newTable.__schemaInited = copy.deepcopy(self.__schemaInited, memodict)
        except AttributeError:
            pass

        try:
            newTable.__select = copy.deepcopy(self.__select, memodict)
        except AttributeError:
            pass

        try:
            newTable.__where = copy.deepcopy(self.__where, memodict)
        except AttributeError:
            pass

        try:
            newTable.__groupby = copy.deepcopy(self.__groupby, memodict)
        except AttributeError:
            pass

        try:
            newTable.__contextby= copy.deepcopy(self.__contextby, memodict)
        except AttributeError:
            pass

        try:
            newTable.__having = copy.deepcopy(self.__having, memodict)
        except AttributeError:
            pass

        try:
            newTable.__sort = copy.deepcopy(self.__sort, memodict)
        except AttributeError:
            pass

        return newTable

    def _setSelect(self, cols):
        self.__schemaInited = True
        if isinstance(cols, tuple):
            cols = list(cols)
        if isinstance(cols, list) is False:
            cols = [cols]
        self.__select = cols

    def _init_schema(self):
        if self.__schemaInited is True:
            return
        colNames = self.__session.run("colNames(%s)" % self.__tableName)
        # schema = self.__session.run("schema(%s)" % self.__tableName)  # type: dict
        # colDefs = schema.get('colDefs')  # type: DataFrame
        self.vecs = {}
        self.__columns = colNames
        if colNames is not None:
            for colName in colNames:
                self.vecs[colName] = Vector(name=colName, tableName=self.__tableName, s=self.__session)
            self._setSelect(colNames)

    def __getattr__(self, item):
        vecs = object.__getattribute__(self, "vecs")
        if item not in vecs:
            return object.__getattribute__(self, item)
        else:
            return vecs[item]

    def __getitem__(self, colOrCond):
        if isinstance(colOrCond, FilterCond):
            return self.where(colOrCond)
        else:
            return self.select(colOrCond)

    def tableName(self):
        return self.__tableName

    def _setTableName(self, tableName):
        self.__tableName = tableName

    def session(self):
        return self.__session

    def _addWhereCond(self, conds):
        try:
            _ = self.__where
        except AttributeError:
            self.__where = []

        if isinstance(conds, list) or isinstance(conds, tuple):
            self.__where.extend([str(x) for x in conds])
        else:
            self.__where.append(str(conds))

    def _setSort(self, bys):
        if isinstance(bys, list) or isinstance(bys, tuple):
            self.__sort = [str(x) for x in bys]
        else:
            self.__sort = [str(bys)]

    def _setTop(self, num):
        self.__top = str(num)

    def _setWhere(self, where):
        self.__where = where

    def select(self, cols):
        selectTable = copy.deepcopy(self)
        selectTable._setSelect(cols)
        return selectTable

    def where(self, conds):
        whereTable = copy.deepcopy(self)
        whereTable._addWhereCond(conds)
        return whereTable

    def _setGroupby(self, groupby):
        try:
            _ = self.__contextby
            raise RuntimeError('multiple context/group-by are not allowed ')
        except AttributeError:
            self.__groupby = groupby

    def _setContextby(self, contextby):
        try:
            _ = self.__groupby
            raise RuntimeError('multiple context/group-by are not allowed ')
        except AttributeError:
            self.__contextby = contextby

    def _setHaving(self, having):
        self.__having = having

    def groupby(self, cols):
        groupbyTable = copy.deepcopy(self)
        groupby = TableGroupby(groupbyTable, cols)
        groupbyTable._setGroupby(groupby)
        return groupby

    def contextby(self, cols):
        contextbyTable = copy.deepcopy(self)
        contextby = TableContextby(contextbyTable, cols)
        contextbyTable._setContextby(contextby)
        return contextby

    def sort(self, bys):
        sortTable = copy.deepcopy(self)
        sortTable._setSort(bys)
        return sortTable

    def top(self, num):
        topTable = copy.deepcopy(self)
        topTable._setTop(num)
        return topTable

    def selectAsVector(self, colName):
        if colName:
            self._setSelect(colName)
        pattern = re.compile("select", re.IGNORECASE)
        query = pattern.sub('exec', self.showSQL())
        return self.__session.run(query)

    def count(self):
        return self.__session.run("exec count(*) from %s" % self.__tableName)

    @property
    def columns(self):
        if not self.__columns:
            cols = self.__session.run("columnNames(%s)" % self.__tableName)
            self.__columns = cols
        return self.__columns

    @property
    def schema(self):
        schema = self.__session.run("schema(%s)" % self.__tableName)  # type: dict
        colDefs = schema.get('colDefs')  # type: DataFrame
        return colDefs

    def pivotby(self, index, column, value, aggFunc=None):
        """
        create a pivot table.
        see www.dolphindb.com/help/pivotby.html

        :param index: the result table has the same number of rows as # unique values on this column
        :param column: the result table has the same number of columns as # unique values on this column
        :param value: column to aggregate
        :param aggFunc: aggregation function, default lambda x: x
        :return: TablePivotBy object
        """
        pivotByTable = copy.deepcopy(self)
        return TablePivotBy(pivotByTable, index, column, value, aggFunc)

    def merge(self, right, how='inner', on=None, left_on=None, right_on=None, sort=False):
        """
        Merge two tables using ANSI SQL style join semantics.

        :param right: right table or the name of the right table on remote server
        :param how: one of {'left', 'right', 'outer', 'inner'}, default 'inner'
            left: see http://www.dolphindb.com/help/index.html?leftjoin.html
            right: see http://www.dolphindb.com/help/index.html?leftjoin.html
            outer: see http://www.dolphindb.com/help/index.html?fulljoin.html
            inner: see http://www.dolphindb.com/help/index.html?equaljoin.html
        :param on: column or list of columns
            columns to join on, must be present on both tables.
        :param left_on: column or list of columns
            left table columns to join on, default to on if None
        :param right_on: column or list of columns
            right table columns to join on, default to on if None
        :param sort: True or False
        :return: merged Table object
        """
        howMap = {'inner': 'ej',
                  'left': 'lj',
                  'right': 'lj',
                  'outer': 'fj'}
        joinFunc = howMap[how]
        joinFuncPrefix = '' if sort is False or joinFunc == 'fj' else 's'
        leftTableName = self.tableName()
        rightTableName = right.tableName() if isinstance(right, Table) else right
        if how == 'right':
            leftTableName, rightTableName = rightTableName, leftTableName
            left_on, right_on = right_on, left_on

        if on is not None and not isinstance(on, list) and not isinstance(on, tuple):
            on = [on]
        if left_on is not None and not isinstance(left_on, list) and not isinstance(left_on, tuple):
            left_on = [left_on]
        if right_on is not None and not isinstance(right_on, list) and not isinstance(right_on, tuple):
            right_on = [right_on]

        if on is not None:
            left_on, right_on = on, on
        elif left_on is None and right_on is None:
            raise Exception('at least one of {\'on\', \'left_on\', \'right_on\'} must be present')
        elif left_on is not None and right_on is not None and len(left_on) != len(right_on):
            raise Exception('\'left_on\' must have the same length as \'right_on\'')

        if left_on is None and right_on is not None:
            left_on = right_on
        if right_on is None and left_on is not None:
            right_on = left_on

        leftColumnNames = ''.join(['`' + x for x in left_on])
        rightColumnNames = ''.join(['`' + x for x in right_on])
        finalTableName = '%s(%s,%s,%s,%s)' % (joinFuncPrefix + joinFunc, leftTableName, rightTableName, leftColumnNames, rightColumnNames)
        self._init_schema()
        right._init_schema()
        joinTable = copy.deepcopy(self)
        # leftAliasPrefix = 'lhs_' if how != 'right' else 'rhs_'
        # rightAliasPrefix = 'rhs_' if how != 'right' else 'lhs_'
        # leftSelectCols = [leftTableName + '.' + col + ' as ' + leftTableName + "_" + col for col in self._getSelect()]
        # rightSelectCols = [rightTableName + '.' + col + ' as ' + rightTableName + "_" + col for col in right._getSelect()]
        leftSelectCols = self._getSelect()
        rightSelectCols = [rightTableName + '.' + col + ' as ' + rightTableName + "_" + col for col in
                           right._getSelect() if col in self._getSelect()]
        joinTable._setTableName(finalTableName)
        joinTable._setSelect(leftSelectCols + rightSelectCols)
        return joinTable

    def merge_asof(self, right, on=None, left_on=None, right_on=None):
        """
        As-of merge two tables on some columns.
        see http://www.dolphindb.com/help/index.html?asofjoin.html

        :param right: right table or the name of the right table on remote server
        :param on: column or list of columns
            columns to join on, must be present on both tables.
        :param left_on: column or list of columns
            left table columns to join on, default to on if None
        :param right_on: column or list of columns
            right table columns to join on, default to on if None
        :return: merged Table object
        """

        leftTableName = self.tableName()
        rightTableName = right.tableName() if isinstance(right, Table) else right

        if on is not None and not isinstance(on, list) and not isinstance(on, tuple):
            on = [on]
        if left_on is not None and not isinstance(left_on, list) and not isinstance(left_on, tuple):
            left_on = [left_on]
        if right_on is not None and not isinstance(right_on, list) and not isinstance(right_on, tuple):
            right_on = [right_on]

        if on is not None:
            left_on, right_on = on, on
        elif left_on is None and right_on is None:
            raise Exception('at least one of {\'on\', \'left_on\', \'right_on\'} must be present')
        elif left_on is not None and right_on is not None and len(left_on) != len(right_on):
            raise Exception('\'left_on\' must have the same length as \'right_on\'')

        if left_on is None and right_on is not None:
            left_on = right_on
        if right_on is None and left_on is not None:
            right_on = left_on

        leftColumnNames = ''.join(['`' + x for x in left_on])
        rightColumnNames = ''.join(['`' + x for x in right_on])
        finalTableName = 'aj(%s,%s,%s,%s)' % (leftTableName, rightTableName, leftColumnNames, rightColumnNames)
        self._init_schema()
        right._init_schema()
        joinTable = copy.deepcopy(self)
        # leftAliasPrefix = 'lhs_'
        # rightAliasPrefix = 'rhs_'
        # leftSelectCols = [leftTableName + '.' + col + ' as ' + leftTableName +"_" + col for col in self._getSelect()]
        # rightSelectCols = [rightTableName + '.' + col + ' as ' + rightTableName + "_" + col for col in right._getSelect()]
        leftSelectCols = self._getSelect()
        rightSelectCols = [rightTableName + '.' + col + ' as ' + rightTableName + "_" + col   for col in right._getSelect() if col in self._getSelect()]
        joinTable._setTableName(finalTableName)
        joinTable._setSelect(leftSelectCols + rightSelectCols)
        return joinTable

    def _getSelect(self):
        return self.__select

    def _assembleSelect(self):
        try:
            if len(self.__select):
                return ','.join(self.__select)
            else:
                return '*'
        except AttributeError:
            return '*'

    def _assembleWhere(self):
        try:
            return 'where ' + ' and '.join(self.__where)
        except AttributeError:
            return ''

    def _assembleGroupbyOrContextby(self):
        try:
            return 'group by ' + ','.join(self.__groupby)
        except AttributeError:
            try:
                return 'context by ' + ','.join(self.__contextby)
            except AttributeError:
                return ''

    def _assembleOrderby(self):
        try:
            return 'order by ' + ','.join(self.__sort)
        except AttributeError:
            return ''

    def showSQL(self):
        import re
        queryFmt = 'select {top} {select} from {table} {where} {groupby} {having} {orderby}'
        fmtDict = {}
        fmtDict['top'] = ("top " + self.__top) if self.__top else ''
        fmtDict['select'] = self._assembleSelect()
        fmtDict['table'] = self.tableName()
        fmtDict['where'] = self._assembleWhere()
        fmtDict['groupby'] = self._assembleGroupbyOrContextby()
        fmtDict['having'] = ("having " + self.__having) if self.__having else ''
        fmtDict['orderby'] = self._assembleOrderby()
        query = re.sub(' +', ' ', queryFmt.format(**fmtDict).strip())
        return query

    def append(self, table):
        if not isinstance(table, Table):
            raise RuntimeError("Only DolphinDB Table object is accepted")

        runstr = "%s.append!(%s)" % (self.tableName(), table.tableName())
        self.__session.run(runstr)
        return self

    def update(self, cols, vals):
        tmp = copy.deepcopy(self)
        updateTable = TableUpdate(t=tmp, cols=cols, vals=vals)
        return updateTable

    def rename(self, newName):
        self.__session.run(newName+'='+self.tableName())
        self.__tableName = newName

    def delete(self):
        tmp = copy.deepcopy(self)
        delTable = TableDelete(t=tmp)
        return delTable

    def drop(self, cols):
        if cols is not None and len(cols) and isinstance(cols, list):
            runstr = '{table}.drop!([{cols}])'
            fmtDict = dict()
            fmtDict['table'] = self.tableName()
            fmtDict['cols'] = '"' + '","'.join(cols) + '"'
            query = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            for col in cols:
               for colName in self.__select:
                   if col.lower() == colName.lower():
                       self.__select.remove(colName)
            self.__session.run(query)
        else:
            runstr = '{table}.drop!([{cols}])'
            fmtDict = dict()
            fmtDict['table'] = self.tableName()
            fmtDict['cols'] ="'"+cols + "'"
            query = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            for colName in self.__select:
                if cols.lower() == colName.lower():
                    self.__select.remove(colName)
        return self


    def executeAs(self, newTableName):
        self.__session.run(newTableName + "=" + self.showSQL())
        return Table(data=newTableName, s=self.__session)

    def contextby(self, cols):
        contextbyTable = copy.deepcopy(self)
        contextby = TableContextby(contextbyTable, cols)
        contextbyTable._setContextby(contextby)
        return contextby

    def ols(self, Y, X, INTERCEPT=True):
        """
        :param Y: is the dependent variable, table column
        :param X: is the independent variable, a list of table column names
        :param INTERCEPT:
        :return:a dictionary with all regression statistics

        """
        myY = ""
        myX = []

        if isinstance(Y, str):
            myY = Y
        else:
            raise ValueError("Y must be a column name")
        if isinstance(X,str):
            myX = [X]
        elif isinstance(X, list):
            myX = X
        else:
            raise ValueError("X must be a column name or a list of column names")
        if not len(myY) or not len(myX):
            raise ValueError("Invalid Input data")
        schema = self.__session.run("schema(%s)" % self.__tableName)
        if 'partitionColumnName' in schema and schema['partitionColumnName']:
            dsstr = "sqlDS(<SQLSQL>)".replace('SQLSQL', self.showSQL()).replace('select select','select')
            runstr = "olsEx({ds},{Y},{X},{INTERCEPT},2)"
            fmtDict = dict()
            fmtDict['table'] = self.tableName()
            fmtDict['Y'] = '"'+myY+'"'
            fmtDict['X'] = str(myX)
            fmtDict['ds'] = dsstr
            fmtDict['INTERCEPT'] = str(INTERCEPT).lower()
            query = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            return self.__session.run(query)
        else:
            runstr = "z=exec ols({Y},{X},{INTERCEPT},2) from {table}"
            fmtDict = dict()
            fmtDict['table'] = self.tableName()
            fmtDict['Y'] = myY
            fmtDict['X'] = str(myX)
            fmtDict['INTERCEPT'] = str(INTERCEPT).lower()
            query = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            return self.__session.run(query)

    def toDF(self):
        """
        execute sql query on remote dolphindb server

        :return: query result as a pandas.DataFrame object
        """
        self._init_schema()

        query = self.showSQL()
        df = self.__session.run(query) # type: DataFrame

        return df
    toDataFrame = toDF


class TableDelete(object):
    def __init__(self, t):
        self.__t = t

    def _assembleWhere(self):
        try:
            return 'where ' + ' and '.join(self.__where)
        except AttributeError:
            return ''

    def _addWhereCond(self, conds):
        try:
            _ = self.__where
        except AttributeError:
            self.__where = []

        if isinstance(conds, list) or isinstance(conds, tuple):
            self.__where.extend([str(x) for x in conds])
        else:
            self.__where.append(str(conds))

    def where(self, conds):
        self._addWhereCond(conds)
        return self

    def showSQL(self):
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller = calframe[1][3]
        if caller !='execute' and caller !='print' and caller !="str" and caller !='<module>':
            return self.__t.showSQL()
        queryFmt = 'delete from {table} {where}'
        fmtDict = {}
        fmtDict['table'] = self.__t.tableName()
        fmtDict['where'] = self._assembleWhere()
        query = re.sub(' +', ' ', queryFmt.format(**fmtDict).strip())
        return query

    def execute(self):
        """
        execute sql query on remote dolphindb server
        :return: query result as a pandas.DataFrame object
        """
        query = self.showSQL()
        self.__t.session().run(query) # type: DataFrame

    def toDF(self):
        """
        execute sql query on remote dolphindb server
        :return: query result as a pandas.DataFrame object
        """
        query = self.showSQL()

        df = self.__t.session().run(query) # type: DataFrame

        return df


class TableUpdate(object):
    def __init__(self, t, cols, vals):
        self.__t = t
        self.__cols = cols
        self.__vals = vals

    def _assembleUpdate(self):
        query = ""
        for col, val in zip(self.__cols, self.__vals):
            query += col +"="+val + ","
        return query[:-1]

    def _assembleWhere(self):
        try:
            return 'where ' + ' and '.join(self.__where)
        except AttributeError:
            return ''

    def _addWhereCond(self, conds):
        try:
            _ = self.__where
        except AttributeError:
            self.__where = []

        if isinstance(conds, list) or isinstance(conds, tuple):
            self.__where.extend([str(x) for x in conds])
        else:
            self.__where.append(str(conds))

    def where(self, conds):
        self._addWhereCond(conds)
        return self

    def showSQL(self):
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller = calframe[1][3]
        if caller !='execute' and caller !='print' and caller !="str" and caller !='<module>':
            return self.__t.showSQL()
        queryFmt = 'update {table} set {update} {where}'
        fmtDict = {}
        fmtDict['update'] = self._assembleUpdate()
        fmtDict['table'] = self.__t.tableName()
        fmtDict['where'] = self._assembleWhere()
        query = re.sub(' +', ' ', queryFmt.format(**fmtDict).strip())
        return query

    def execute(self):
        """
        execute sql query on remote dolphindb server
        :return: query result as a pandas.DataFrame object
        """
        query = self.showSQL()
        self.__t.session().run(query) # type: DataFrame

    def toDF(self):
        """
        execute sql query on remote dolphindb server
        :return: query result as a pandas.DataFrame object
        """
        query = self.showSQL()

        df = self.__t.session().run(query) # type: DataFrame

        return df


class TablePivotBy(object):
    def __init__(self, t, index, column, value, agg=None):
        self.__row = index
        self.__column = column
        self.__val = value
        self.__t = t;
        self.__agg = agg;

    def toDF(self):
        """
        execute sql query on remote dolphindb server
        :return: query result as a pandas.DataFrame object
        """
        query = self.showSQL()

        df = self.__t.session().run(query) # type: DataFrame

        return df

    toDataFrame = toDF

    def _assembleSelect(self):
        return self.__val if self.__agg is None else _getFuncName(self.__agg) + '(' + self.__val + ')'

    def _assembleWhere(self):
        return self.__t._assembleWhere()

    def _assemblePivotBy(self):
        return 'pivot by ' + self.__row + ',' + self.__column

    def executeAs(self, newTableName):
        self.__session.run(newTableName + "=" + self.showSQL())
        return Table(data=newTableName, s=self.__t.session())

    def showSQL(self):
        import re
        queryFmt = 'select {select} from {table} {where} {pivotby}'
        fmtDict = {}

        fmtDict['select'] = self._assembleSelect()
        fmtDict['table'] = self.__t.tableName()
        fmtDict['where'] = self._assembleWhere()
        fmtDict['pivotby'] = self._assemblePivotBy()
        query = re.sub(' +', ' ', queryFmt.format(**fmtDict).strip())
        return query

    def executeAs(self, newTableName):
        self.__t.session().run(newTableName + "=" + self.showSQL())
        return Table(data=newTableName, s=self.__t.session())

    def selectAsVector(self, colName):
        if colName:
            self._setSelect(colName)
        pattern = re.compile("select", re.IGNORECASE)
        query = pattern.sub('exec', self.showSQL())
        return self.__session.run(query)


class TableGroupby(object):
    def __init__(self, t, groupBys, having=None):
        if isinstance(groupBys, list):
            self.__groupBys = groupBys
        else:
            self.__groupBys = [groupBys]
        self.__having = having
        self.__t = t # type: Table

    def sort(self, bys):
        sortTable = copy.deepcopy(self.__t)
        sortTable._setSort(bys)
        return TableGroupby(sortTable, self.__groupBys, self.__having)

    def executeAs(self, newTableName):
        self.__t.session().run(newTableName + "=" + self.showSQL())
        return Table(data=newTableName, s=self.__t.session())

    def __getitem__(self, item):
        selectTable = self.__t.select(item)
        return TableGroupby(selectTable, groupBys=self.__groupBys, having=self.__having)

    def __iter__(self):
        self.__groupBysIdx = 0
        return self

    def next(self):
        try:
            result = self.__groupBys[self.__groupBysIdx]
        except IndexError:
            raise StopIteration
        self.__groupBysIdx += 1
        return result

    def __next__(self):
        return self.next()

    def having(self, expr):
        havingTable = copy.deepcopy(self.__t)
        self.__having = expr
        havingTable._setHaving(self.__having)
        return havingTable

    def ols(self, Y, X, INTERCEPT=True):
        return self.__t.ols(Y=Y,X=X, INTERCEPT=INTERCEPT)

    def selectAsVector(self, colName):
        if colName:
            self._setSelect(colName)
        pattern = re.compile("select", re.IGNORECASE)
        query = pattern.sub('exec', self.showSQL())
        return self.__session.run(query)

    def showSQL(self):
        return self.__t.showSQL()

    def agg(self, func):
        '''
        apply aggregate functions on all columns except grouping columns
        e.g. sum(x) computes the sum of x.

        :param func: aggregate function name or a list of aggregate function names or a dict of column label/expression->func
        :return: Table object
        '''
        selectCols = self.__t._getSelect()
        if isinstance(func, list):
            selectCols = [_getFuncName(f) + '(' + x + ')' for x in selectCols for f in func if x not in self.__groupBys]
        elif isinstance(func, dict):
            funcDict = {}
            for colName, f in func.iteritems():
                funcDict[colName] = f if isinstance(f, list) else [f]
            # selectCols = [_getFuncName(f) + '(' + x + ')' for x, funcs in funcDict.iteritems() for f in funcs if x not in self.__groupBys]
            selectCols = [_getFuncName(f) + '(' + x + ')' for x, funcs in funcDict.iteritems() for f in funcs ]
        elif isinstance(func, str):
            # selectCols = [_getFuncName(func) + '(' + x + ')' for x in selectCols if x not in self.__groupBys]
            selectCols = [_getFuncName(func) + '(' + x + ')' for x in selectCols]
        else:
            raise RuntimeError('invalid func format, func: aggregate function name or a list of aggregate function names'
                               ' or a dict of column label/expression->func')
        return self.__t.select(selectCols)

    def sum(self):
        return self.agg('sum')

    def avg(self):
        return self.agg('avg')

    def count(self):
        return self.agg('count')

    def max(self):
        return self.agg('max')

    def min(self):
        return self.agg('min')

    def first(self):
        return self.agg('first')

    def last(self):
        return self.agg('last')

    def size(self):
        return self.agg('size')

    def sum2(self):
        return self.agg('sum2')

    def std(self):
        return self.agg('std')

    def var(self):
        return self.agg('var')

    def prod(self):
        return self.agg('prod')

    def agg2(self, func, cols):
        '''
        apply aggregate functions of two arguments.
        e.g. wsum(x,y) computes the sum of x, weighted by y.

        :param func: aggregate function name or a list of aggregate function names
        :param cols: (x,y) tuple or a list of (x,y) tuple where x and y are column labels or column expressions
        :return: Table object
        '''
        if isinstance(cols, list) is False:
            cols = [cols]
        if isinstance(func, list) is False:
            func = [func]
        if __builtin__.sum([1 for x in cols if isinstance(x, tuple) is False or len(x) != 2]):
            raise RuntimeError('agg2 only accepts (x,y) pair or a list of (x,y) pair as cols')
        funcName = [_getFuncName(f) + '(' + x + ',' + y + ')' for f in func for x,y in cols]
        if funcName:
            self.__t._getSelect().extend(funcName)
        return self.__t.select(self.__t._getSelect())

    def wavg(self, cols):
        return self.agg2('wavg', cols)

    def wsum(self, cols):
        return self.agg2('wsum', cols)

    def covar(self, cols):
        return self.agg2('covar', cols)

    def corr(self, cols):
        return self.agg2('corr', cols)

    def toDF(self):
        """
        execute sql query on remote dolphindb server
        :return: query result as a pandas.DataFrame object
        """
        query = self.showSQL()
        df = self.__t.session().run(query)  # type: DataFrame
        return df


class TableContextby(object):
    def __init__(self, t, contextBys, having=None):
        if isinstance(contextBys, list):
            self.__contextBys = contextBys
        else:
            self.__contextBys = [contextBys]
        self.__t = t # type: Table
        self.__having = having

    def sort(self, bys):
        sortTable = copy.deepcopy(self.__t)
        sortTable._setSort(bys)
        return TableContextby(sortTable, self.__contextBys)

    def having(self, expr):
        havingTable = copy.deepcopy(self.__t)
        self.__having = expr
        havingTable._setHaving(self.__having)
        return havingTable

    def __getitem__(self, item):
        selectTable = self.__t.select(item)
        return TableContextby(selectTable, contextBys=self.__contextBys)

    def __iter__(self):
        self.__contextBysIdx = 0
        return self

    def next(self):
        try:
            result = self.__contextBys[self.__contextBysIdx]
        except IndexError:
            raise StopIteration
        self.__contextBysIdx += 1
        return result

    def __next__(self):
        return self.next()

    def selectAsVector(self, colName):
        if colName:
            self._setSelect(colName)
        pattern = re.compile("select", re.IGNORECASE)
        query = pattern.sub('exec', self.showSQL())
        return self.__session.run(query)

    def top(self,num):
        return self.__t.top(num=num)

    def having(self, expr):
        havingTable = copy.deepcopy(self.__t)
        self.__having = expr
        havingTable._setHaving(self.__having)
        return havingTable

    def executeAs(self, newTableName):
        self.__t.session().run(newTableName + "=" + self.showSQL())
        return Table(data=newTableName, s=self.__t.session())

    def showSQL(self):
        return self.__t.showSQL()

    def agg(self, func):
        '''
        apply aggregate functions on all columns except grouping columns
        e.g. sum(x) computes the sum of x.

        :param func: aggregate function name or a list of aggregate function names or a dict of column label/expression->func
        :return: Table object
        '''
        selectCols = self.__t._getSelect()
        if isinstance(func, list):
            selectCols = [_getFuncName(f) + '(' + x + ')' for x in selectCols for f in func if x not in self.__contextBys]
        elif isinstance(func, dict):
            funcDict = {}
            for colName, f in func.iteritems():
                funcDict[colName] = f if isinstance(f, list) else [f]
            selectCols = [_getFuncName(f) + '(' + x + ')' for x, funcs in funcDict.iteritems() for f in funcs if x not in self.__contextBys]
        elif isinstance(func, str):
            selectCols = [_getFuncName(func) + '(' + x + ')' for x in selectCols if x not in self.__contextBys]
        else:
            raise RuntimeError('invalid func format, func: aggregate function name or a list of aggregate function names'
                               ' or a dict of column label/expression->func')
        columns = self.__contextBys[:]
        columns.extend(selectCols)
        lowered = [x.lower() for x in columns]
        for x in self.__t._getSelect():
            if x.lower() not in lowered:
                columns.append(x)
        return self.__t.select(columns)


    def agg2(self, func, cols):
        '''
        apply aggregate functions of two arguments.
        e.g. wsum(x,y) computes the sum of x, weighted by y.

        :param func: aggregate function name or a list of aggregate function names
        :param cols: (x,y) tuple or a list of (x,y) tuple where x and y are column labels or column expressions
        :return: Table object
        '''
        if isinstance(cols, list) is False:
            cols = [cols]
        if isinstance(func, list) is False:
            func = [func]
        if __builtin__.sum([1 for x in cols if isinstance(x, tuple) is False or len(x) != 2]):
            raise RuntimeError('agg2 only accepts (x,y) pair or a list of (x,y) pair as cols')
        funcName = [_getFuncName(f) + '(' + x + ',' + y + ')' for f in func for x,y in cols]
        if funcName:
            self.__t._getSelect().extend(funcName)
        return self.__t.select(self.__t._getSelect())

    def sum(self):
        return self.agg('sum')

    def avg(self):
        return self.agg('avg')

    def count(self):
        return self.agg('count')

    def max(self):
        return self.agg('max')

    def min(self):
        return self.agg('min')

    def first(self):
        return self.agg('first')

    def last(self):
        return self.agg('last')

    def size(self):
        return self.agg('size')

    def sum2(self):
        return self.agg('sum2')

    def std(self):
        return self.agg('std')

    def var(self):
        return self.agg('var')

    def prod(self):
        return self.agg('prod')

    def cumsum(self):
        return self.agg('cumsum')

    def cummax(self):
        return self.agg('cummax')
    
    def cumprod(self):
        return self.agg('cumprod')

    def cummin(self):
        return self.agg('cummin')

    def wavg(self, cols):
        return self.agg2('wavg', cols)

    def wsum(self, cols):
        return self.agg2('wsum', cols)

    def covar(self, cols):
        return self.agg2('covar', cols)

    def corr(self, cols):
        return self.agg2('corr', cols)

    def eachPre(self, args):
        '''
        apply function args[0] to each pair of ajacent elements of args[1].
        see www.dolphindb.com/help/eachPreP.html for more.
        '''
        return self.agg2('eachPre', args)


wavg=TableGroupby.wavg
wsum=TableGroupby.wsum
covar=TableGroupby.covar
corr=TableGroupby.corr
count=TableGroupby.count
max=TableGroupby.max
min=TableGroupby.min
sum=TableGroupby.sum
sum2=TableGroupby.sum2
size=TableGroupby.size
avg=TableGroupby.avg
std=TableGroupby.std
prod=TableGroupby.prod
var=TableGroupby.var
first=TableGroupby.first
last=TableGroupby.last
eachPre=TableContextby.eachPre
cumsum=TableContextby.cumsum
cumprod=TableContextby.cumprod
cummax=TableContextby.cummax
cummin=TableContextby.cummin