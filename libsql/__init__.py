import re
from types import LambdaType

from libdlg.dlgError import FieldNotBelongToTableError
from libdlg.dlgDateTime import DLGDateTime
from libsql.sqlValidate import is_queryType, is_fieldType, is_tableType, is_Py4, is_P4Jnl
from libsql.sqlRecords import Records
from libdlg.dlgControl import DLGControl
from libdlg.dlgStore import ZDict, Lst
from libdlg.dlgUtilities import (
    isnum,
    bail,
    getTableOpKeyValue,
    noneempty,
    is_array,
    reg_rev_change_specifier,
    fix_name,
    ignore_actions,
    reg_dbtablename,
)
from libsql.sqlQuery import *

'''  [$File: //dev/p4dlg/libsql/__init__.py $] [$Change: 609 $] [$Revision: #8 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
'''

__all__ = ('DLGSql',)

class DLGSql(DLGControl):
    def __getitem__(self, item):
        try:
            return self.__dict__.get(item)
        except:
            pass

    def __init__(
            self,
            objp4,
            records=None,
            cols=None,
            query=None,
            **tabledata
    ):
        tabledata = ZDict(tabledata)
        self.closed = False
        self.objp4 = objp4
        if (self.objp4 is None):
            self.objp4 = ZDict()
        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.objp4.logger \
                        if (hasattr(self.objp4, 'logger')) \
                        else tabledata.logger or 'INFO',
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
        )
        ]
        ''' who's asking?
        '''
        self.is_jnlobject = (is_P4Jnl(self.objp4) is True)
        self.is_py4object = (is_Py4(self.objp4) is True)
        ''' Everything relies on the assumption that the schema of
            the current server version is known, keep it close by!
        '''
        self.oSchema = self.objp4.oSchema
        ''' importing DomainType directly will cause a circular 
            dependency problem, so use objp4's class reference.
        '''
        self.oSchemaType = self.objp4.oSchemaType \
            if (hasattr(self.objp4, 'oSchemaType')) \
            else None
        ''' it doesn't really matter which the table we query, 
            but if it happens to be db.domain, then we need to 
            be a little more specific as to the type of domain
            we're looking at. 
        '''
        self.domaintypes = self.oSchemaType.values_names_bydatatype('DomainType') \
            if (self.oSchemaType is not None) \
            else self.get_domaintypes()
        ''' decide on how we modify the p4 table names,
            I.e.: `db.domain` --> `domain`, `dbdomain` or `db_domain` 
        '''
        self.oTableFix = fix_name('remove')
        ''' compute new columns, if any.
        '''
        compute = self.objp4.compute
        if (isinstance(compute, str)):
            self.compute = Lst(
                item.strip().split('=') for item in Lst(
                    compute.split(';')
                ).clean()
            ).clean()
        self.compute = compute or Lst()
        self.maxrows = self.objp4.maxrows
        self.cols = cols
        self.records = records
        self.oDateTime = DLGDateTime()
        self.query = query or Lst()
        ''' if we don't have tablename & fieldsmap, we have nothing.

            However, we need a few lines lines of code since, depending
            on what is doing a select (P4Jnl or Py4), those values can
            be determined differently. I.e. they can be passed in via kwargs
            or by looking at the query.
        '''
        (
            self.tablename,
            self.fieldsmap,
        ) = \
            (
                tabledata.tablename,
                tabledata.fieldsmap,
            )

        if (self.tablename is None):
            qry = query \
                if (isinstance(query, Lst) is False) \
                else query(0)
            if (hasattr(qry, 'left')):
                self.tablename = qry.left.tablename
            elif (is_tableType(qry) is True):
                self.tablename = self.objp4.qry.tablename

        if (self.fieldsmap is None):
            if (
                    (self.tablename is not None) &
                    (self.fieldsmap is None)
            ):
                self.fieldsmap = self.objp4.tablememo[self.tablename].fieldsmap

        self.reference = None
        super(DLGSql, self).__init__()

    def get_domaintypes(self):
        datatypes = self.oSchema.p4schema.datatypes.datatype
        qry = (lambda dtype: dtype.name == 'DomainType')
        try:
            return next(
                filter(
                    qry,
                    datatypes
                )
            )
        except Exception as err:
            bail(err)

    def __iter__(self):
        for col in self.cols:
            yield self[col]

    def validate_distinct(self, field):
        if (field is None): return
        if (is_fieldType(field) is True):
            field = field.fieldname
        field = self.fieldsmap[field.lower()]
        if (field is None):
            raise FieldNotBelongToTableError(self.tablename, field)
        return field

    def __call__(
            self,
            records=None,
            cols=None,
            query=None,
            close_session=True,
            *fieldnames,
            **kwargs
    ):
        query = query or self.query
        cols = cols or self.cols
        records = records or self.records

        (
            fieldnames,
            kwargs
        ) = \
            (
                Lst(fieldnames),
                ZDict(kwargs)
            )

        self.records = self.select(
            records=records,
            cols=cols,
            query=query,
            close_session=close_session,
            *fieldnames,
            **kwargs
        )
        return self.records

    def skiprecord(self, record, tablename):
        ''' reasons for skipping records...
                1. something is wrong with the record
                2. it is not a record
                3. tablename is either None or empty

                TODO: think about these 2...
                4. tablename != self.tablename
                    * hum.. I don't think this should be a reason to skip...
                        - error id = `cols_NE_record_keys`
                5. fieldnames != col names
                    * same here...
                        - error is = `inst_tablename_NE_tablename`
        '''

        def is_error():
            ''' for example, this sync record indicates an error but is
                really just a warning. We can just log it and skip it.

                In this example, the depotFile does exist, but it is flagged as being deleted,
                therefore, `no such file`

                >>> qry = oP4.sync.depotFile.contains('Colors')
                >>> sync_results = oP4(qry).select()

                    {'code': 'error',
                    'data': 'Colors - no such file(s).\n',
                    'severity': 2,
                    'generic': 17}
            '''
            error_msg = Lst(
                'code',
                'data',
                'severity',
                'generic'
            ).intersect(record.getkeys())

            if (len(error_msg) == 4):

                if (
                        (record.code == 'error') &
                        (record.severity >= 1) &
                        (record.generic >= 1)

                ):
                    return (True, record.data)
            return (False, None)

        try:
            if (isinstance(record, list)):
                record = ZDict(zip(self.cols, record))
            (record_is_error, error_data) = is_error()
            skip_record = ZDict(
                {
                    'record_is_error': (record_is_error is True),
                    'ignore_action': (
                        (record.db_action is not None) &
                        (record.db_action in ignore_actions)
                    ),
                    'is_transaction_record': (isnum(tablename) is True),
                    'cols_NE_record_keys': (self.cols != record.getkeys()),
                    'inst_tablename_NE_tablename': (tablename != self.tablename)
                }
            )
            for reason in skip_record:
                if (
                        (skip_record[reason] is True) &
                        (reason == 'record_is_error')
                ):
                    self.logwarning(error_data)
                    return True
            return False
        except Exception as err:
            bail(err)

    def close(self):
        try:
            (
                self.closed,
                self.records,
                self.query,
                self.cols,
                self.distinct,
                self.groupby,
                self.having,
                self.sort,
                self.orderby,
                self.contraint,
            ) = \
                (
                    True,
                    Lst(),
                    None,
                    Lst(),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
        finally:
            self.loginfo('RecordSet records & query have been reset')

    def recurseQuery(self, q):
        ''' Drill into an embedded query until the query's left side
            is of the type Py4Field/JNLField. return its `fieldname`
            and `tablename` attributes.
        '''
        (
            op,
            left,
            right
        ) = \
            (
                q.op,
                q.left,
                q.right
            )
        (
            fieldname,
            tablename
        ) = \
            (
                left.fieldname,
                left.tablename
            )
        operators = andops + orops + xorops + notops
        opname = op.__name__ \
            if (callable(op) is True) \
            else op
        if (not opname in operators):
            if (is_fieldType(left) is True):
                fieldname = left.fieldname
                tablename = left.tablename
                return (fieldname, tablename)
        else:
            (
                fieldname,
                tablename
            ) = \
                (
                    self.recurseQuery(left)
                )
        return fieldname, tablename

    def evaluate(self, qry, record):
        try:
            # "@some[?,#,?#,op]@[?,#,?#,op]"
            (
                tablename,
                fieldname,
                value,
                op,
            ) = \
                (
                    None,
                    None,
                    None,
                    None,
                )
            if (type(qry).__name__ in (
                    'ZDict',
                    'DLGQuery',
                    'DLGExpression'
            )
            ):
                tablename = qry.left.tablename
                fieldname = qry.left.fieldname
                value = qry.right
                op = qry.op
            elif (isinstance(qry, str)):
                (
                    tablename,
                    fieldname,
                    value,
                    op
                ) = getTableOpKeyValue(qry)

            elif (isinstance(qry, list)):
                qry = Lst(qry)
                (
                    fieldname,
                    value,
                    op
                ) = \
                    (
                        qry(0),
                        qry(1),
                        qry(2)
                    )

            if (None in (fieldname, tablename)):
                (fieldname, tablename) = self.recurseQuery(qry)
            fieldsmap = self.fieldsmap or self.objp4.fieldsmap
            fieldname = fieldsmap[fieldname.lower()]

            def eparse(
                    op,
                    record,
                    tablename,
                    fieldname,
                    value,
                    inversion=False
            ):
                res = False
                ''' do we need to invert?
                '''
                if (tablename.sartswith('~')):
                    tablename = re.sub('~', '', tablename)
                    return eparse(op, record, tablename, fieldname, inversion=True)
                elif (op == '!#'):
                    op = re.sub('!', '', op)
                    return eparse(op, record, tablename, fieldname, inversion=True)

                if (
                        op in ('=', EQ)
                ):
                    res = self.objp4.__eq__(
                        str(record[fieldname]),
                        str(value)
                    )
                elif (
                        op in ('!=', NE)
                ):
                    res = self.objp4.__ne__(
                        str(record[fieldname]),
                        str(value)
                    )
                elif (
                        op in ('<', LT)
                ):
                    res = self.objp4.__lt__(
                        float(record[fieldname]),
                        float(value)
                    )
                elif (
                        op in ('<=', LE)
                ):
                    res = self.objp4.__le__(
                        float(record[fieldname]),
                        float(value)
                    )
                elif (
                        op in ('>', GT)
                ):
                    res = self.objp4.__gt__(
                        float(record[fieldname]),
                        float(value)
                    )
                elif (
                        op in ('>=', GE)
                ):
                    res = self.objp4.__ge__(
                        float(record[fieldname]),
                        float(value)
                    )
                elif (
                        op in (NOT)
                ):
                    res = ~record[fieldname]
                elif (
                        op in ('#', '#?', CONTAINS, SEARCH)
                ):
                    res = (self.objp4.memoizefield(value).search(
                        str(record[fieldname])) is not None
                           )
                elif (
                        op in ('#^', STARTSWITH)
                ):
                    res = (self.objp4.memoizefield(f'^{value}').match(
                        str(record[fieldname])) is not None
                           )
                elif (
                        op in ('#$', ENDSWITH)
                ):
                    res = (self.objp4.memoizefield(f'{value}$').match(
                        str(record[fieldname])) is not None
                           )
                elif (
                        op in ('#^$', '##', MATCH)
                ):
                    res = (self.objp4.memoizefield(f'^{value}$').match(
                        str(record[fieldname])) is not None
                           )
                elif (
                        isinstance(record[fieldname], (list, dict))
                ):
                    res = (qry in [str(rvalue) for rvalue in record.getvalues()])
                if (inversion is True):
                    res = not res
                return res

            result = eparse(
                op,
                record,
                tablename,
                fieldname
            )
            return result
        except ValueError as err:
            self.logerror(err)
            return False

    '''      (op in ('#^', '#$', '#')) / (op == '!#')
    '''

    def ziprecord(self, record, cols, idx):
        record = Lst(record)
        ''' action field: pv, dv, mx, etc. 
            value of actionfield = 0 if not idx else 1 
        '''
        actionfield = 0
        skip_record = False
        if ('idx' in cols):
            record.appendleft(idx)
            actionfield = 1
        if (record[actionfield] not in ignore_actions):
            record = ZDict(
                zip(
                    cols,
                    record
                )
            )
        else:
            skip_record = True
        return (record, skip_record)


    def parse(self, qry, record=None, opfunc=None):
        if (type(qry) is LambdaType):
            out = ((qry(record) & 1) | 0)
        elif (
                (isinstance(qry, (str, list))) |
                (type(qry).__name__ in (
                        'ZDict',
                        'DLGQuery',
                        'DLGExpression'
                    )
                )
        ):
            if (opfunc is None):
                receval = self.evaluate(qry, record=record)
            else:
                value = record[qry.left.fieldname]
                ''' cast numeric fields to int so that
                    the record's field value and the qry's
                    right side are both living on the same 
                    planet (int <--> int)
                '''
                if (isnum(qry.right) is True):
                    qright = int(qry.right)
                    if (isnum(value) is True):
                        value = int(value)
                ''' TODO: revisit this - I don't remember why
                    evaluating left against right in one case
                    and evaluating right against left in the
                    other... '//depot/pycharmprojects/git-fusion/build/vmware-studio/git-fusion-vpkgs/librtmp0_2.4~20110711.gitc28f1bab-1_amd64.deb'
                '''
                receval = opfunc(value, qry.right) \
                    if (type(qry).__name__ in (
                    'DLGQuery',
                    'DLGExpression'
                )
                        ) \
                    else opfunc(qry.right, value)

            if (isinstance(receval, bool) is True):
                receval = int(receval)
            out = ((receval & 1) | 0)
        else:
            out = ((qry & 1) | 0)
        return out

    def build_results(self, qry, record):
        try:
            if (isinstance(qry, bool) is True):
                return qry
            (
                op,
                left,
                right
            ) = getTableOpKeyValue(qry) \
                if (isinstance(qry, str)) \
                else (
                qry.op,
                qry.left,
                qry.right
            )
            if (isinstance(right, str)):
                ''' might be best just to carry over a check if thisright side arg is a file?'''
                if (reg_rev_change_specifier.match(str(right)) is not None):
                    for item in ('#', '@'):
                        right_bits = Lst(
                            re.split(
                                item,
                                str(right),
                                maxsplit=1)
                        )
                        right = right_bits(0)

            built = None
            opname = op.__name__ \
                if (callable(op) is True) \
                else op

            (
                exp_func,
                qry_func
            ) = \
                (
                    expression_table(opname),
                    optable(opname)
                )

            ''' queries can be wrapped in AND/OR/XOR/NOT expressions...
                start with these
            '''
            operators = (andops + orops + xorops + notops)
            if (opname in operators):

                (
                    buildleft,
                    buildright
                ) = \
                    (
                        self.build_results(left, record),
                        None
                    )

                if (right is not None):
                    # this should likely cause an exception,
                    # the value of right should be a qry (
                    # with its own 'op', 'left', 'right'
                    # attributes
                    #
                    # TODO: fix this likely exception
                    buildright = self.build_results(right, record)
                    if (isinstance(buildright, (int, bool)) is False):
                        if (buildright.right is not None):
                            buildright = buildright.right

                if (
                        opname in (AND, 'AND', '&')
                ):
                    built = (
                        buildleft &
                        buildright
                    )
                elif (
                        opname in (OR, 'OR', '|')
                ):
                    built = (
                        buildleft |
                        buildright
                    )
                elif (
                        opname in (XOR, 'XOR', '^')
                ):
                    built = (
                        buildleft ^
                        buildright
                    )
                elif (
                        opname in notops
                ):
                    if (left is not None):
                        built = self.parse(buildleft)
                        return built
                    elif not (left or right):
                        built = DLGExpression(self.objp4, op)

            elif (exp_func is not None):
                built = self.parse(qry, record, exp_func)
                if (qry.inversion is True):
                    built = not bool(built)
            elif (qry_func is not None):
                ''' done with AND/OR/XOR.
                    time to handle queries & expressions like:

                        =, !=, <, >, <=, >=, 
                        CONTAINS (# / !#), 
                        STARTSWITH (#^ / !#^), 
                        ENDSWITH (#$ / !#$), etc. 
                '''
                if (is_queryType(qry) is True):
                    built = self.parse(qry, record, qry_func)
                    if (qry.inversion is True):
                        built = not bool(built)
            elif (isinstance(qry, bool)):
                built = qry
            elif not (left or right):
                built = DLGExpression(self.objp4, op)
            else:
                bail(
                    f"Operator not supported: {opname}.\n"
                )
            return built
        except Exception as err:
            bail(err)

    def aggregate(self, records, query=None, **kwargs):
        kwargs = ZDict(kwargs)

        (orderby,
         limitby,
         groupby,
         sortby,
         find,
         filter,
         exclude,
         search,
         ) = (
            kwargs.orderby,
            kwargs.limitby,
            kwargs.groupby,
            kwargs.sortby,
            kwargs.find,
            kwargs.filter,
            kwargs.exclude,
            kwargs.search,
        )

        if (orderby is not None):
            '''  orderby         -->     and/or limitby
            '''
            if (isinstance(orderby, str)):
                orderby = [item for item in orderby.split(',')] \
                    if (',' in orderby) \
                    else [orderby]
            elif (type(orderby).__name__ in ('JNLField', 'Py4Field')):
                orderby = [orderby]
            records = records.orderby(*orderby) \
                if (limitby is None) \
                else records.orderby(*orderby, limitby=limitby)
        elif (limitby is not None):
            '''  limitby         -->     and nothing else...
            '''
            records = records.limitby(limitby)
        if (groupby is not None):
            '''  groupby
            '''
            records = records.groupby(groupby)
        if (exclude is not None):
            '''  exclude

                 >>> for record in records.exclude(lambda rec: rec.type=='99'):
                 >>>     print record.client
                 my_client
            '''
            records = records.exclude(exclude)
        if (filter is not None):
            '''  filter
            '''
            records = records.filter(filter)
        if (find is not None):
            '''  find
            '''
            records = records.find(find)
        if (sortby is not None):
            '''  sort

                        >>> records = oJnl(oJnl.rev).select()
                        >>> Qry=(lambda rec: 'depot' in rec.depotFile)
                        >>> records = records.find(Qry).sort( lambda rec: rec.depotFile)
                        >>> for rec in records:
                        >>>     print rec.depotFile
                        //depot/aFolder/aFile
                        //depot/anotherFolder/anotherFile

                        >>> for record in records.sort(lambda rec: rec.mount):
                        >>>     print rec.client
                        Catmart_client
                        Charotte_client
        '''
            records = records.sort(sortby)
        if (search is not None):
            for record in records:
                if (record.depotFile is not None):
                    fcontent = ''
        return records

    def get_recordsIterator(self, records, tables=[]):
        if (self.is_jnlobject is True):
            tablenames = [f'db.{tbl}' for tbl in tables] or [f'db.{self.tablename}']
            try:
                return enumerate([
                    rec[1] for (idx, rec) in enumerate(records)
                    if (rec[1][2] in tablenames)  # if (reg.match(rec[1][2]))
                ], start=1)
                # return filter(lambda rec: (rec[1][2] in tablenames), records)
            except Exception as err:
                bail(err)

        return enumerate(records, start=1) \
            if (type(records) is not enumerate) \
            else records

    def guess_records(self, query, tablename):
        (
            left,
            op,
            right
        ) = \
            (
                None,
                None,
                None
            )
        grecords = None
        if (isinstance(query, DLGExpression) is True):
            if (reg_dbtablename.match(tablename) is not None):
                tablename = self.oTableFix.normalizeTableName(tablename)
            (
                left,
                op,
                objp4
            ) = \
                (
                    query.left,
                    query.op,
                    query.objp4
                )
            right = getattr(objp4, tablename)()
            opfunc = expression_table(op)
            grecords = opfunc(left, right)
        if (
                (isinstance(right, Records) is True) &
                (is_array(grecords) is True)
        ):
            grecords = Records(grecords, Lst(), self.objp4)
        return grecords

    def get_record_components(
            self,
            *fieldnames,
            query=None,
            cols=None,
            records=None,
            **kwargs
    ):
        kwargs = ZDict(kwargs)
        ''' query
        '''
        if (query is None):
            query = self.query \
                if (self.query is not None) \
                else []
        if (isinstance(query, list) is False):
            query = Lst([query])
        ''' cols
        '''
        if (noneempty(cols) is True):
            cols = self.cols or Lst()
        if (
                (len(cols) > 0) &
                (not 'idx' in cols)
        ):
            cols.insert(0, 'idx')
        ''' remove field `code`
        '''
        if ('code' in cols):
            try:
                cols.pop(cols.index('code'))
            except Exception as err:
                self.objp4.logwarning(err)
        ''' tablename
        '''
        tablename = self.tablename or kwargs.tablename
        if (self.tablename is None):
            self.tablename = tablename

        if (tablename is None):
            try:
                if (isinstance(query, Lst) is True):
                    tablename = query(0).left.tablename
                else:
                    tablename = query.left.tablename
            except:
                if (self.reference is not None):
                    tablename = self.reference.left.tablename
        ''' records
        '''
        if (records is None):
            records = self.records
        ''' records is not enumerator & whatever 
            it is, it is empty! Just do the few 
            lines below and get out!
        '''
        if (type(records) != enumerate):
            records = self.guess_records(query, tablename)

        fieldsmap = kwargs.fieldsmap or self.fieldsmap

        if (len(fieldnames) == 1):
            fieldtype = type(fieldnames[0]).__name__
            if (fieldtype in ('JNLTable', 'Py4Table')):
                fieldnames = cols
        fieldnames = Lst(fieldnames or self.objp4.fieldnames).storageindex(reversed=True)
        return (tablename, fieldnames, fieldsmap, query, cols, records)

    def computecolumns(self, record):
        try:
            for (key, value) in self.compute:
                self.cols.merge(key)
                self.objp4.updateenv(**record)
                record.update(
                    **{
                        key: eval(value, ZDict(), self.objp4.env)
                    }
                )
                self.loginfo(f'computed new column ({key})')
            return record
        except Exception as err:
            bail(err)