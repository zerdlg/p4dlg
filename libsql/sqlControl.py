import re
from types import LambdaType

from libdlg.dlgError import FieldNotBelongToTableError
from libdlg.dlgDateTime import DLGDateTime
from libsql.sqlValidate import *
from libsql.sqlRecords import Records
from libdlg.dlgControl import DLGControl
from libdlg.dlgStore import Storage, Lst
from libdlg.dlgUtilities import (
    isnum,
    bail,
    getTableOpKeyValue,
    noneempty,
    is_array,
    reg_rev_change_specifier,
    fix_name,
    ignore_actions,
)
from libsql.sqlQuery import *

'''  [$File: //dev/p4dlg/libsql/sqlControl.py $] [$Change: 693 $] [$Revision: #14 $]
     [$DateTime: 2025/04/22 07:22:55 $]
     [$Author: mart $]
'''

__all__ = ['DLGSql', 'fields2ints']


def fields2ints(record):
    # crecord = Record(record.copy())
    crecord = Storage(record.copy())
    for fkey in crecord.getkeys():
        if (isnum(record[fkey]) is True):
            try:
                record[fkey] = int(record[fkey])
            except ValueError:
                record[fkey] = float(record[fkey])
            except Exception as err:
                print(err)
    return record


class DLGSql(DLGControl):
    ''' `commitlist` are records yet to be committed to the target `DB system`
    '''

    def __getitem__(self, item):
        try:
            return self.__dict__.get(item)
        except: pass

    def __setitem__(self, key, value):
        self.__dict__[str(key)] = value

    def __init__(
            self,
            objp4,
            records=None,
            cols=None,
            query=None,
            **tabledata
    ):
        self.tabledata = tabledata = Storage(tabledata)
        self.closed = False
        self.objp4 = objp4
        if (self.objp4 is None):
            self.objp4 = Storage()
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
        self.domaintypes = self.oSchemaType.valuesnames_bydatatype('DomainType') \
            if (self.oSchemaType is not None) \
            else self.get_domaintypes()
        ''' decide on how we modify the p4 table names,
            I.e.: `db.domain` --> `domain`, `dbdomain` or `db_domain` 
        '''
        self.oTableFix = fix_name('remove')
        ''' compute new columns, if any.
        '''
        compute = self.objp4.compute \
            if (hasattr(self.objp4, 'compute')) \
            else self.tabledata['compute'] \
            if ('compute' in self.tabledata.keys()) \
            else []
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
        self.query = query
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

        if (
                (self.tablename is None) &
                (query is not None)
        ):
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

    #def __iter__(self):
        #for col in self.cols:
        #    yield self[col]

    def __iter__(self):
        for fieldname in self.objp4[self.tablename].fieldnames:
            yield self[fieldname]

    def validate_distinct(self, field):
        if (field is None):
            return
        if (is_fieldType_or_expressionType(field) is True):
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
        (
            query,
            cols,
            records,
            fieldnames,
            kwargs
        ) = \
            (
                query or self.query,
                cols or self.cols,
                records or self.records,
                Lst(fieldnames),
                Storage(kwargs)
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
                record = Storage(zip(self.cols, record))
            (record_is_error, error_data) = is_error()
            skip_record = Storage(
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
                (
                    fieldname,
                    tablename
                ) = \
                    (
                        left.fieldname,
                        left.tablename
                    )
        else:
            (
                fieldname,
                tablename
            ) = \
                (
                    self.recurseQuery(left)
                )
        return (fieldname, tablename)

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
                    'Storage',
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
            record = Storage(
                zip(
                    cols,
                    record
                )
            )
        else:
            skip_record = True
        return (record, skip_record)

    def parse__(self, qry, record=None, opfunc=None):
        out = None
        if (type(qry) is LambdaType):
            out = ((qry(record) & 1) | 0)
        elif (
                (isinstance(qry, (str, list))) |
                (type(qry).__name__ in (
                        'Storage',
                        'DLGQuery',
                        'DLGExpression'
                )
                )
        ):
            if (opfunc is None):
                receval = self.evaluate(qry, record=record)
            else:
                value = qry.left.op(qry.left, record) \
                    if (is_query_or_expressionType(qry.left) is True) \
                    else record[qry.left.fieldname]
                try:
                    value = int(value)
                    ''' cast numeric fields to int so that
                        the record's field value and the qry's
                        right side are both living on the same 
                        planet (int <--> int)
                    '''
                    qry.right = int(qry.right)
                    ''' TODO: revisit this - I don't remember why
                        evaluating left against right in one case
                        and evaluating right against left in the
                        other... '
                    '''
                    receval = opfunc(value, qry.right) \
                        if (is_query_or_expressionType(qry) is True) \
                        else opfunc(qry.right, value)
                    if (is_expressionType(qry) is True):
                        return receval
                except Exception as err:
                    bail(err)
            if (isinstance(receval, bool) is True):
                receval = int(receval)
            out = ((receval & 1) | 0)
        else:
            out = ((qry & 1) | 0)
        return out

    def parse_exp(self, op, left, right, record, exp=None):
        out = ((left(record) & 1) | 0) if (type(left) is LambdaType) else None
        if (out is not None):
            return out
        if (isinstance(exp, str) is True):
            receval = self.evaluate(left, record=record)
            return receval

        value = left.op(left, record) \
            if (is_expressionType(left) is True) \
            else record[left.fieldname]
        [int(item) for item in (value, left, right) if (isnum(item) is True)]
        receval = op(left, record) \
            if (right is None) \
            else op(value, right)
        return receval

    def calculate_receval(self, receval):
        if (isinstance(receval, bool) is True):
            receval = int(receval)
        return ((receval & 1) | 0)

    def parse_qry(self, op, left, right, record, qry=None):
        if (type(left) is LambdaType):
            return self.calculate_receval(left(record))
        if (isinstance(qry, str) is True):
            receval = self.evaluate(left, record=record)
            return self.calculate_receval(receval)

        value = None
        if (
                (isinstance(left, dict) is True) |
                (is_fieldType_or_queryType(left) is True)
        ):
            value = left.op(left, record) \
                if (is_queryType(left) is True) \
                else record[left.fieldname]
        if (value is not None):
            left = value
        if (op in comparison_ops):
            if (
                    (right is not None) &
                    (isnum(left) is True) &
                    (isnum(right) is True)
            ):
                (left, right) = (float(left), float(right))
        if (right is None):
            right = record
        receval = op(left, right)
        return self.calculate_receval(receval)
    '''
    >>> ((p4.files.depotFile.len() + 1) > 64)
    or >>> 

    <DLGQuery {'objp4': <Py4 anastasia.local:1777 >, 
               'op': <function GT at 0x107801260>, 
               'left': <DLGExpression {'inversion': False,
                                       'left': <DLGExpression {'inversion': False,
                                                               'left': <Py4Field depotFile>,
                                                               'objp4': <Py4 anastasia.local:1777 >,
                                                               'op': <function LEN at 0x1078018a0>,
                                                               'right': None}>,
                                       'objp4': <Py4 anastasia.local:1777 >,
                                       'op': <function ADD at 0x107801e40>,
                                       'right': 1}>, 
               'right': 64, 
               'inversion': False}>
    '''

    def build_results(self, qry, record):
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
            ''' might be best just to carry over a check 
                if this right side arg is a file?
            '''
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
        AOXN = (andops + orops + xorops + notops)
        if (opname in AOXN):
            (
                buildleft,
                buildright
            ) = \
                (
                    self.build_results(left, record),
                    None
                )

            if (right is not None):
                # This should likely cause an exception,
                # the value of right should be a qry (
                # with its own 'op', 'left', 'right'
                # attributes. At any rate...
                #
                # TODO: fix likely exception
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
                # not sure... needs testing
                if (left is not None):
                    built = self.parse_qry(buildleft.op, buildleft.left, buildleft.right, record)
                elif not (left or right):
                    built = DLGExpression(self.objp4, op)

        elif (exp_func is not None):
            for (akey, avalue) in Storage(
                    {
                        'left': left,
                        'right': right
                    }
            ).items():
                if (hasattr(avalue, 'op')):
                    if (avalue.op) is not None:
                        avalue = self.parse_exp(
                            avalue.op,
                            avalue.left,
                            avalue.right,
                            record
                        )
                        if (hasattr(avalue, 'op')):
                            if (avalue.inversion is True):
                                avalue = not bool(avalue)
                if (akey == 'left'):
                    left = avalue
                else:
                    right = avalue
            if (is_fieldType_or_expressionType(left) is True):
                built = self.parse_exp(
                    op,
                    left,
                    right,
                    record
                )
            else:
                built = op(left, right)

        elif (qry_func is not None):
            for (akey, avalue) in Storage(
                    {
                        'left': left,
                        'right': right
                    }
            ).items():
                if (is_expressionType(avalue) is True):
                    avalue = self.build_results(avalue, record)
                if (hasattr(avalue, 'op')):
                    if (avalue.op is not None):
                        parsefunc = self.parse_exp \
                            if (is_expressionType(avalue) is True) \
                            else self.parse_qry
                        avalue = parsefunc(op, avalue, right, record)
                        if (hasattr(avalue, 'op')):
                            if (avalue.inversion is True):
                                avalue = not bool(avalue)
                if (akey == 'left'):
                    left = avalue
                else:
                    right = avalue
            built = self.parse_qry(
                op,
                left,
                right,
                record
            )
            if (qry.inversion is True):
                built = not bool(built)

        elif (isinstance(qry, bool)):
            built = qry
        elif not (left or right):
            # doh! if (op is qry) ???
            built = DLGExpression(self.objp4, op)
        else:
            bail(
                f"Operator not supported: {opname}.\n"
            )
        return built

    def aggregate(self, records, **kwargs):
        kwargs = Storage(kwargs)
        (orderby,
         limitby,
         groupby,
         sortby,
         find,
         filter,
         exclude,
         search,
         count,
         dlgsum,
         dlgavg,
         dlgmin,
         dlgmax,
         dlglen,
         as_groups,
         distinct,
         add,
         sub,
         mul,
         mod,
         substr,
         ) = (
            kwargs.orderby,
            kwargs.limitby,
            kwargs.groupby,
            kwargs.sortby,
            kwargs.find,
            kwargs.filter,
            kwargs('exclude'),
            kwargs.search,
            kwargs.count,
            kwargs.sum,
            kwargs.avg,
            kwargs.min,
            kwargs.max,
            kwargs.len,
            kwargs.as_groups or False,
            kwargs.distinct,
            kwargs.add,
            kwargs.sub,
            kwargs.mul,
            kwargs.mod,
            kwargs.substr
        )

        if (noneempty(records) is True):
            return records
        if (orderby is not None):
            '''  orderby         -->     and/or limitby
            '''
            kwargs.delete('orderby')
            if (isinstance(orderby, str)):
                orderby = [item for item in orderby.split(',')] \
                    if (',' in orderby) \
                    else [orderby]
            elif (is_fieldType(orderby) is True):
                orderby = [orderby]
            records = records.orderby(*orderby) \
                if (limitby is None) \
                else records.orderby(*orderby, limitby=limitby)
        if (limitby is not None):
            '''  limitby         -->     and nothing else...
            '''
            kwargs.delete('limitby')
            records = records.limitby(limitby, **kwargs)
        if (groupby is not None):
            '''  groupby
            '''
            kwargs.delete('groupby')
            records = records.groupby(groupby, **kwargs)  # as_groups=as_groups, **kwargs)
        if (exclude is not None):
            '''  exclude

                 >>> for record in records.exclude(lambda rec: rec.type=='99'):
                 >>>     print record.client
                 my_client
            '''
            records = records.exclude(exclude, **kwargs)
        if (filter is not None):
            '''  filter
            '''
            kwargs.delete('filter')
            records = records.filter(filter, **kwargs)
        if (find is not None):
            '''  find
            '''
            kwargs.delete('find')
            records = records.find(find, **kwargs)
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
                        zerdlg_client
                        Charotte_client
        '''
            kwargs.delete('sortby')
            # records = records.sort(sortby)
            records = records.sortby(sortby, **kwargs)
        if (count is not None):
            ''' count
            '''
            if (is_fieldType(count) is True):
                count = getattr(count, 'count')()
            kwargs.delete('count')
            return count.op(count, records, **kwargs)
        if (dlgsum is not None):
            ''' sum
            '''
            if (is_fieldType(dlgsum) is True):
                dlgsum = getattr(dlgavg, 'sum')()
            kwargs.delete('sum')
            return dlgsum.op(dlgsum, records, **kwargs)
        if (dlgavg is not None):
            ''' avg
            '''
            if (is_fieldType(dlgavg) is True):
                dlgavg = getattr(dlgavg, 'avg')()
            kwargs.delete('avg')
            return dlgavg.op(dlgavg, records, **kwargs)
        if (dlgmin is not None):
            ''' max
            '''
            if (is_fieldType(dlgmin) is True):
                dlgmin = getattr(dlgmin, 'min')()
            kwargs.delete('min')
            return dlgmin.op(dlgmin, records, **kwargs)
        if (dlgmax is not None):
            ''' max
            '''
            if (is_fieldType(max) is True):
                dlgmax = getattr(dlgmax, 'max')()
            kwargs.delete('max')
            return dlgmax.op(dlgmax, records, **kwargs)
        if (dlglen is not None):
            ''' len
            '''
            if (is_fieldType(dlglen) is True):
                dlglen = getattr(dlglen, 'len')()
            kwargs.delete('len')
            return dlglen.op(dlglen, records, **kwargs)
        if (search is not None):
            ''' search
            '''
            if (is_fieldType(search) is True):
                search = getattr(search, 'search')()
            kwargs.delete('search')
            for record in records:
                if (record.depotFile is not None):
                    fcontent = ''
        if (add is not None):
            ''' add
            '''
            if (is_fieldType(add) is True):
                add = getattr(add, 'add')()
            kwargs.delete('add')
            return add.op(add, records, **kwargs)
        if (sub is not None):
            ''' sub
            '''
            if (is_fieldType(sub) is True):
                sub = getattr(sub, 'sub')()
            kwargs.delete('sub')
            return sub.op(sub, records, **kwargs)
        if (mul is not None):
            ''' mul
            '''
            if (is_fieldType(mul) is True):
                mul = getattr(mul, 'mul')()
            kwargs.delete('mul')
            return mul.op(mul, records, **kwargs)
        if (mod is not None):
            ''' mod
            '''
            if (is_fieldType(mod) is True):
                mod = getattr(mod, 'mod')()
            kwargs.delete('mod')
            return mod.op(mod, records, **kwargs)
        if (distinct is not None):
            ''' distinct
            '''
            if (is_fieldType(distinct) is True):
                distinct = getattr(distinct, 'distinct')()
            #elif (type(distinct) is slice):
            #    distinct = distinct.op(distinct)
            kwargs.delete('distinct')
            return distinct.op(distinct, records, **kwargs)
        if (substr is not None):
            ''' substring
            '''
            kwargs.delete('substr')
            return substr.op(substr, records, **kwargs)
        ''' That's it - return all records.
        '''
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
        if (
                (is_recordType(records) is True) &
                (self.query is not None)
        ):
            outrecords = Records([], [], objp4=self.objp4)
            outrecords.insert(0, records)
            return enumerate(outrecords, start=1)
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
        # if (is_expressionType(query) is True):
        #    if (reg_dbtablename.match(tablename) is not None):
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
                (is_expressionType(query) is True) &
                (is_recordsType(grecords) is False)
        ):
            return grecords

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
        kwargs = Storage(kwargs)
        fieldnames = Lst(fieldnames)
        ''' query
        
        if (query is None):
            if (isinstance(self.query, list) is True):
                self.query.append(query)
            else:
                query = self.query or []
        elif (isinstance(query, list) is True):
            query = Lst([query])
        '''
        if (query is None):
            query = self.query or []
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

        ''' tablename & records
        '''
        tablename = self.tablename or self.tabledata.tablename
        if (tablename is None):
            try:
                tablename = query(0).left.tablename \
                    if (isinstance(query, Lst) is True) \
                    else query.left.tablename
            except:
                if (self.reference is not None):
                    tablename = self.reference.left.tablename

        ''' It could happen - that tablename might still be None
        '''
        if (tablename is None):
            if (fieldnames(0) is None):
                for key in kwargs.keys():
                    if (is_expressionType(kwargs[key]) is True):
                        tablename = kwargs[key].tablename
            elif (is_expressionType(fieldnames(0)) is True):
                tablename = fieldnames(0).tablename
        ''' records is not enumerator or it is None, let's start grasping.
        '''
        if (
                (records is None) |
                (type(records) != enumerate)
        ):
            records = Records([], [], self.objp4, **self.tabledata)
            if (self.records is not None):
                if (is_recordType(self.records) is True):
                    if (len(self.records) > 0):
                        records = self.records
                elif (type(self.records) == enumerate):
                    records = self.records
            if (tablename is not None):
                if (type(records) != enumerate):
                    if (len(records) == 0):
                        records = self.objp4(self.objp4[tablename]).records
            else:
                recset = None
                if (len(query) > 0):
                    if (isinstance(query, str) is True):
                        ''' TODO:
                            comeback here & support str queries
                        '''
                    elif (is_queryType(query) is True):
                        recset = self.objp4(query)
                elif (fieldnames(0) is not None):
                    recset = self.objp4(fieldnames(0).left._table) \
                        if (is_fieldType_or_expressionType(fieldnames(0)) is True) \
                        else self.objp4(self.objp4[tablename])
                records = recset.records

        fieldsmap = self.fieldsmap or self.tabledata.fieldsmap
        fieldnames = Lst(fieldnames).storageindex(reversed=True)
        return (tablename, fieldnames, fieldsmap, query, cols, records)

    def computecolumns(self, record):
        try:
            for (key, value) in self.compute:
                self.cols.merge(key)
                self.objp4.updateenv(**record)
                record.update(
                    **{
                        key: eval(value, Storage(), self.objp4.env)
                    }
                )
                self.loginfo(f'computed new column ({key})')
            return record
        except Exception as err:
            bail(err)

    def fiels2ints(self, record):
        return fields2ints(record)
    # def fields2ints(self, record):
    #    crecord = Record(record.copy())
    #    for fkey in crecord.getkeys():
    #        if (isnum(record[fkey]) is True):
    #            record[fkey] = int(record[fkey])
    #    return record