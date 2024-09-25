import re
from types import LambdaType
from pprint import pformat

from libdlg.dlgDateTime import DLGDateTime
from libdlg.dlgQuery_and_operators import *
from libdlg.dlgRecords import DLGRecords
from libdlg.dlgControl import DLGControl
from libdlg.dlgStore import Storage, Lst
from libdlg.dlgUtilities import (
    is_iterable,
    isnum,
    bail,
    getTableOpKeyValue,
    noneempty,
    xrange,
    is_array,
    is_P4Jnl,
    reg_rev_change_specifier,
    fix_name,
    ignore_actions,
    reg_dbtablename
)

'''  [$File: //dev/p4dlg/libdlg/dlgSelect.py $] [$Change: 479 $] [$Revision: #56 $]
     [$DateTime: 2024/09/20 07:42:22 $]
     [$Author: mart $]
'''

__all__ = ('Select',)

class Select(DLGControl):
    def __init__(
                self,
                objp4,
                records=None,
                cols=None,
                query=None, **kwargs
    ):
        kwargs = Storage(kwargs)
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
                        else kwargs.logger or 'INFO',
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
        )
        ]
        self.is_jnlobject = (is_P4Jnl(self.objp4) is True)
        self.oSchema = self.objp4.oSchema
        ''' importing DomainType directly will cause a circular 
            dependency problem, so use objp4's class reference.
        '''
        self.oSchemaType = self.objp4.oSchemaType \
            if (hasattr(self.objp4, 'oSchemaType')) \
            else None
        ''' I don't like this but, if we can't get help from SchemaType 
            because of a circular dependency, we'll crack open the schema
            and grab the data we need...
            
            TODO: fix circular dependency. 
        '''
        self.domaintypes = self.oSchemaType.values_names_bydatatype('DomainType') \
            if (self.oSchemaType is not None) \
            else self.get_domaintypes()
        ''' compute new columns
        '''
        self.compute = Lst()
        compute = self.objp4.compute or []
        if (noneempty(compute) is False):
            if (isinstance(compute, str)):
                self.compute = Lst(item.strip().split('=') for item in \
                                   Lst(compute.split(';')).clean()).clean()
            elif (isinstance(compute, list)):
                self.compute = compute

        self.maxrows = self.objp4.maxrows
        self.oTableFix = fix_name('remove')
        self.cols = cols
        self.records = records
        self.query = query or Lst()
        qry = query \
            if (isinstance(query, list) is False) \
            else query[0]
        (
            tablename,
            fieldsmap
        )  = \
            (
                None,
                None
            )


        if (hasattr(qry, 'left')):
            tablename = qry.left.tablename
        self.tablename = tablename
        if (tablename is not None):
            self.fieldsmap = self.objp4.fieldsmap \
                if (self.is_jnlobject is False) \
                else self.objp4.tablememo[tablename].fieldsmap
        super(Select, self).__init__()
        self.oDateTime = DLGDateTime()

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
        for i in xrange(len(self.records)):
            yield self[i]

    def __call__(
                self,
                records=None,
                cols=None,
                query=None,
                raw_records=False,
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
                Storage(kwargs)
            )

        self.records = self.select(
            records=records,
            cols=cols,
            query=query,
            raw_records=False,
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
                4. tablename != self.tablename
                4. fieldnames != col names
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
                if AND(
                        AND(
                            (
                                (record.code == 'error'),
                                (record.severity >= 1)
                            ),
                        (record.generic >= 1)
                        )
                ):
                    return (
                        True,
                        record.data
                    )

            return (
                False,
                None
            )

        try:
            if (isinstance(record, list)):
                record = Storage(zip(self.cols, record))
            (record_is_error, error_data) = is_error()
            skip_record = Storage(
                                    {
                                        'record_is_error': (record_is_error is True),
                                        'ignore_action':  AND(
                                                                (record.db_action is not None),
                                                                (record.db_action in ignore_actions)
                                                                ),
                                        'is_transaction_record': (isnum(tablename) is True),
                                        'cols_NE_record_keys': (self.cols != record.getkeys()),
                                        'inst_tablename_NE_tablename': (tablename != self.tablename)
                                    }
            )
            for reason in skip_record:
                if AND(
                        (skip_record[reason] is True),
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
                self.cols
            ) = \
                (
                    True,
                    Lst(),
                    None,
                    Lst()
                )
        finally:
            self.loginfo('DLGRecordSet records & query have been reset')

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
                    res = self.objp4.__eq__(str(record[fieldname]), str(value))
                elif (
                        op in ('!=', NE)
                ):
                    res =self.objp4.__ne__(str(record[fieldname]), str(value))
                elif (
                        op in ('<', LT)
                ):
                    res = self.objp4.__lt__(float(record[fieldname]), float(value))
                elif (
                        op in ('<=', LE)
                ):
                    res = self.objp4.__le__(float(record[fieldname]), float(value))
                elif (
                        op in ('>', GT)
                ):
                    res = self.objp4.__gt__(float(record[fieldname]), float(value))
                elif (
                        op in ('>=', GE)
                ):
                    res = self.objp4.__ge__(float(record[fieldname]), float(value))
                elif (
                        op in (NOT)
                ):
                    res = ~record[fieldname]
                elif (
                        op in ('#', '#?', CONTAINS, SEARCH)
                ):
                    res = (self.objp4.memoizefield(value).search(str(record[fieldname])) is not None)
                elif (
                        op in ('#^', STARTSWITH)
                ):
                    res = (self.objp4.memoizefield(f'^{value}').match(str(record[fieldname])) is not None)
                elif (
                        op in ('#$', ENDSWITH)
                ):
                    res = (self.objp4.memoizefield(f'{value}$').match(str(record[fieldname])) is not None)
                elif (
                        op in ('#^$', '##', MATCH)
                ):
                    res = (self.objp4.memoizefield(f'^{value}$').match(str(record[fieldname])) is not None)
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

    def parse(self, qry, record=None, opfunc=None):
        if (type(qry) is LambdaType):
            out = OR(
                    AND(
                        qry(record), 1
                    ), 0
            )
        elif OR(
                (isinstance(qry, (str, list))),
                (type(qry).__name__ in (
                        'Storage',
                        'DLGQuery',
                        'DLGExpression'
                    )
                )
        ):
            if (opfunc is not None):
                qright = qry.right
                value = record[qry.left.fieldname]
                ''' cast numeric fields to int so that
                    the record's field value and the qry's
                    right side are both living on the same 
                    plaing field
                '''
                if (isnum(qright) is True):
                    qright = int(qright)
                    if (isnum(value) is True):
                        value = int(value)
                ''' TODO: revisit this - I don't remember why
                    evaluating left against right in one case
                    the evaluating right against left in the
                    other...
                '''
                receval = opfunc(value, qright) \
                    if (type(qry).__name__ in (
                    'DLGQuery',
                    'DLGExpression'
                )
                        ) \
                    else opfunc(qright, value)
            else:
                receval = self.evaluate(qry, record=record)
            if (isinstance(receval, bool) is True):
                receval = int(receval)
            out = OR(
                    AND(
                        receval, 1
                    ), 0
            )
        else:
            out = OR(
                    AND(
                        qry, 1
                    ), 0
            )
        return out

    def _validate_exp_qry(self, op):
        (
            op_expression,
            op_query
        ) = \
            (
                expression_table(op),
                optable(op)
            )
        return op_expression \
            if (op_expression is not None) \
            else op_query \
            if (op_query is not None) \
            else None

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
            exp_func = expression_table(opname)
            qry_func = optable(opname)
            ''' queries can be wrapped in AND/OR/XOR/NOT expressions...
                start with these
            '''
            operators = andops + orops + xorops + notops
            if (opname in operators):
                (buildleft, buildright) = (None, None)
                buildleft = self.build_results(left, record)
                if (right is not None):
                    buildright = self.build_results(right, record)
                    if (isinstance(buildright, (int, bool)) is False):
                        if (buildright.right is not None):
                            buildright = buildright.right

                if (
                        opname in (AND, 'AND', '&')
                ):
                    built = AND(
                                 buildleft,
                                 buildright
                )
                elif (
                        opname in (OR, 'OR', '|')
                ):
                    built = OR(
                                buildleft,
                                buildright
                    )
                elif (
                        opname in (XOR, 'XOR', '^')
                ):
                    built = XOR(
                                 buildleft,
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
                    time to handle queries & expressions
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

    def update_datefields(self, record, fieldnames):
        for name in fieldnames:
            try:
                datestamp = self.oDateTime.to_p4date(record[name])
                record.merge({name: datestamp})
            except Exception as err:
                self.logerror(f'Failed to convert Date field ({name}) from epoch to datestamp.\n{err}')
        return record

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

    def filter_records(self, records, **kwargs):
        kwargs = Storage(kwargs)
        count = kwargs.count
        orderby = kwargs.orderby
        limitby = kwargs.limitby
        groupby = kwargs.groupby
        sort = kwargs.sort
        find = kwargs.find
        filter = kwargs.filter
        exclude = kwargs.exclude
        search = kwargs.search
        filters = pformat(
            [
                fltr for fltr in
                           (
                               orderby,
                               limitby,
                               groupby,
                               sort,
                               find,
                               filter,
                               exclude,
                               search
                           )
                if (fltr is not None)
            ]
        )
        self.loginfo(f'Applying filters: {filters}')
        if (orderby is not None):
            '''  orderby         -->     and/or limitby
            '''
            try:
                if (isinstance(orderby, str)):
                    orderby = [item for item in orderby.split(',')] \
                        if (',' in orderby) \
                        else [orderby]
                records = records.orderby(*orderby) \
                    if (limitby is None) \
                    else records.orderby(*orderby, limitby=limitby)
            except Exception as err:
                bail(err)
        elif (limitby is not None):
            '''  limitby         -->     and nothing else...
            '''
            try:
                records = records.limitby(limitby)
            except Exception as err:
                bail(err)
        if (groupby is not None):
            '''  groupby
            '''
            try:
                records = records.groupby(groupby)
            except Exception as err:
                bail(err)
        if (exclude is not None):
            '''  exclude

                    >>> for record in records.exclude(lambda rec: rec.type=='99'):
                    >>>     print record.client
                    Catmart_client
            '''
            try:
                records = records.exclude(exclude)
            except Exception as err:
                bail(err)
        if (filter is not None):
            '''  filter
            '''
            try:
                records = records.filter(filter)
            except Exception as err:
                bail(err)
        if (find is not None):
            '''  find
            '''
            try:
                records = records.find(find)
            except Exception as err:
                bail(err)
        if (sort is not None):
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
            try:
                records = records.sort(sort)
            except Exception as err:
                bail(err)
        if (search is not None):
            for record in records:
                if (record.depotFile is not None):
                    fcontent = ''
        return records

    def get_recordsIterator(self, records):
        reg = re.compile(f'^db\.{self.tablename}$')
        if (self.is_jnlobject is True):
            try:
                target_records = [
                    rec[1] for (idx, rec) in enumerate(records)
                    if (reg.match(Lst(Lst(rec)(1))(2)))
                ]
                self.loginfo(f'{len(target_records)} target records')
                return enumerate(target_records, start=1)
            except Exception as err:
                bail(err)
        return enumerate(records, start=1) \
                if (type(records) is not enumerate) \
                else records

    def get_records_datetime_fields(self, tablename):
        ''' promote fields that looks like date/time fields &
            them to p4 formatted date/time stamps (yyyy/mm/dd)
        '''
        datetime_fields = []
        table = getattr(self.objp4, tablename)
        for fld in table.fields:
            (
                fieldname,
                fieldtype
            ) \
                = (
                table.fields[fld].fieldname,
                table.fields[fld].type
            ) \
                if (isinstance(fld, str)) \
                else (
                fld.name,
                fld.type
            ) \
                if (is_fieldType(fld) is True) \
                else (
                fld.fieldname,
                fld.type
            ) \
                if (type(fld).__name__ == 'Storage') \
                else (
                None,
                None
            )
            if (fieldname in (
                    'date',
                    'Date',
                    'Access',
                    'Update',
                    'accessDate',
                    'updateDate',
            )
            ):
                datetime_fields.append(fieldname)
        return datetime_fields

    def select(
            self,
            *fieldnames,
            records=None,
            cols=None,
            query=None,
            raw_records=False,
            close_session=True,
            **kwargs
    ):
        (fieldnames, kwargs) = (
            Lst(fieldnames or self.objp4.fieldnames),
            Storage(kwargs)
        )
        if (query is None):
            query = self.query

        tablename = self.tablename or kwargs.tablename
        if (self.tablename is None):
            self.tablename = tablename
        if AND(
                (tablename is None),
                (query is not None)
        ):
            left = query[0].left \
                if (isinstance(query, list)) \
                else query.left
            tablename = left.tablename

        if (cols is None):
            cols = self.cols or Lst()
        if AND(
                (len(cols) > 0),
                (not 'idx' in cols)
        ):
            cols.insert(0, 'idx')
        if (records is None):
            records = self.records
        ''' records is not enumerator & whatever 
            it is, it is empty! Just do the few 
            lines below and get out!
        '''
        if (type(records) != enumerate):
            if (len(records) == 0):
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
                out = None
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
                    out = opfunc(left, right)
                if AND(
                        (isinstance(right, DLGRecords) is True),
                        (is_array(out) is True )
                ):
                    out = DLGRecords(out, Lst(), self.objp4)
                return out

        ''' remove field `code`
        '''
        if (noneempty(cols) is False):
            if ('code' in cols):
                try:
                    cols.pop(cols.index('code'))
                except Exception as err:
                    self.objp4.logwarning(err)

        fieldnames = Lst(fieldnames).storageindex(reversed=True)

        (
            eor,
            recordcounter
        ) = \
            (
                False,
                0
            )
        outrecords = Lst()
        ''' insertrecords 
            decide now if outrecords should be 
            returned as a list or as DLGRecords
            (insert or append?)
        '''
        insertrecord = outrecords.append
        if (raw_records is False):
            outrecords = DLGRecords(Lst(), cols, self.objp4)
            insertrecord = outrecords.insert
        datetime_fields = self.get_records_datetime_fields(tablename)
        try:
            recordsiter = self.get_recordsIterator(records)
            while (eor is False):
                table_mismatch = False
                try:
                    if (tablename is None):
                        bail(
                            "tablename may not be None!"
                        )
                    if (reg_dbtablename.match(tablename) is not None):
                        tablename = self.oTableFix.normalizeTableName(tablename)
                    ''' next id & record
                    '''
                    (
                    idx,
                    record
                    ) = (
                        next(recordsiter)
                    )
                    ''' TODO: other abstractions may frown on this validation...
                        not everyone comes across as being a list of things!
                        
                        maybe we should do `if (self.is_jnlobject is True)` instead?
                    '''
                    if (isinstance(record, list) is True):
                        ''' Querying journal records 
                        '''
                        record = Lst(record)
                        ''' action field: pv, dv, mx, etc. 
                            value of actionfield = 0 if not idx else 1 
                        '''
                        actionfield = 0
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
                            # what if depotFile does not exist? maybe action is 'deleted' ?
                            # {'code': 'error',
                            #  'data': 'Colors - no such file(s).\n',
                            #  'severity': 2,
                            #  'generic': 17}
                            # TODO: we best find a clever solution for this!
                        else:
                            table_mismatch = True
                    if (table_mismatch is False):
                        if ('idx' in cols):
                            if (not 'idx' in record):
                                record.merge({'idx': idx})

                        QResults = Lst()
                        if (len(query) == 0):
                            QResults.append(0)
                        for qry in query:
                            recresult = self.build_results(qry, record)
                            QResults.append(recresult)

                        if (sum(QResults) == len(query)):
                            ''' time to compute new columns (if any)
                            '''
                            if (len(self.compute) > 0):
                                record = Storage(self.computecolumns(record))
                            ''' match column to their records 
                            '''
                            if (len(fieldnames) > 0):
                                rec = Storage()
                                for fn in fieldnames.keys():
                                    key = fieldnames[fn]
                                    value = record[fieldnames[fn]]
                                    rec.merge({key: value})
                                record = rec

                            skip = self.skiprecord(record, tablename)
                            if (skip is False):
                                if AND(
                                        (not 'code' in cols),
                                        (record.code is not None)
                                ):
                                    record.delete('code')

                                if (len(datetime_fields) > 0):
                                    record = self.update_datefields(record, datetime_fields)
                                if (noneempty(self.maxrows) is False):
                                    recordcounter += 1
                                    if (recordcounter <= self.maxrows):
                                        insertrecord(record)
                                    else:
                                        eor = True
                                else:
                                    insertrecord(record)
                # BUG: sometimes StopIteration is skipped (oJnl.rev.depotFile.contains(...))
                # so wrapping while loop in its own try: except: finally block for now...
                # until time for this is to be had... argh!!!!!
                except (StopIteration, EOFError):
                    eor = True
                    if (hasattr(records, 'read')):
                        records.close()
                except Exception as err:
                    eor = True
                    bail(err)
        finally:
            if (len(kwargs) > 0):
                outrecords = self.filter_records(outrecords, **kwargs)
                self.loginfo(f'records filtered: {len(outrecords)}')
            self.loginfo(f'record counter: {recordcounter}')
            self.loginfo(f'records retrieved {len(outrecords)}')
            if (close_session is True):
                self.close()
            return outrecords