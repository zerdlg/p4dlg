from libdlg.dlgDateTime import DLGDateTimeConvert
from libsql.sqlRecords import Records
from libdlg.dlgStore import Storage, Lst, StorageIndex
from libdlg.dlgUtilities import (
    bail,
    reg_dbtablename,
    noneempty,
)
from libsql.sqlControl import *
from libsql.sqlValidate import *

'''  [$File: //dev/p4dlg/libsql/sqlSelect.py $] [$Change: 693 $] [$Revision: #28 $]
     [$DateTime: 2025/04/22 07:22:55 $]
     [$Author: mart $]
'''

__all__ = ('Select',)

class Select(DLGSql):
    def select(
            self,
            *fieldnames,
            records=None,
            cols=None,
            query=None,
            distinct=None,
            close_session=True,
            leave_field_values_untouched=False,
            datetype='datetime',
            **kwargs
    ):

        #if (len(fieldnames) == 1):
        #    if (isinstance(fieldnames[0], list) is True):
        #        fieldnames = Lst(fieldnames).pop(0)
        (
            kwargs,
            eor,
            recordcounter,
            distinctrecords,
            distinctvalues,
            oJoin,
            jointype,
            expression,
        ) = \
            (
                Storage(kwargs),
                False,
                0,
                Storage(),
                set(),
                None,
                None,
                None,
            )
        flat = kwargs.flat or False

        ''' define, resolve & get relevant record components 
            (tablename, fieldnames, fieldsmap, query, cols & records)
        '''
        (
            tablename,
            fieldnames,
            fieldsmap,
            query,
            cols,
            records
        ) = self.get_record_components(
            *fieldnames,
            query=query,
            cols=cols,
            records=records,
            **kwargs
        )
        #if (query is None):
        #    query = self.query
        ''' last minute queries and expressions
        '''
        cfieldnames = StorageIndex(fieldnames.copy())
        for fidx in cfieldnames.getkeys():
            if (is_expressionType(cfieldnames[fidx]) is True):
                ''' Handle expressions that attempt to pass for queries!
                '''
                expression = fieldnames.pop(fidx)
                if (is_substrType(expression) is False):
                    #fieldnameidx = fieldnames.index(expression)
                    #fieldnames.pop(fieldnameidx)
                    # fieldnames.mergein(expression.left, fidx)
                    # expressions.append(expression)
                    opname = expression.op.__name__.lower() \
                        if (callable(expression.op) is True) \
                        else expression.op.lower()
                    kwargs.update(**{opname: expression})
                    break
            elif (is_queryType(cfieldnames[fidx]) is True):
                if (query is None):
                    query = fieldnames.pop(fidx)
                elif (isinstance(query, list) is True):
                    query.append(query)
        if (is_tableType(fieldnames(0)) is True):
            bail("there doesn't seem to be any valid reason to pass in a DLGTable class reference as a parameter when selecting records.")

        ''' check expression & distinct values for substring expressions now
            since they need to be handled *before* we process aggregators
        '''
        fieldname = None
        if (is_substrType(expression) is True):
            fieldname = expression.fieldname
        elif (is_expressionType(distinct) is True):
            expression = distinct
            if (is_substrType(distinct) is True):
                fieldname = distinct.fieldname
                distinct = True

        kwargs.delete('fieldsmap', 'tablename')
        if (records is None):
            return Records(records=[], cols=cols, objp4=self.objp4)
        aggregators = (
                'groupby',
                'having',
                'sortby',
                'orderby',
                'distinct',
                'substr',
                'count',
                'sum',
                'avg',
                'min',
                'max',
                'len',
                'add',
                'sub',
                'mul',
                'mod',
                'match',
                'search',
                'regex',
        )
        kwargs.delete(*[aggregator for aggregator in aggregators if (kwargs[aggregator] is None)])
        ''' Are we joining records ? 
            What kind of join? inner/join - outer/left - merge_records ?
            Should we flatten records ? (default is False)
            
            In any case, time to set that up.
        '''
        if (
                (kwargs.merge_records is not None) |
                (kwargs.join is not None) |
                (kwargs.left is not None)
        ):
            merge_or_join = (kwargs.merge_records or kwargs.join)
            (oJoin, jointype) = (merge_or_join, 'inner') \
                if (merge_or_join is not None) \
                else (kwargs.left, 'outer')
            if (kwargs.merge_records is not None):
                flat = True
            if (flat is True):
                oJoin.flat = True
        kwargs.delete(
            *[
                'flat',
                'join',
                'left',
                'merge_records'
            ]
        )
        if (
                (oJoin is None) &
                (self.reference is not None)
        ):
            oJoin = self.reference.right._table.on(self.reference)
        outrecords = Records(Lst(), cols, self.objp4)
        recordsiter = self.get_recordsIterator(records)
        while (eor is False):
            skip_record = False
            try:
                if (reg_dbtablename.match(tablename) is not None):
                    tablename = self.oTableFix.normalizeTableName(tablename)

                (
                idx,
                record
                ) = \
                    (
                        next(recordsiter)
                    )

                ''' some abstractions may frown on this validation...
                    not everyone comes across as being a list of things!
                '''
                if (isinstance(record, list) is True):
                    (record, skip_record) = self.ziprecord(record, cols, idx)
                ''' first skip_record check
                '''
                if (skip_record is False):
                    ''' p4 tables are keyed tables, and field `id` 
                        is already in use in a few tables (I.e. db.server). 
                        so p4dlg will adopt filed name `idx` in its place,
                        for now. 
                         
                        * add it to cols if needed.
                    '''
                    if (
                            ('idx' in cols) &
                            (not 'idx' in record)
                    ):
                        record.merge({'idx': idx})
                    ''' evaluate the current record & collect 
                        the result of each query statement                         
                    '''
                    QResults = Lst()
                    if (len(query) == 0):
                        QResults.append(0)
                    '''
                    if (expression is not None):
                        op = expression.op \
                            if (expression.left.op is None) \
                            else expression.left.op
                        opname = op.__name__.lower() \
                            if (callable(op) is True) \
                            else op.lower()
                        #if (opname in ('len', ))
                        rec = op(expression.left, record)
                        recresult = self.build_results(expression, record)
                        #if (exp_opname is not None):
                        #    opname = exp_opname
                        record.update(**{opname: recresult})
                    '''
                    for qry in query:
                        recresult = self.build_results(qry, record)
                        QResults.append(recresult)
                    ''' sum the results & keep (or skip) the record 
                    '''
                    if (sum(QResults) == len(query)):
                        ''' if any, compute new columns now and adjust the record
                        '''
                        if (len(self.compute) > 0):
                            record = Storage(self.computecolumns(record))
                        ''' match column to their records 
                            hum...
                            
                        intersect = fieldnames.getvalues().intersect(record.getkeys())
                        if (len(intersect) != len(fieldnames)):
                            raise RecordFieldsNotMatchCols(fieldnames.getvalues(), record.getkeys())
                        '''
                        if (len(fieldnames) > 0):
                            ''' we have a custom field list to output! - re-define the record accordingly!
                            '''
                            rec = Storage()
                            for fn in fieldnames.keys():
                                field = fieldnames[fn].fieldname \
                                    if (is_fieldType(fieldnames[fn]) is True) \
                                    else fieldnames[fn]
                                value = record[field]
                                rec.merge({field: value})
                                #record.update(**{field: value})
                            record = rec
                        ''' should this record should be skipped? check again.
                        '''
                        #skip_record = self.skiprecord(record, tablename)
                        if (skip_record is False):
                            if (
                                    (not 'code' in cols) &
                                    (record.code is not None)
                            ):
                                record.delete('code')
                            ''' should field values remain untouched? (default is False)
                            
                                this behaviour is too encompassing
                                TODO: apply this behaviour to a list of fields (or `ALL`) instead. 
                            '''
                            if (leave_field_values_untouched is False):
                                ''' convert date/time related record field values.
                                
                                    datetype:
                                     
                                        Is the date/time stamp's default return value.
                                    
                                        eg.
                                        +----------+-----------------------+
                                        | datetime | '2024/06/29 00:00:00' |
                                        +----------+-----------------------+
                                        | date     | '2014/06/29'          |
                                        +----------+-----------------------+
                                        | time     | '00:00:00'            |
                                        +----------+-----------------------+
                                '''
                                record = (
                                    DLGDateTimeConvert(self.objp4)
                                          (
                                        record=record,
                                        tablename=tablename,
                                        datetype=datetype
                                    )
                                )
                                record = fields2ints(record)
                                ''' check if any other field values need to be converted from 
                                    field flags or masks (as per the p4 schema definition) 
                                    
                                    TODO: this!
                                '''

                            if (is_substrType(expression) is True):
                                expstore = Storage({'substr':expression})
                                record[fieldname] = self.aggregate(record, **expstore)
                                #kwargs.delete('substr')

                            ''' should records be distinct?
                            
                                * the distinct value can be one of:
                                    - None (skip these next few code lines) 
                                    - a bool (default is False) (again, skip)
                                    - a field object (get its fieldname)
                                    - a fieldname (which is really the thing we are looking for)
                            '''
                            if (distinct is not None):
                                if (is_expressionType(distinct) is True):
                                    kwargs.distinct = distinct
                                if (is_fieldType(distinct) is True):
                                    fieldname = distinct.fieldname
                                elif (isinstance(distinct, str) is True):
                                    fieldname = distinct
                                elif (isinstance(distinct, bool) is True):
                                    if (distinct is False):
                                        distinct = None
                                    elif (distinct is True):
                                        if (fieldname is None):
                                            qry = query(0) \
                                                if (len(query) > 0) \
                                                else expression
                                            if (qry is not None):
                                                if (is_queryType(qry) is True):
                                                    fieldname = qry.left.fieldname
                                                elif (is_fieldType_or_expressionType(qry) is True):
                                                    fieldname = qry.fieldname
                                            elif (len(fieldnames) > 0):
                                                if (fieldnames(0) is not None):
                                                    if (is_fieldType_or_expressionType(fieldnames(0)) is True):
                                                        fieldname = fieldnames(0).fieldname
                                                    else:
                                                        fieldname = fieldnames(0).left.fieldname
                                            else:
                                                for kkey in kwargs.getkeys():
                                                    if (is_expressionType(kwargs[kkey]) is True):
                                                        fieldname = kwargs[kkey].fieldname
                                    if (hasattr(distinct, 'objp4') is True):
                                        fieldname = self.validate_distinct(distinct)
                                if (fieldname is not None):
                                    distinctvalue = record(fieldname)
                                    if (distinctvalue is not None):
                                        if (distinctvalue not in distinctrecords):
                                            distinctrecords.merge({distinctvalue: record}, overwrite=False)
                            else:
                                ''' how about 'maxrows', have we set a max value?
                                '''
                                if (self.maxrows > 0):
                                    recordcounter += 1
                                    if (recordcounter <= self.maxrows):
                                        outrecords.insert(idx, record)
                                    else:
                                        eor = True
                                else:
                                    ''' that's it for this record, insert and move on! 
                                    '''
                                    outrecords.insert(idx, record)
            except (StopIteration, EOFError):
                eor = True
                if (hasattr(records, 'close')):
                    if (records.close is not None):
                        records.close()
            except Exception as err:
                eor = True
                bail(err)

        if (len(distinctrecords) > 0):
            outrecords = Records(distinctrecords.getvalues(), cols, self.objp4)
        ''' Time to join/merge records 
        '''
        if (oJoin is not None):
            if (
                    (kwargs.groupby is not None) &
                    (kwargs.as_groups is True)
            ):
                ''' If the `as_groups` attribute is set to True, so must the `flat` 
                    attribute when grouping records by fields.
                    
                    Therefore, force `flat` to True.
                '''
                flat = True
            (
                joiner,
                joinargs
            ) = \
                (
                    oJoin(outrecords, **{'as_groups': kwargs.as_groups or False}).join,
                    {'flat': flat}
                ) \
                    if (jointype == 'inner') \
                    else \
                    (
                        oJoin(outrecords, **{'as_groups': kwargs.as_groups or False}).left,
                        {'flat': flat}
                    )
            outrecords = joiner(**joinargs)

        if (len(kwargs) > 0):
            outrecords = self.aggregate(outrecords, **kwargs)
        if (close_session is True):
            self.close()
        return outrecords