from libsql import DLGSql
from libdlg.dlgDateTime import DLGDateTimeConvert
from libsql.sqlRecords import Records
from libdlg.dlgStore import ZDict, Lst
from libdlg.dlgUtilities import (
    bail,
    reg_dbtablename,
)

'''  [$File: //dev/p4dlg/libsql/sqlSelect.py $] [$Change: 609 $] [$Revision: #7 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
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
        kwargs = ZDict(kwargs)
        ''' define & get relevant record components (tablename, fieldnames, query, cols & records))
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

        if (tablename is None):
            if (len(fieldnames) > 0):
                try:
                    fld = fieldnames[0]
                    tablename = fld.tablename
                except:
                    bail(
                    "tablename may not be None!"
                    )

        kwargs.delete('fieldsmap', 'tablename')
        if (records is None):
            return Records(records=[], cols=cols, objp4=self.objp4)

        (
            eor,
            recordcounter
        ) = \
            (
                False,
                0
            )

        distinctrecords = ZDict()
        aggregators = (
                'groupby',
                'having',
                'sortby',
                'orderby',
                'distinct'
        )
        for aggregator in aggregators:
            if (kwargs[aggregator] is None):
                kwargs.delete(aggregator)

        ''' set / define joiners & params
        '''
        (
            oJoin,
            jointype,
            flat,
        ) = \
            (
                None,
                None,
                kwargs.flat or False,
            )

        ''' Are we joining records ? 
            What kind of join? inner/join - outer/left - merge_records ?
            Should we flatten records ? (default is False)
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
                ''' next id & record
                '''
                (
                idx,
                record
                ) = (
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
                    for qry in query:
                        recresult = self.build_results(qry, record)
                        QResults.append(recresult)
                    ''' sum the results & keep (or skip) the record 
                    '''
                    if (sum(QResults) == len(query)):
                        ''' if any, compute new columns now and adjust the record
                        '''
                        if (len(self.compute) > 0):
                            record = ZDict(self.computecolumns(record))
                        ''' match column to their records 
                            hum...
                            
                        intersect = fieldnames.getvalues().intersect(record.getkeys())
                        if (len(intersect) != len(fieldnames)):
                            raise RecordFieldsNotMatchCols(fieldnames.getvalues(), record.getkeys())
                        '''
                        if (len(fieldnames) > 0):
                            ''' we have a custom field list to output! - re-define the record accordingly!
                            '''
                            rec = ZDict()
                            for fn in fieldnames.keys():
                                field = fieldnames[fn]
                                fieldtype = type(field).__name__
                                if (fieldtype in ('JNLField', 'Py4Field')):
                                    field = field.fieldname
                                value = record[field]
                                rec.merge({field: value})
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
                                fnames = fieldnames.getvalues() \
                                    if (type(fieldnames).__name__ == 'StorageIndex') \
                                    else fieldnames
                                record = (
                                    DLGDateTimeConvert(self.objp4)
                                          (
                                        record=record,
                                        tablename=tablename,
                                        datetype=datetype
                                    )
                                )
                                ''' check if any other field values need to be converted from 
                                    field flags or masks (as per the p4 schema definition) 
                                    
                                    TODO: this!
                                '''

                            ''' should records be distinct?
                            '''
                            if (distinct is not None):
                                distinct = self.validate_distinct(distinct)
                                distinctvalue = record(distinct)
                                ''' what was I thinking??!!!! 
                                    TODO: rewrite this aggregator!
                                '''
                                if (distinctvalue is not None):
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
                                    ''' that's it for this record, for now, insert and move on! 
                                    '''
                                    outrecords.insert(idx, record)
            except (StopIteration, EOFError):
                eor = True
                if (hasattr(records, 'read')):
                    records.close()
            except Exception as err:
                eor = True
                bail(err)
        if (distinct is not None):
            outrecords = Records(distinctrecords.getvalues(), cols, self.objp4)
        if (len(kwargs) > 0):
            outrecords = self.aggregate(outrecords, **kwargs)
            self.loginfo(f'records filtered: {len(outrecords)}')
        self.loginfo(f'record counter: {recordcounter}')
        self.loginfo(f'records retrieved {len(outrecords)}')

        ''' Time to join/merge records 
        '''
        if (oJoin is not None):
            (
                joiner,
                joinargs
            ) = \
                (
                    oJoin(outrecords).join,
                    {'flat': flat}
                ) \
                    if (jointype == 'inner') \
                    else \
                    (
                        oJoin(outrecords).left,
                        {'flat': flat}
                    )
            outrecords = joiner(**joinargs)

        if (close_session is True):
            self.close()

        return outrecords