from libdlg.dlgStore import Storage, Lst, StorageIndex
from libjnl.jnlFile import JNLFile
from libdlg.dlgRecordset import DLGRecordSet
from libdlg.dlgRecords import DLGRecords
from libdlg.dlgUtilities import (
    is_Py4,
    is_P4Jnl
)
from libdlg.dlgRecord import DLGRecord

'''  [$File: //dev/p4dlg/libdlg/dlgSelect.py $] [$Change: 479 $] [$Revision: #56 $]
     [$DateTime: 2024/09/20 07:42:22 $]
     [$Author: mart $]
'''

__all__ = ['DLGJoin']

class DLGJoin(object):
    '''
        USAGE:

        * No need to access this class directly. Best to access via _table.on

            >>> jnl.change.on(jnl.rev.change == jnl.change.change)

        >>> recs = jnl(jnl.rev).select(join=jnl.change.on(jnl.rev.change == jnl.change.change))

        * alternatively, this is equivalent:
        >>> recs = jnl(jnl.rev.change == jnl.change.change).select()

        >>> recs
        <DLGRecords (17610)>

        >>> recs(0)
        <DLGRecord {'action': '8',
         'change': '142',
         'client': 'lxcharlotte.pycharm',
         'date': '2021/11/25',
         'db_action': 'pv',
         'depotFile': '//depot/pycharmprojects/sQuery/lib/sqFileIO.py',
         'depotRev': '1',
         'descKey': '142',
         'description': 'renaming for case consistency',
         'digest': '45C82D6A13E755DEBDE0BD32EA4B7961',
         'identify': '',
         'idx': 1,
         'importer': '',
         'lbrFile': '//depot/pycharmprojects/sQuery/lib/sqfileUtils.py',
         'lbrIsLazy': '1',
         'lbrRev': '1.121',
         'lbrType': '0',
         'modTime': '1630482775',
         'root': '//depot/pycharmprojects/sQuery/lib/*',
         'size': '18420',
         'table_name': 'db.rev',
         'table_revision': '9',
         'traitLot': '0',
         'type': '0',
         'user': 'bigbird'}>

         The WorkFLows:

             join (innerjoin)

            1.  lefttable = jnl.rev
                righttable = jnl.change
                query = (lefttable.change == righttable.change)
                ...
                    oRecordset = objp4(constraint)
                    constraint = query
                    query = constraint.left     --> (type JNLTable) --> do not append to jnlQueries
                    ...
                    oRecordSet = DLGRecordset(objp4, oJNLFile, **tabledata)
                    * query is a JNLTable, so do not pass as query
                    return oRecordset(constraint=constraint)
                    ...

                    oRecordset.select()
                        oSelect = DLGSelect()
                        if (constraint is not None):
                            kwargs.update(**{'join': jnl.change.on(constraint)})
                        records = oSelect.select(*fieldnames, **kwwargs)
                        return joined records

            2.  lefttable = jnl.rev
                constraint = (jnl.rev.change == jnl.change.change)
                records = jnl(lefttable).select(constraint)


            left (left outerjoin)

                lefttable = jnl.rev
                righttable = jnl.change
                constraint = (jnl.rev.change == jnl.change.change)
                records = objp4().select(
                            lefttable.ALL, righttable.ALL,
                            left = righttable.on(constraint)
                            )
    '''

    def __init__(
            self,
            objp4,
            constraint,
    ):
        self.objp4 = objp4
        self.constraint = constraint
        self.records = None
        self.left_records = None
        self.exclude_filednames = [
            'idx',
            'db_action',
            'table_revision',
            'table_name',
            'access',
            'accessDate',
            'update',
            'updateDate',
            'status',
            'code'
        ]
        self.cField = self.constraint.left.fieldname
        self.cRecordset = None
        self.cGroupRecords = None
        self.cMemo = {}

    def __call__(self, records=None):
        self.left_records = records \
            if (records is not None) \
            else self.guess_records()
        self.cRecordset = self.define_recordset()
        self.cGroupRecords = self.group_records()
        return self

    def memoize_records(self, key, record=None):
        memo = {}
        try:
            memo = self.cMemo[key]
        except KeyError:
            if (record is not None):
                memo = self.cMemo[key] = record
        return memo

    def guess_records(self):
        records = (
            JNLFile(self.objp4.journal, self.objp4.reader)
        ) if (is_P4Jnl(self.objp4) is True) \
            else getattr(self.left_records, '__call__')(*self.left_records.options) \
            if (type(self.left_records).__name__ == 'Py4Run') \
            else self.objp4()#self.constraint.right._table)#DLGRecords(Lst(), Lst(), self.objp4)
        return records

    def define_recordset(self):
        cQuery = self.constraint.right._table
        cTablename = cQuery.tablename
        cTabledata = self.objp4.memoizetable(cTablename)
        [
            cTabledata.update(**{kitem: self.objp4[kitem]}) for kitem in (
            'recordchunks',
            'schemadir',
            'oSchemaType',
            'logger',
            'maxrows'
        )
        ]
        cTabledata.update(
            **{
                'tablename': cTablename,
                'constraint': self.constraint,
                'tabletype': type(self.objp4)
            }
        )
        ''' There are constraints for linking 2 table (SQL JOIN).
            build a dedicated recordset then pass it on to
            Select.select via `oRecordSet`
        '''
        records = self.records or self.guess_records()
        cRecordset = DLGRecordSet(self.objp4, records, **cTabledata)
        [setattr(cRecordset, cKey, cValue) for (cKey, cValue) in {
            'constraint': self.constraint,
            'tabledata': Storage(cTabledata)
        }.items()]
        return cRecordset

    def select_cRecords(self):
        return self.cRecordset.select()

    def group_records(self):
        cRecords = self.select_cRecords()
        cGroupRecords = cRecords.groupby(
            self.cField,
            orderby='idx',
            groupdict=True
        )
        return cGroupRecords

    def join(self):
        #cKeys = self.cMemo.keys()
        mRecords = DLGRecords(records=[], cols=[], objp4=self.objp4)
        records = self.left_records
        for record in records:
            fieldvalue = record[self.cField.fieldname]
            crecord_right = self.cGroupRecords[fieldvalue]
            crecord_right.delete(*self.exclude_filednames)
            if (crecord_right is not None):
                #crecord_right = self.memoize_records(str(fieldvalue))
                record.merge(crecord_right)
                mRecords.insert(record)
        return mRecords

    def left(self, exclude_matches=False):
        mRecords = DLGRecords(records=[], cols=[], objp4=self.objp4)
        records = self.left_records
        for record in records:
            fieldvalue = record[self.cField.fieldname]
            crecord_right = self.cGroupRecords[fieldvalue]
            crecord_right.delete(*self.exclude_filednames)
            if ( crecord_right is not None):
                record.merge(crecord_right)
            mRecords.insert(record)
        return mRecords


    


