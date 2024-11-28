from libdlg.dlgQuery_and_operators import (
    AND,
    OR
)
from libdlg.dlgQuery_and_operators import is_recordsType
from libjnl.jnlFile import JNLFile
from libdlg.dlgRecordset import DLGRecordSet
from libdlg.dlgRecords import DLGRecords
from libdlg.dlgRecord import DLGRecord
from libdlg.dlgUtilities import (
    is_Py4,
    is_P4Jnl
)


'''  [$File: //dev/p4dlg/libdlg/dlgSelect.py $] [$Change: 479 $] [$Revision: #56 $]
     [$DateTime: 2024/09/20 07:42:22 $]
     [$Author: mart $]
'''

__all__ = ['DLGJoin']

class DLGJoin(object):
    '''
        USAGE & details:

                                merge_records - join - left

        merge_records:
            A non-exceptional merging of 2 records into one (like braiding), where
            matches from the right side overwrite those on the left.

            Equivalen to join + flat=True

            eg.
            >>> reference = (jnl.rev.change == jnl.change.change)
            >>> recs = jnl(jnl.rev).select(merge_records=jnl.change.on(reference))

                which is equivalent to using join + flat=True
                >>> recs = jnl(jnl.rev).select(join=jnl.change.on(reference), flat=True)

            >>> recs.first()
            <DLGRecord {'action': '8',
                        'change': '142',
                        'client': 'bigbird.pycharm',
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

        join (inner):
            A merging of 2 records into one. however, the tablename must be included in the syntax (I.e.:
            rec.rev.depotFile & rec.change.user) since a join's default behaviour (flat=False) is to
            contain both records.

            Note that records are skipped where inner fields are non-matching.

            eg.
            >>> recs = jnl(jnl.rev).select(join=jnl.change.on(reference)

                * alternatively, this syntax is equivalent:
                >>> recs = jnl(reference).select()

            >>> recs.first()
            <DLGRecord {'change': <DLGRecord {'access': '',
                                              'change': '142',
                                              'client': 'bigbird.pycharm',
                                              'date': '2021/11/25',
                                              'db_action': 'pv',
                                              'descKey': '142',
                                              'description': 'renaming for case consistency',
                                              'identify': '',
                                              'idx': 1,
                                              'importer': '',
                                              'root': '',
                                              'status': '0',
                                              'table_name': 'db.change',
                                              'table_revision': '3',
                                              'user': 'bigbird'}>,
                        'rev': <DLGRecord {'action': '8',
                                           'change': '142',
                                           'date': '2021/11/25',
                                           'db_action': 'pv',
                                           'depotFile': '//depot/pycharmprojects/sQuery/lib/sqFileIO.py',
                                           'depotRev': '1',
                                           'digest': '45C82D6A13E755DEBDE0BD32EA4B7961',
                                           'idx': 1,
                                           'lbrFile': '//depot/pycharmprojects/sQuery/lib/sqfileUtils.py',
                                           'lbrIsLazy': '1',
                                           'lbrRev': '1.121',
                                           'lbrType': '0',
                                           'modTime': '1630482775',
                                           'size': '18420',
                                           'table_name': 'db.rev',
                                           'table_revision': '9',
                                           'traitLot': '0',
                                           'type': '0'}>
                        }>

            >>> print(f"Change `{rec.rev.change}` on depotFile `{rec.rev.depotFile}` by user `{rec.change.user}`")
            Change `142` on depotFile `//depot/pycharmprojects/sQuery/lib/sqFileIO.py` by user `bigbird`

        left (outer):
            like join but records with non-matching fields are included in overall outrecords.

            eg.
            >>> recs = jnl(jnl.rev).select(left=jnl.change.on(reference)
    '''

    def __init__(
            self,
            objp4,
            reference,
    ):
        self.objp4 = objp4
        self.reference = reference
        self.records = None
        self.left_records = None
        self.exclude_fieldnames = [
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
        self.cField = self.reference.left
        self.cGroupRecords = None
        self.cMemo = {}

    def __call__(self, records=None):
        self.left_records = records
        cRecordset = self.define_recordset()
        self.cGroupRecords = self.select_and_group_records(cRecordset)
        return self

    def memoize_records(self, key, record=None):
        memo = {}
        try:
            memo = self.cMemo[key]
        except KeyError:
            if (record is not None):
                memo = self.cMemo[key] = record
        return memo

    def define_recordset(self):
        cQuery = self.reference.right._table
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
                'reference': self.reference,
                'tabletype': type(self.objp4)
            }
        )
        ''' There are constraints for linking 2 table (SQL JOIN).
            build a dedicated recordset then pass it on to
            Select.select via `oRecordSet`
        '''
        records = []
        if (is_P4Jnl(self.objp4) is True):
            records = (
                JNLFile(self.objp4.journal, reader=self.objp4.reader)
            )
        elif (type(self.left_records).__name__ == 'Py4Run'):
            records = getattr(self.left_records, '__call__')(*self.left_records.options)
        cRecordset = DLGRecordSet(self.objp4, records, **cTabledata)
        return cRecordset

    def select_and_group_records(self, recset):
        cRecords = recset.select()
        cGroupRecords = cRecords.groupby(
            self.cField,
            orderby='idx',
            groupdict=True
        )
        return cGroupRecords

    def is_matching(self, left, right):
        fsum = sum([OR(1, 0) for field in left.getkeys()
                    if (field in right.getkeys())])
        return True \
            if (fsum > 0) \
            else False

    def join_record(self, jointype=None, flat=False):
        # cKeys = self.cMemo.keys()
        mRecords = DLGRecords(records=[], cols=[], objp4=self.objp4)
        records = self.left_records
        (
            lefttable,
            righttable
        ) = \
            (
                self.reference.left._table,
                self.reference.right._table
            )
        for record in records:
            fieldvalue = record[self.cField.fieldname]
            crecord_right = self.cGroupRecords[fieldvalue]
            if (crecord_right is not None):
                if (is_recordsType(crecord_right) is True):
                    crecord_right = crecord_right.last()
                crecord_right.delete(*self.exclude_fieldnames)
                # crecord_right = self.memoize_records(str(fieldvalue))
                if (flat is True):
                    record.merge(crecord_right)
                else:
                    record = DLGRecord(
                        {
                            lefttable.tablename: record,
                            righttable.tablename: crecord_right
                        }
                    )
                if (jointype == 'inner'):
                    mRecords.append(record)
            if (jointype == 'outer'):
                mRecords.append(record)
        return mRecords

    def merge_records(self, flat=True):
        return self.join_record(
            jointype='inner',
            flat=flat
        )

    def join(self, flat=False):
        return self.join_record(
            jointype='inner',
            flat=flat
        )

    def left(self, flat=False):
        return self.join_record(
            jointype='outer',
            flat=flat
        )