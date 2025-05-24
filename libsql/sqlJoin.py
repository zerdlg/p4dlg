from itertools import product

from libsql.sqlValidate import *
from libsql.sqlRecords import Records
from libsql.sqlRecord import Record
from libdlg.dlgUtilities import isnum, bail
from libdlg.dlgStore import Storage

'''  [$File: //dev/p4dlg/libsql/sqlJoin.py $] [$Change: 724 $] [$Revision: #18 $]
     [$DateTime: 2025/05/19 20:19:42 $]
     [$Author: zerdlg $]
'''

__all__ = ['Join']

class Join(object):
    '''
        USAGE & details:

                                merge_records - join - left

        merge_records:
            A non-exceptional merging of 2 records into one (like braiding), where
            matches from the right side overwrite those on the left.

            Equivalent to join + flat=True

            eg.
            >>> reference = (jnl.rev.change == jnl.change.change)
            >>> recs = jnl(jnl.rev).select(merge_records=jnl.change.on(reference))

                which is equivalent to using join + flat=True
                >>> recs = jnl(jnl.rev).select(join=jnl.change.on(reference), flat=True)

            >>> recs.first()
            <Record {'action': '8',
                    'change': '142',
                    'client': 'zerdlg.pycharm',
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
                    'user': 'zerdlg'}>

        join (inner):
            A merging of 2 records into one. however, the tablename must be included in the syntax (I.e.:
            rec.rev.depotFile & rec.change.user) since a join's default behaviour (flat=False) is to
            contain both records.

            Note that records are skipped where inner fields are non-matching.

            eg.
            >>> recs = jnl(jnl.rev).select(join=jnl.change.on(reference))

                * alternatively, this syntax is equivalent:
                >>> recs = jnl(reference).select()

            >>> recs.first()
            <Record {'change': <Record {'access': '',
                                      'change': '142',
                                      'client': 'zerdlg.pycharm',
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
                                      'user': 'zerdlg'}>,
                'rev': <Record {'action': '8',
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
            Change `142` on depotFile `//depot/pycharmprojects/sQuery/lib/sqFileIO.py` by user `zerdlg`

        left (outer):
            like join but records with non-matching fields are included in overall outrecords.

            eg.
            >>> recs = jnl(jnl.rev).select(left=jnl.change.on(reference))

        braid (merge tables based on a reference/query):
            eg.
            >>> recs = jnl(p4.<table1>).select(mergetable=p4.<table2>.on(QRY))
    '''

    def __init__(
            self,
            objp4,
            reference,
            flat=False
    ):
        self.objp4 = objp4
        self.reference = reference
        self.flat = flat
        self.as_groups = False
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

    def __call__(self, records=Records([])):
        if (len(records) == 0):
            bail('Cannot join empty records')
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
            'schemadir',
            'oSchemaType',
            'logger',
        )
        ]
        cTabledata.update(
            **{
                'tablename': cTablename,
                'reference': self.reference,
                'tabletype': type(self.objp4)
            }
        )
        ''' There is a reference for linking 2 table (SQL JOIN).
            build a dedicated recordset then pass it on to
            Select.select via `oRecordSet`
        '''
        cRecordset = self.objp4(cQuery)
        return cRecordset

    def select_and_group_records(self, recset, orderby=None):
        if orderby is None:
            cfield = self.cField
            orderby = self.cField or 'idx'
        cRecords = recset.select()
        cGroupRecords = cRecords.groupby(
            self.cField,
            orderby=orderby,
            #as_groups=True
        )
        return cGroupRecords

    def is_matching(self, left, right):
        fsum = sum([(1 | 0) for field in left.getkeys() if (field in right.getkeys())])
        return True \
            if (fsum > 0) \
            else False

    def join_record(self, jointype=None, flat=False, maxrows=0):
        outrecords = Records(
            records=[],
            cols=[],
            is_flat=flat,
            is_join=True,
            objp4=self.objp4
        )

        (
            lefttable,
            righttable,
        ) = \
            (
                self.reference.left._table,
                self.reference.right._table,
            )
        (
            leftrecords,
            rightrecords
        ) = \
            (
                self.cGroupRecords,
                self.left_records
            ) \
                if (
                    len(self.cGroupRecords) < len(self.left_records)
            ) \
                else (
                self.left_records,
                self.cGroupRecords
            )

        reffield = self.cField.fieldname \
            if (is_fieldType(self.cField) is True) \
            else self.cField
        refkeys = set(recitem[reffield] for recitem in filter(lambda rec: rec[reffield], leftrecords))
        (eor, recordscount) = (False, 0)
        for refkey in refkeys:
            if (eor is True):
                break
            ''' `exclude` will retrieve the target records while 
                removing them from the original Records.
                
                if maxrows > 0, we can assume at this point that 
                the number of left records has already reached its 
                maxrows - so we work on rigt_records
            '''
            left_records = leftrecords.exclude(lambda rec: (rec[reffield] == refkey))
            right_records = rightrecords.exclude(lambda rec: (rec[reffield] == refkey))
            for leftrecord in left_records:
                if (eor is False):
                    for rightrecord in right_records:
                        if (maxrows > 0):
                            recordscount += 1
                            if (recordscount > maxrows):
                                eor = True
                                break
                        if (flat is True):
                            updt_record = Record(leftrecord.merge(**rightrecord))
                        else:
                            updt_record = Record(
                                {
                                    lefttable.tablename: leftrecord,
                                    righttable.tablename: rightrecord
                                }
                            )
                        outrecords.insert(updt_record)
                else:
                    break
        if (
                (len(outrecords) > 0) &
                (len(outrecords.cols) == 0)
        ):
            outrecords.cols = outrecords(0).getkeys()
        return outrecords

    def braid(self, flat=True):
        ''' TODO: this - modify code below to fit brading requirements.

            * should we re-define keying fields for arbitrary
              mix and match to force a relationship?
        '''
        utrecords = Records(
            records=[],
            cols=[],
            is_flat=flat,
            is_join=True,
            objp4=self.objp4
        )

        (
            lefttable,
            righttable,
        ) = \
            (
                self.reference.left._table,
                self.reference.right._table,
            )
        (
            leftrecords,
            rightrecords
        ) = \
            (
                self.cGroupRecords,
                self.left_records
            ) \
                if (
                    len(self.cGroupRecords) < len(self.left_records)
            ) \
                else (
                self.left_records,
                self.cGroupRecords
            )

        reffield = self.cField.fieldname \
            if (is_fieldType(self.cField) is True) \
            else self.cField

        (eor, recordscount) = (False, 0)
        refkeys = set(recitem[reffield] for recitem in filter(lambda rec: rec[reffield], leftrecords))
        for refkey in refkeys:
            if (eor is True):
                break
            ''' `exclude` will retrieve the target records while 
                removing them from the original Records.
            '''
            left_records = leftrecords.exclude(lambda rec: (rec[reffield] == refkey))
            right_records = rightrecords.exclude(lambda rec: (rec[reffield] == refkey))
            for leftrecord in left_records:
                if (eor is False):
                    for rightrecord in right_records:
                        if (maxrows > 0):
                            recordscount += 1
                            if (recordscount > maxrows):
                                eor = True
                                break
                    if (flat is True):
                        updt_record = Record(leftrecord.merge(**rightrecord))
                    else:
                        updt_record = Record(
                            {
                                lefttable.tablename: leftrecord,
                                righttable.tablename: rightrecord
                            }
                        )
                    outrecords.insert(updt_record)
                else:
                    break
        if (len(outrecords.cols) == 0):
            outrecords.cols = outrecords(0).getkeys()
        return outrecords0

    def merge_records(self, flat=True):
        return self.join_record(
            jointype='inner',
            flat=flat
        )

    def braid(self, flat=True):
        return self.braid(
            flat=flat
        )

    def join(self, flat=False, maxrows=0):
        return self.join_record(
            jointype='inner',
            flat=flat,
            maxrows=maxrows
        )

    def left(self, flat=False):
        return self.join_record(
            jointype='outer',
            flat=flat
        )