import os
from types import LambdaType
from libdlg.dlgRecord import DLGRecord
from libdlg.dlgStore import Lst, Storage, objectify
from libdlg.dlgSearch import Search
# from libdlg.p4qLogger import LogHandler
from libdlg.dlgUtilities import bail, xrange, noneempty
from libdlg.dlgTables import *
from libdlg.dlgError import *

'''  [$File: //dev/p4dlg/libdlg/dlgRecords.py $] [$Change: 479 $] [$Revision: #36 $]
     [$DateTime: 2024/09/20 07:42:22 $]
     [$Author: mart $]
'''

__all__ = ['DLGRecords']

# TODO: add compounds like: split, apply (agregate), combine (merge)
class DLGRecords(object):
    __repr__ = lambda self: f'<DLGRecords ({len(self)})>'
    __str__ = __repr__
    __bool__ = lambda self: True \
        if (self.first() is not None) \
        else False
    __hash__ = lambda self: hash(frozenset(self))

    def __len__(self):
        return len(self.records)

    def __nonzero__(self):
        return 1 \
            if (len(self.records) > 0) \
            else 0

    def __getslice__(self, x, y):
        return self.__class__(
            self.records[x: y],
            self.cols,
            objp4=self.objp4
        )

    def column(self, column=None):
        return Lst([rec[str(column) \
            if (column is not None) \
            else self.cols(0)] for rec in self])

    def first(self):
        return self(0)

    def last(self):
        return self(-1)

    def copy(self):
        return DLGRecords(self[:])

    def empty_records(self):
        return DLGRecords(Lst(), Lst(), self.objp4)

    def count(self):
        return len(self)

    def __init__(
            self,
            records=Lst(),
            cols=Lst(),
            objp4=None,
            **kwargs
    ):
        self.objp4 = objp4 or Storage()
        self.records = Lst(DLGRecord(record) for record in records)
        self.cols = cols
        self.oSearch = Search()
        self.grid = None

    def __call__(
            self,
            idx,
            default=None,
            cast=DLGRecord
    ):
        '''  Don't raise an  exception on IndexError, return default
        '''
        invalid_types = [
            'NoneType',
            'BooleanType'
        ]
        (
            record,
            cast
        ) \
            = self.get_record(
                            len(self),
                            idx,
                            default,
                            cast
        )
        if (record is not None):
            try:
                return cast(record) \
                    if (
                            (not type(cast).__name__ in invalid_types)
                            & (callable(cast) is True)
                ) \
                    else record
            except Exception as err:
                print(err)
        if (isinstance(record, dict) is True):
            record = DLGRecord(record)
        return record

    def get_record(
            self,
            length,
            idx,
            default,
            cast
    ):
        defaultrecord = lambda: 0
        ret = (default, False)
        if (self.idx_is_valid(idx, length) is True):
            ret = (self[idx], cast)
        elif (default is defaultrecord):
            ret = (default, cast)
        return ret

    def idx_is_valid(self, idx, length):
        try:
            if (
                    (
                            (idx < length)
                            & (length > 0)
                    )
                    | (
                            (-length < idx)
                            & (idx < 0)
                    )
            ):
                return True
        except Exception as err:
            print(err)
        return False

    grid = None
    def as_grid(self, *args, **kwargs):
        if (self.grid is None):
            self.grid = DataGrid(self)
        dgrid = self.grid(*args, **kwargs)
        print(dgrid)

    def __add__(self, other):
        if (isinstance(other, list) is True):
            other = DLGRecords(other)
        if (len(self.cols) == 0):
            try:
                self.cols = other.cols or other.first().getkeys()
            except Exception as err:
                msg = f"Failed to modify current record columns with '{other.cols}'\nError: {err}"
                bail(msg)
        records = self.records
        cols = self.cols
        othercols = other.cols
        if (cols == other.cols):
            records = (records + othercols)
            return DLGRecords(records, cols, self.objp4)
        else:
            bail(
                f'Cannot add records with different fields\n\
Our record fields: {cols}\nYour record fields: {othercols}'
            )

    def __and__(self, altrecords):
        if (self.cols == altrecords.cols):
            records = DLGRecords()
            alt_records = DLGRecords(altrecords.records)
            for record in self.records:
                if (record in alt_records):
                    records.append(record)
                    alt_records.remove(record)
            return DLGRecords(records, self.cols, self.objp4)

    def __or__(self, altrecords):
        if (self.cols == altrecords.cols):
            records = Lst([
                    record for record in altrecords.records if (not record in self.records)
            ] + self.records)
            return DLGRecords(records, self.cols, self.objp4)

    def __eq__(self, altrecords):
        return (self.records == altrecords.records) \
            if (isinstance(altrecords, DLGRecords)) \
            else False

    def __getitem__(self, key):
        record = self.records(key)
        if (isinstance(record, list)):
            return record
        if (type(record).__name__ == 'DLGRecord'):
            record = record.as_dict()
        if (isinstance(record, Storage)):
            keys = record.getkeys()
            if (len(keys) == 1):
                return record[record.getkeys().first()]
        return record

    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]

    def append(self, record):
        self.records.append(DLGRecord(record))

    def insert(self, record):
        try:
            self.records.append(DLGRecord(record))
        except Exception as err:
            bail(
                f'Failed to insert new record: {record}\nError: {err}'
            )

    def bulkinsert(self, *records):
        try:
            [
                self.records.append(DLGRecord(record) \
                    if (type(record) is not DLGRecord) \
                    else record) for record in records
            ]
        except Exception as err:
            bail(
                f"Failed to do a bulkinsert of new records:\nError: {err}"
            )

    def delete(self, func):
        '''  not yet implemented

            >>> for record in records.delete(lambda record: record.fieldname.startswith('M'))
        '''
        records = self
        if (len(records) == 0):
            return DLGRecords(Lst(), self.cols, self.objp4)

        i = 0
        while (i < len(self)):
            if (func(self[i]) is True):
                del self.records[i]
            else:
                i += 1

    def remove(self, func):
        self.delete(func)

    def update(self, func, **update_fields):
        records = self
        if (len(records) == 0):
            return DLGRecords(Lst(), self.cols, self.objp4)

        i = 0
        while (i < len(self)):
            if (func(self[i]) is True):
                self.records[i].update(**update_fields)
            else:
                i += 1

    def limitby(self, args, records=None):
        if (len(args) != 2):
            return self
        args = Lst(args)
        (
            start,
            end
        ) = \
            (
                (args(0) or 0,
                args(1) or len(self))
            )
        (
            outrecords,
            idx
        ) = \
            (
                Lst(),
                0
            )

        if (records is None):
            records = self
        for rec in records:
            if (start <= idx):
                outrecords.append(rec)
            idx += 1
            if (idx == end):
                break
        return DLGRecords(outrecords, self.cols, self.objp4)

    '''     find, exclude, sortby, orderby

            >>> for record in records.exclude(lambda rec: rec.type=='99'):
            >>>     ...

            >>> for record in records.sort(lambda rec: rec.mount):
                    print rec.client
                my_client
                other_client

            >>> records = oJnl(oJnl.rev).select()
            >>> Qry=(lambda rec: 'depot' in rec.depotFile)
            >>> records = records.find(Qry).sort(lambda rec: rec.depotFile)
            >>> for rec in records:
                    print rec.depotFile
                //depot/aFolder/aFile
                //depot/anotherFolder/anotherFile
    '''

    def find(
            self,
            func,
            limitby=None
    ):
        ''' returns a new set of P4Records / does not modify the original.
        '''

        records = self
        if (len(records) == 0):
            return DLGRecords(Lst(), self.cols, self.objp4)

        outrecords = Lst()
        (start, end) = limitby or (0, len(self))
        idx = 0
        for record in self:
            if func(record):
                if (start <= idx):
                    outrecords.append(record)
                idx += 1
                if (idx == end):
                    break
        return DLGRecords(outrecords, self.cols, self.objp4)

    def exclude(self, func):
        ''' returns a new set of DLGRecords / modifies the original.

                e.g.
                >>> for record in records.exclude(lambda record: record.fieldname.startswith('M'))
        '''
        if (len(self) == 0):
            return self
        excluded = Lst()
        i = 0
        while (i < len(self)):
            record = self[i]
            if func(record):
                excluded.append(record)
                del self.records[i]
            else:
                i += 1
        return DLGRecords(excluded, self.cols, self.objp4)

    def sortby(
            self,
            field,
            limitby=None,
            reverse=False,
            records=None
    ):
        ''' returns a new set of DLGRecords sorted by a condition / does not modify the original.

            >>> out = records.sortby(oJnl.domain.accessDate)
            >>> out.as_grid()
            +-----+-----------+----------------+------------+-----------------+------+--------------+-----------------------------+--------+--------+-------+------------+------------+---------+------------------+--------+----------+-----------+
            | idx | db_action | table_revision | table_name | name            | type | extra        | mount                       | mount2 | mount3 | owner | updateDate | accessDate | options | description      | stream | serverid | partition |
            +-----+-----------+----------------+------------+-----------------+------+--------------+-----------------------------+--------+--------+-------+------------+------------+---------+------------------+--------+----------+-----------+
            | 1   | pv        | 6              | db.domain  | anyschema       | 99   |              | /Users/mart/anyschema_2db   |        |        | mart  | 2021/03/12 | 2021/03/12 | 4096    | Created by mart. |        |          | 0         |
            | 6   | pv        | 6              | db.domain  | p4client        | 99   | gareth.local | /Users/mart/p4src           |        |        | mart  | 2021/03/12 | 2021/03/13 | 4096    | Created by mart. |        |          | 0         |
            | 2   | pv        | 6              | db.domain  | client.protodev | 99   |              | /Users/mart/protodev        |        |        | mart  | 2021/04/14 | 2021/04/14 | 0       | Created by mart. |        |          | 0         |
            | 4   | pv        | 6              | db.domain  | localclient     | 99   | raspberrypi  | /home/pi                    |        |        | mart  | 2021/03/14 | 2021/04/14 | 0       | Created by mart. |        |          | 0         |
            | 5   | pv        | 6              | db.domain  | martclient      | 99   | gareth.local | /Users/mart                 |        |        | mart  | 2021/03/12 | 2021/07/20 | 4096    | Created by mart. |        |          | 0         |
            | 7   | pv        | 6              | db.domain  | pycharmclient   | 99   | gareth.local | /Users/mart/PycharmProjects |        |        | mart  | 2021/06/17 | 2021/08/05 | 2       | Created by mart. |        |          | 0         |
            +-----+-----------+----------------+------------+-----------------+------+--------------+-----------------------------+--------+--------+-------+------------+------------+---------+------------------+--------+----------+-----------+
        '''
        if (records is None):
            records = self
        if (len(records) == 0):
            return records

        if (limitby is not None):
            if (isinstance(limitby, tuple) is True):
                records = records.limitby(limitby)
            else:
                bail('limitby must be of type `tuple`')

        fieldname = field.fieldname \
            if (type(field).__name__ in ('JNLField', 'Py4Field')) \
            else field
        outrecords = Lst(
            sorted(
                records,
                key=lambda k: k[fieldname],
                reverse=reverse
            )
        )
        return DLGRecords(outrecords, self.cols, self.objp4)

    def orderby(
            self,
            *fields,
            limitby=None,
            reverse=False,
            records=None
    ):
        ''' returns a new set of P4Records / does not modify the original.
            >>> out = records.orderby(*[oJnl.domain.name, oJnl.domain.accessDate],)
            >>> out.as_grid()


        '''

        records = self \
            if (records is None) \
            else DLGRecords(records, self.cols, self.objp4) \
            if (type(records).__name__ != 'DLGRecords') \
            else records

        if (len(records) == 0):
            return records

        for field in fields:
            if (not field in self.cols):
                bail(
                    f"Invalid column {field}. Try again."
                )
        if (limitby is not None):
            if (isinstance(limitby, tuple) is True):
                records = records.limitby(limitby)
            else:
                bail('limitby must be of type `tuple`')

        for field in fields:
            fieldname = field.fieldname \
                if (type(field).__name__ in ('JNLField', 'Py4Field')) \
                else field
            records = sorted(
                records,
                key=lambda k: k[fieldname],
                reverse=reverse
            )
        return DLGRecords(records, self.cols, self.objp4)

    def groupby(
            self,
            *fields,
            limitby=None,
            orderby=None,
            reverse=False,
            groupdict=False,
            records=None
    ):
        '''     groupby

                USAGE:
                        >>> journal = os.path.abspath('./journals/journal2')
                        >>> oSchema = schema('r15.2')
                        >>> oJnl = P4Jnl(journal, oSchema)
                        >>> qry = (oJnl.domain.type=='99')
                        >>> records = oJnl(qry).select("id","type","name","mount","owner","options")
                        >>> records
                        <DLGRecords (500)>
                        >>> grecs = records.groupby('name',orderby='idx')
                        # list of last record of each group name
                        >>> grecs_list = {grp_name: grecs[grp_name].last() for grp_name in grecs}
                        <<< grec_list
                         {'charlotte': {'id': 51,
                                        'type': '99',
                                        'name': 'charlotte',
                                        'mount': '/Users/mart/Downloads/libotr-4.1.0',
                                        'owner': 'mart',
                                        'options': '0'},
                         'charlotte-2': {'id': 148052,
                                         'type': '99',
                                         'name': 'charlotte-2',
                                         'mount': '/Users/mart',
                                         'owner': 'mart',
                                         'options': '0'},
                         ...}
        '''
        for field in fields:
            if (not field in self.cols):
                bail(
                    f"Invalid column name `{field}`. Try again."
                )

        if (records is None):
            records = self
        if (len(records) == 0):
            '''  we have no records! return an empty set
            '''
            return records

        records_by_group = Storage()
        recordslen = len(records)

        def group(record, num):
            if (num  <= (len(fields) - 1)):
                try:
                    rec = group(record, (num + 1)) or Lst([record])
                    name = str(record[fields[num].fieldname]) \
                        if (type(fields[num]).__name__ in ('JNLField', 'Py4Field')) \
                        else str(record[fields[num]])
                    if (isinstance(rec, dict) is True):
                        records_by_group[name].append(rec)
                    elif (isinstance(rec, (list, Lst)) is True):
                        records_by_group[name] += rec
                    else:
                        records_by_group[name] = rec
                except Exception as err:
                    records_by_group[name] = rec

        '''  start with limitby 
        '''
        if (limitby is not None):
            records = self.limitby(limitby)
            recordslen = (len(records) - 1)

        ''' group records by processing one at a time
        '''
        for i in range(0, recordslen):
            rec = records(i)
            group(rec, 0)
            recordslen -= 1

        ''' orderby & reverse, as instructed
        '''
        groupnames = records_by_group.getkeys()
        for groupname in groupnames:
            records_by_group[groupname] = DLGRecords(records_by_group[groupname])
            if (orderby is not None):
                recordgroup = self.orderby(orderby, reverse=reverse, records=records_by_group[groupname])
                records_by_group.update(
                    **{
                        groupname: DLGRecords(
                            recordgroup,
                            self.cols,
                            self.objp4
                        )
                    }
                )

        ''' return grouped records as a dict , eg.
                {
                    group_name: <DLGRecords(
                                        <{DLGRecord}>
                                        <{DLGRecord}>,
                                        <{DLGRecord}>,
                                        <{DLGRecord}>
                                        )>,
                   ,group_name2: <DLGRecords(
                                        <{DLGRecord}>,
                                        <{DLGRecord}>
                                        )>
                }
            
            or return as a single list of grouped records
                <DLGRecords(
                        <{DLGRecord}>
                        <{DLGRecord}>,
                        <{DLGRecord}>,
                        <{DLGRecord}>,
                        <{DLGRecord}>,
                        <{DLGRecord}>
                        )>
        '''
        if (groupdict is True):
            return records_by_group
        outrecords = Lst()
        for rgroup in records_by_group.getkeys():
            outrecords += records_by_group[rgroup]
        return DLGRecords(outrecords, self.cols, self.objp4)
