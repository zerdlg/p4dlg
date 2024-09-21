import os
from types import LambdaType
from libdlg.dlgRecord import P4QRecord
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

__all__ = ['P4QRecords']

# TODO: add compounds like: split, apply (agregate), combine (merge)
class P4QRecords(object):
    __repr__ = lambda self: f'<P4QRecords ({len(self)})>'
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
        return P4QRecords(self[:])

    def empty_records(self):
        return P4QRecords(Lst(), Lst(), self.objp4)

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
        self.records = Lst(P4QRecord(record) for record in records)
        self.cols = cols
        self.oSearch = Search()

    def __call__(
            self,
            idx,
            default=None,
            cast=P4QRecord
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
            record = P4QRecord(record)
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

    def datagrid(self):
        return DataGrid(self)()

    def printer(self, *args, **kwargs):
        print(DataGrid(self)(*args, **kwargs))

    def __add__(self, other):
        if (isinstance(other, list) is True):
            other = P4QRecords(other)
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
            return P4QRecords(records, cols, self.objp4)
        else:
            bail(
                f'Cannot add records with different fields\n\
Our record fields: {cols}\nYour record fields: {othercols}'
            )

    def __and__(self, altrecords):
        if (self.cols == altrecords.cols):
            records = P4QRecords()
            alt_records = P4QRecords(altrecords.records)
            for record in self.records:
                if (record in alt_records):
                    records.append(record)
                    alt_records.remove(record)
            return P4QRecords(records, self.cols, self.objp4)

    def __or__(self, altrecords):
        if (self.cols == altrecords.cols):
            records = Lst([
                    record for record in altrecords.records if (not record in self.records)
            ] + self.records)
            return P4QRecords(records, self.cols, self.objp4)

    def __eq__(self, altrecords):
        return (self.records == altrecords.records) \
            if (isinstance(altrecords, P4QRecords)) \
            else False

    def __getitem__(self, key):
        record = self.records(key)
        if (isinstance(record, list)):
            return record
        if (type(record).__name__ == 'P4QRecord'):
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
        self.records.append(P4QRecord(record))

    def insert(self, record):
        try:
            self.records.append(P4QRecord(record))
        except Exception as err:
            bail(
                f'Failed to insert new record: {record}\nError: {err}'
            )

    def bulkinsert(self, *records):
        try:
            [
                self.records.append(P4QRecord(record) \
                    if (type(record) is not P4QRecord) \
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
            return P4QRecords(Lst(), self.cols, self.objp4)

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
            return P4QRecords(Lst(), self.cols, self.objp4)

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
        return P4QRecords(outrecords, self.cols, self.objp4)

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
            return P4QRecords(Lst(), self.cols, self.objp4)

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
        return P4QRecords(outrecords, self.cols, self.objp4)

    def exclude(self, func):
        ''' returns a new set of P4QRecords / modifies the original.

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
        return P4QRecords(excluded, self.cols, self.objp4)

    def sortby(
            self,
            key,
            reverse=False,
            records=None
    ):
        ''' returns a new set of P4QRecords / does not modify the original.
        '''

        if (records is None):
            records = self

        if (len(records) == 0):
            return records
        outrecords = Lst()

        if (type(key) is LambdaType):
            outrecords = Lst(
                [
                    rec for (rec, s) in sorted(
                        zip(records, self),
                        key=lambda rec: key(rec[1]),
                        reverse=reverse
                    )
                ]
            )
        if (isinstance(key, str)):
            outrecords = Lst(
                sorted(
                    records,
                    key=lambda k: k[key],
                    reverse=reverse
                )
            )
        return P4QRecords(outrecords, self.cols, self.objp4)

    def orderby(
            self,
            *fields,
            limitby=None,
            reverse=False,
            records=None
    ):
        ''' returns a new set of P4Records / does not modify the original.
        '''
        if (records is None):
            records = self

        if (len(records) == 0):
            return records

        for field in fields:
            if (not field in self.cols):
                bail(
                    f"Invalid column {field}. Try again."
                )
        fields = Lst(fields)
        if (limitby is not None):
            records = records.limitby(limitby)
        if (len(fields) > 0):
            for field in fields:
                records = sorted(
                    records,
                    key=lambda k: k[field],
                    reverse=reverse
                )
        return records

    def groupby(
            self,
            *fields,
            limitby=None,
            orderby=None,
            groupdict=False,    # affects return value
            records=None,
            **kwargs
    ):
        '''     groupby

                USAGE:
                        >>> journal = os.path.abspath('./journals/journal2')
                        >>> oSchema = schema('r15.2')
                        >>> oJnl = P4Jnl(journal, oSchema)
                        >>> qry = (oJnl.domain.type=='99')
                        >>> records = oJnl(qry).select("id","type","name","mount","owner","options")
                        >>> records
                        <P4QRecords (500)>
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
        (fields, kwargs) = (Lst(fields), Storage(kwargs))
        for field in fields:
            if (not field in self.cols):
                bail(
                    f"Invalid column name `{field}`. Try again."
                )

        if (records is None):
            records = self

        if (len(records) == 0):
            return records

        def makegroup(record, num, groups):
            if (num > (len(fields) - 1)):
                return Lst([record])
            fieldname = fields[num]
            name = str(record[fieldname])
            if (len(name) == 0):
                return groups
            if (isinstance(groups, Storage) is True):
                if (name not in groups.getkeys()):
                    groups[name] = makegroup(
                        record,
                        (num + 1),
                        Storage()
                    )
                else:
                    rec = makegroup(
                        record,
                        (num + 1),
                        groups[name]
                    )

                    if (isinstance(rec, dict) is True):
                        groups[name].append(rec)
                    elif (isinstance(rec, list) is True):
                        groups[name] += rec
                    else:
                        groups[name] = rec
            elif (isinstance(groups, Lst) is True):
                num += 1
                makegroup(
                    record,
                    num,
                    Storage()
                )
            return groups

        outrecords = Lst()
        records_by_group = Storage()
        recordslen = (len(records) - 1)

        if (len(records) == 0):
            '''  we have no records! return an empty set
            '''
            return records

        '''  start with limitby 
        '''
        if (limitby is not None):
            records = self.limitby(*limitby)
            recordslen = (len(records) - 1)

        for i in range(0, recordslen):
            rec = records(i)
            makegroup(
                            rec,
                            0,
                            records_by_group
            )
            recordslen -= 1

        recfields = records_by_group.getkeys()
        for recfield in recfields:
            recordgroup = records_by_group[recfield]
            if (orderby is not None):
                recordgroup = self.orderby(orderby, records=recordgroup)
                records_by_group.update(
                    **{
                        recfield: P4QRecords(
                            recordgroup,
                            self.cols,
                            self.objp4
                        )
                    }
                )
        if (groupdict is False):
            for rgroup in records_by_group.getkeys():
                outrecords += records_by_group[rgroup].records
            return P4QRecords(outrecords, self.cols, self.objp4)
        '''     return grouped records

                {group_name: < Records([<{record}>
                                        <{record}>,
                                        <{record}>,
                                        <{record}>]>),
                ,group_name2: <Records([<{record}>,
                                        <{record}>]})
        '''
        return records_by_group

    def join(self, field, name=None, constraint=None, fields=[], orderby=None):
        if (len(self) == 0):
            return self

        mode = 'referencing' \
            if (field.type == 'id') \
            else 'referenced'

        func = lambda ids: field.belongs(ids)

        (objp4, ids, maps) = (self.objp4, [], {})

        if (noneempty(fields) is True):
            fields = Lst(f for f in field._table if f.readable)

        if (mode == 'referencing'):
            # try all referenced field names
            names = [name] \
                if (name is not None) \
                else list(
                            set(
                f.name for f in field._table._referenced_by if (f.name in self[0])
                            )
            )

            # get all the ids
            ids = [row.get(name) for row in self for name in names]

            # filter out the invalid ids
            ids = filter(lambda id: str(id).isdigit(), ids)

            # build the query
            query = func(ids)

            if constraint:
                query = query & constraint

            tmp = not field.name in [f.name for f in fields]
            if tmp:
                fields.append(field)

            other = objp4(query).select(*fields, orderby=orderby)#, cacheable=True)

            for row in other:
                id = row[field.name]
                maps[id] = row

            for row in self:
                for name in names:
                    row[name] = maps.get(row[name])

        if mode == 'referenced':
            if (name is None):
                name = field._tablename

            # build the query
            query = func([row.id for row in self])

            if constraint: query = query & constraint

            name = name or field._tablename

            tmp = not field.name in [f.name for f in fields]
            if tmp:
                fields.append(field)

            other = objp4(query).select(*fields, orderby=orderby)#, cacheable=True)

            for row in other:
                id = row[field]
                if not id in maps: maps[id] = []
                if tmp:
                    try:
                        del row[field.name]
                    except:
                        del row[field.tablename][field.name]

                        if not row[field.tablename] and len(row.keys())==2:
                            del row[field.tablename]
                            row = row[row.keys()[0]]

                maps[id].append(row)
            for row in self:
                row[name] = maps.get(row.id, [])
        return self