import os
import re
from types import LambdaType
from libdlg.dlgQuery_and_operators import *
from libdlg.dlgRecord import DLGRecord
from libdlg.dlgStore import (
    Lst,
    Storage,
    objectify
)
from libdlg.dlgSearch import Search
from libdlg.dlgUtilities import (
    bail,
    xrange,
    noneempty
)
from libdlg.dlgTables import *
# from libdlg.p4qLogger import LogHandler
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

    def count(self, distinct=None):
        return len(self)

    def __init__(
            self,
            records=Lst(),
            cols=Lst(),
            objp4=None,
            **tabledata
    ):
        self.objp4 = objp4 or Storage()
        self.records = Lst(DLGRecord(record) for record in records)
        self.cols = cols
        self.oSearch = Search()
        self.grid = None
        ''' thinking specifically for Py4 & Search
        '''
        self.tabledata = Storage(tabledata)

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
            if (not type(cast).__name__ in invalid_types):
                if (callable(cast) is True):
                    record = cast(record)
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
            if AND(
                    AND(
                            (idx < length),
                            (length > 0)
                    )
                    | AND(
                            (-length < idx),
                            (idx < 0)
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
            return DLGRecords(Lst(records), self.cols, self.objp4)

    def __or__(self, altrecords):
        if (self.cols != altrecords.cols):
            raise Exception("columns of one record are not compatible with fields of the other")
        records = Lst([
                record for record in altrecords.records if (not record in self.records)
        ] + self.records)
        return self.__class__(records, self.cols, self.objp4)


    def __eq__(self, altrecords):
        return (self.records == altrecords.records) \
            if (type(altrecords).__name__ == 'DLGRecords') \
            else False

    def __getitem__(self, key):
        return self.records[key]

    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]

    def append(self, record):
        self.records.append(DLGRecord(record))

    def insert(self, idx, record):
        self.records.insert(idx, DLGRecord(record))

    def bulkinsert(self, *records):
        [
            self.records.insert(idx, DLGRecord(record) \
                if (type(record) is not DLGRecord) \
                else record) for (idx, record) in records
        ]

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

    def update_old(self, func, **update_fields):
        records = self
        if (len(records) == 0):
            return DLGRecords(Lst(), self.cols, self.objp4)

        i = 0
        while (i < len(self)):
            if (func(self[i]) is True):
                self.records[i].update(**update_fields)
            else:
                i += 1

    def update(self, **update_fields):
        records = self
        if (len(records) == 0):
            return DLGRecords(Lst(), self.cols, self.objp4)
        i = 0
        while (i < len(records)):
            #if (func(self[i]) is True):
            self.records[i].update(**update_fields)
            #else:
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
            +-----+-----------+----------------+------------+-----------------+------+--------------+---------------------------+--------+--------+-------+------------+------------+---------+----------------+--------+----------+-----------+
            | idx | db_action | table_revision | table_name | name            | type | extra        | mount                     | mount2 | mount3 | owner | updateDate | accessDate | options | description    | stream | serverid | partition |
            +-----+-----------+----------------+------------+-----------------+------+--------------+---------------------------+--------+--------+-------+------------+------------+---------+----------------+--------+----------+-----------+
            | 1   | pv        | 6              | db.domain  | anyschema       | 99   |              | /Users/gc/anyschema_2db   |        |        | gc    | 2021/03/12 | 2021/03/12 | 4096    | Created by gc. |        |          | 0         |
            | 6   | pv        | 6              | db.domain  | p4client        | 99   | gareth.local | /Users/gc/p4src           |        |        | gc    | 2021/03/12 | 2021/03/13 | 4096    | Created by gc. |        |          | 0         |
            | 2   | pv        | 6              | db.domain  | client.protodev | 99   |              | /Users/gc/protodev        |        |        | gc    | 2021/04/14 | 2021/04/14 | 0       | Created by gc. |        |          | 0         |
            | 4   | pv        | 6              | db.domain  | localclient     | 99   | raspberrypi  | /home/pi                  |        |        | gc    | 2021/03/14 | 2021/04/14 | 0       | Created by gc. |        |          | 0         |
            | 5   | pv        | 6              | db.domain  | bigbirdclient   | 99   | gareth.local | /Users/gc                 |        |        | gc    | 2021/03/12 | 2021/07/20 | 4096    | Created by gc. |        |          | 0         |
            | 7   | pv        | 6              | db.domain  | pycharmclient   | 99   | gareth.local | /Users/gc/PycharmProjects |        |        | gc    | 2021/06/17 | 2021/08/05 | 2       | Created by gc. |        |          | 0         |
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
            recordfields = records.records(0).getkeys()
            if fieldname not in recordfields:
                raise FieldNotInRecord(fieldname, recordfields)
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
            having=None,
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
                                        'mount': '/Users/gc/Downloads/libotr-4.1.0',
                                        'owner': 'gc',
                                        'options': '0'},
                         'charlotte-2': {'id': 148052,
                                         'type': '99',
                                         'name': 'charlotte-2',
                                         'mount': '/Users/gc',
                                         'owner': 'gc',
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

    ''' extending P4QRecordSets/filters for p4 specific records (actions on files, changes/revs, spec/specs, ...

                    p4 actions on files as P4QRecordSets attributes.

                        so, anything that takes a filename

                        add
                        annotate
                        clean
                        copy
                        describe
                        diff / diff2
                        edit
                        delete
                        grep
                        integ / integrate
                        lock
                        merge
                        move / rename
                        print
                        revert
                        submit
                        sync / flush
                        shelve
                        unshelve

                    perhaps attributes to compliment actions on changes/revs

                    as well, actions on specs & particularly on spec IO

                '''

    def sync(
            self,
            *options,
            limitby=None,
            action=None,
            include_deleted_files=False,
            #
            # cmd line args
            #
            preview=False,
            uselist=True,
            force=False,
            keepclient=False,
            max=None,
            publish=False,
            safe=False,
            estimates=False,
            **kwargs
    ):
        (options, kwargs) = (Lst(options), Storage(kwargs))
        records = self.select(close=False)
        (start, end) = limitby \
            if (noneempty(limitby) is False) \
            else (0, len(records))
        records = records.limitby((start, end))

        if (len(records) == 0):
            self.loginfo('searched 0 records')
            return self

        (
            syncrecords,
            syncfiles,
            cols,
            idx,
            cols,
            exclude_actions
        ) = \
            (
                Lst(),
                list(),
                self.cols,
                0,
                Lst(),
                Lst()
            )

        if (include_deleted_files is False):
            exclude_actions.append('delete')

        try:
            for record in records:
                if AND(
                        (action is not None),
                        (record.action != action)
                ):
                    exclude_actions.append(record.action)

                recaction = Lst(re.split('/', record.action))(1)
                if (not recaction in exclude_actions):
                    specifier = None
                    if (record.code is not None):
                        record.delete('code')

                    syncFile = record.depotFile or record.clientFile or record.path

                    if (syncFile is not None):
                        query = list(self.query) \
                            if not (isinstance(self.query, list)) \
                            else self.query
                        for q in query:
                            if (isinstance(q.right, Storage)):
                                q = q.left
                            if (q.right == syncFile):
                                specifier = q.left.specifier
                                break
                        if (noneempty(specifier) is True):
                            for item in (
                                    ('rev', '#'),
                                    ('change', '@')
                            ):
                                (
                                    specitem,
                                    specchar
                                ) = \
                                    (
                                        item[0],
                                        item[1]
                                    )
                                if (record[specitem] is not None):
                                    syncFile = f'{syncFile}{specchar}{record[specitem]}'
                                    break
                        else:
                            syncFile = f'{syncFile}{specifier}{record.rev}'
                        syncfiles.append(syncFile)

            for (key, value) in Storage(kwargs.copy()).items():
                if (key.lower() in self.objp4.tablememo['sync'].cmdref.fieldsmap.getkeys()):
                    optkey = f'--{key}'
                    if (noneempty(value) is False):
                        if (isinstance(value, bool)):
                            if (value == True):
                                options.append(key)
                            kwargs.pop(key)
                        else:
                            (
                                options.append(optkey),
                                options.append(value)
                            )
                    options.append(optkey)

            for optionitem in (
                    Lst(force, '-f', '--force'),
                    Lst(preview, '-n', '--preview'),
                    Lst(estimates, '-N', '--estimates'),
                    Lst(safe, '-s', '--safe'),
                    Lst(publish, '-p', '--publish'),
                    Lst(max, '-m', '--max'),
                    Lst(keepclient, '-k', '--keep-client')
            ):
                if AND(
                        (
                                optionitem(0) is True
                        ),
                        (
                                len(Lst(optionitem(1), optionitem(2)).intersect(options)) == 0
                        )
                ):
                    options.append(optionitem(1))
            if (uselist is True):
                if (len(Lst('-l', '--use-list').intersect(options)) == 0):
                    options.append('--use-list')
                    options = options + syncfiles
                else:
                    options.append(' '.join(syncfiles))
            options.insert(0, 'sync')
            cmdargs = self.objp4.p4globals + options
            out = Lst(self.objp4.p4Output('sync', *cmdargs))
            syncrecords.append(out)
            return DLGRecords(syncrecords, cols, self.objp4)
        except Exception as err:
            bail(err)
        finally:
            self.close()

    def search(self, *term, limitby=None):
        ''' [(rec.name, col) for rec in oSchema.p4schema.recordtypes.record for col in rec.column \
        if ((col.name in ('depotFile', 'clientFile')) & (col.type == 'File'))]

            USAGE:
            >>> searchrecords = DLGRecords([], cols=self.cols, objp4=self.objp4)

            >>> filename = '//dev/p4dlg/p4/libpy4/py4IO.py'
            >>> printed = oP4.print(filename)
            >>> content = ''.join([p.data for p in printed[1:] if (p.data is not None)])
            >>> results = printed.search(content, "administrationlist")
            >>> records = [results.sortby('score')]
            >>>
        '''
        records = self.select(close=False)

        (
            start,
            end
        ) = \
            limitby \
                if (noneempty(limitby) is False) \
                else (0, len(records))

        records = records.limitby(
            (
                start,
                end
            )
        )

        searchrecords = Lst()
        term = Lst([term]) \
            if (isinstance(term, str)) \
            else Lst(term)

        if (len(records) == 0):
            self.loginfo('searched 0 records')
            return self

        (
            sources,
            cols,
            idx,
            metadata
        ) = \
            (
                Lst(),
                self.cols,
                0,
                None
            )

        try:
            for record in records:
                try:
                    ''' `cols` column header an be problematic. Though it
                        should have been taken care of by now, try to remove it 
                        anyways.
                    '''
                    if (record.code is not None):
                        code = record.pop('code')
                        if AND(
                                (code == 'text'),
                                (record.data is not None)
                        ):
                            bail(
                                "Cannot search against queries on table `print`. \
                            Try queries on tables with fieds \
                            such as `depotFile`, or 'Description', etc."
                            )

                    ''' is there a 'depotFile' field in record.fieldnames ?
                    '''
                    sourceFile = record.depotFile or record.clientFile or record.path
                    if (sourceFile is not None):
                        query = list(self.query) \
                            if not (isinstance(self.query, list)) \
                            else self.query
                        for q in query:
                            if (isinstance(q.right, Storage)):
                                q = q.left
                            if (q.right == sourceFile):
                                specifier = q.left.specifier
                                if (noneempty(specifier) is False):
                                    append_specifier = ''.join([specifier, record.rev])
                                    sourceFile = ''.join([sourceFile, append_specifier])
                                    break
                                '''
                                specifier = q.left.specifier
                                specifier_value = q.left.specifier_value
                                if (noneempty(specifier) is False):
                                    append_specifier = ''.join([specifier, specifier_value])
                                    sourceFile = ''.join([sourceFile, append_specifier])
                                    break
                                '''
                        args = ['print', sourceFile]
                        cmdargs = self.objp4.p4globals + args
                        out = Lst(self.objp4.p4Output('print', *cmdargs))
                        sources.append(out)
                    elif record.getkeys().intersect(['desc', 'Description']):
                        sources.append(record)
                except Exception as err:
                    bail(err)

            for item in sources:
                (
                    source,
                    metadata
                ) = \
                    (
                        None,
                        Storage()
                    )
                if (isinstance(item, dict)):
                    item = Storage(item)
                    if (item.Description is not None):
                        source = item.Description
                    elif (item.desc is not None):
                        source = item.desc
                    metadata = item
                elif (isinstance(item, Lst)):
                    metadata = Storage(item(0))
                    if (len(out) == 2):
                        source = out(1).data
                    else:
                        source = ''
                        for idx in range(1, len(out)):
                            source += out(idx).data
                results = self.oSearch(source, *term)
                for result in results:
                    context = re.sub('^\s*', '... ', result.context)
                    searchdata = Storage(
                        {
                            'score': result.score,
                            'search_terms': result.terms,
                            'linenumber': result.id,
                            'context': context
                        }
                    )
                    if (start <= idx):
                        if (metadata is not None):
                            searchdata.merge(metadata)
                            searchrecords.append(searchdata)
                    if (len(cols) == 0):
                        cols = searchdata.getkeys()
                    idx += 1
                    if (idx == end):
                        break
            return DLGRecords(searchrecords, cols, self.objp4)
        except Exception as err:
            bail(err)
        finally:
            self.close()