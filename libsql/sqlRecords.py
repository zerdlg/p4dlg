import os
import re
from libsql.sqlRecord import (
    Record)
from libdlg.dlgStore import (
    Lst,
    ZDict
)
from libdlg.dlgSearch import Search
from libdlg.dlgUtilities import (
    bail,
    xrange,
    noneempty
)
from libsql.sqlValidate import *
from libdlg.dlgTables import *
# from libdlg.p4qLogger import LogHandler
from libdlg.dlgError import *

'''  [$File: //dev/p4dlg/libsql/sqlRecords.py $] [$Change: 678 $] [$Revision: #15 $]
     [$DateTime: 2025/04/01 04:47:46 $]
     [$Author: zerdlg $]
'''

__all__ = ['Records']

# TODO: add compounds like: split, apply (agregate), combine (merge)
class Records(object):
    __repr__ = lambda self: f'<Records ({len(self)})>'
    __str__ = __repr__
    __bool__ = lambda self: True \
        if (self.first() is not None) \
        else False
    __hash__ = lambda self: hash(frozenset(self.records))

    def __getitem__(self, key):
        return self.records[key]

    def __iter__(self):
        for i in xrange(len(self.records)):
            yield self[i]

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
        return Records(self[:])

    def empty_records(self):
        return Records(Lst(), Lst(), self.objp4)

    def count_(self, distinct=False, groupname=None):
        if (groupname is None):
            if (distinct is True):
                distinctvalues = set()
                #fieldname = self.tabledata.tablename
                fieldname = self.tabledata.fieldname
                for rec in self.records:
                    distinctvalue = rec(fieldname)
                    if (distinctvalue is not None):
                        distinctvalues.add(distinctvalue)
                return len(distinctvalues)
            return len(self)
        return self.counts(groupname)

    def avg_(self, exp):
        ''

    def min_(self, exp):
        ''

    def max_(self, exp):
        ''

    def sum_(self, exp):
        ''

    def __init__(
            self,
            records=Lst(),
            cols=Lst(),
            objp4=None,
            **tabledata
    ):

        (
            self.cols,
            self.grid,
            self.counts
        ) = \
            (
                cols,
                None,
                ZDict()
            )

        self.objp4 = objp4 or ZDict()
        self.records = Lst(Record(record) for record in records) \
            if (records is not None) \
            else Lst()
        ''' thinking specifically for Py4 & Search
        '''
        self.tabledata = ZDict(tabledata)
        ''' p4 cmd specific tables are of no use if objp4 is P4Jnl - 
            let's get rid of them so as to not bring about confusion.  
        '''
        py4_attributes = (
            'sync',
            'edit',
            'add',
            'integ'
        )
        if (is_P4Jnl(self.objp4) is True):
            for item in py4_attributes:
                try:
                    delattr(self, item)
                except: pass
        #self.as_groups = False

    def __call__(
            self,
            idx,
            default=None,
            cast=Record
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
            record = Record(record)
        return record

    def get_record(
            self,
            length,
            idx,
            default,
            cast
    ):
        defaultrecord = lambda: 0
        return (self[idx], cast) \
            if (self.idx_is_valid(idx, length) is True) \
            else (default, cast) \
            if (default is defaultrecord) \
            else (default, False)

    def idx_is_valid(self, idx, length):
        try:
            if (
                    (
                            (idx < length) &
                            (length > 0)
                    ) |
                    (
                            (-length < idx) &
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
            other = Records(other)
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
            return Records(records, cols, self.objp4)
        else:
            bail(
                f'Cannot add records with different fields\n\
Our record fields: {cols}\nYour record fields: {othercols}'
            )

    def __and__(self, altrecords):
        if (self.cols == altrecords.cols):
            records = Records()
            alt_records = Records(altrecords.records)
            for record in self.records:
                if (record in alt_records):
                    records.append(record)
                    alt_records.remove(record)
            return Records(Lst(records), self.cols, self.objp4)

    def __or__(self, altrecords):
        if (self.cols != altrecords.cols):
            raise Exception("columns of one record are not compatible with fields of the other")
        records = Lst([
                record for record in altrecords.records if (not record in self.records)
        ] + self.records)
        return self.__class__(records, self.cols, self.objp4)

    def __eq__(self, altrecords):
        return (self.records == altrecords.records) \
            if (type(altrecords).__name__ == 'Records') \
            else False

    def append(self, record):
        self.records.append(Record(record))

    def insert(self, idx, record):
        self.records.insert(idx, Record(record))

    def bulkinsert(self, *records):
        [
            self.records.insert(idx, Record(record) \
                if (type(record) is not Record) \
                else record) for (idx, record) in records
        ]

    def delete(self, func):
        '''  not yet implemented

            >>> for record in records.delete(lambda record: record.fieldname.startswith('M'))
        '''
        records = self
        if (len(records) == 0):
            return Records(Lst(), self.cols, self.objp4)
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
            return Records(Lst(), self.cols, self.objp4)

        i = 0
        while (i < len(self)):
            if (func(self[i]) is True):
                self.records[i].update(**update_fields)
            else:
                i += 1

    def update(self, **update_fields):
        records = self
        if (len(records) == 0):
            return Records(Lst(), self.cols, self.objp4)
        i = 0
        while (i < len(records)):
            self.records[i].update(**update_fields)
            i += 1

    def limitby(self, args, records=None):
        ''' would a between() func be useful?
            res = filter(lambda rec: min <= rec.idx <= max, changes)

            and, should start/end be used as rec.id/idx?
            hum...
        '''
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
        return Records(outrecords, self.cols, self.objp4)

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
            return Records(Lst(), self.cols, self.objp4)

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
        return Records(outrecords, self.cols, self.objp4)

    def exclude(self, func):
        ''' returns a new set of Records / modifies the original.

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
        return Records(excluded, self.cols, self.objp4)

    def sortby(
            self,
            field,
            limitby=None,
            reverse=False,
            records=None
    ):
        ''' returns a new set of Records sorted by a condition / does not modify the original.

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
            if (is_fieldType(field) is True) \
            else field
        outrecords = Lst(
            sorted(
                records,
                key=lambda k: k[fieldname],
                reverse=reverse
            )
        )
        return Records(outrecords, self.cols, self.objp4)

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
            else Records(records, self.cols, self.objp4) \
            if (is_recordsType(records) is False) \
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
                if (is_fieldType(field) is True) \
                else field
            recordfields = records.records(0).getkeys()
            if fieldname not in recordfields:
                raise FieldNotInRecord(fieldname, recordfields)
            records = sorted(
                records,
                key=lambda k: k[fieldname],
                reverse=reverse
            )

        return Records(records, self.cols, self.objp4)

    def groupby(
            self,
            *fields,
            limitby=None,
            orderby=None,
            sortby=None,
            reverse=False,
            as_groups=False,
            records=None
    ):
        '''     groupby

        USAMGE EXAMPLE:

        groupby with joined (inner) records

        >>> qry = (p4.files.change == p4.changes.change)

        params: flat = True, as_groups = True

        ** An inner_join with a groupby aggregator must have flat=True!
            - with that, as_groups can be either True or False.

            >>> recs = p4(qry).select(groupby=p4.changes.change, flat=True, as_groups=True)
            >>> recs
            {'609': <Records (102)>,
             '506': <Records (1)>,
             '492': <Records (3)>,
             '516': <Records (1)>,
             ...
             }

            >>> for change in recs:
                ... print(f"{change} - {recs[change].count()} records")
            change 609 - 102 file records
            change 506 - 1 file records
            change 492 - 3 file records
            change 516 - 1 file records
            ...

            >>> sum([grp.count() for grp in recs.getvalues()])
            624

        params: flat = True, as_groups = False

            >>> recs = p4(qry).select(groupby=p4.changes.change, flat=True, as_groups=False)
            >>> recs
            <Records (624)>
            >>> recs.count()
            624
            >>> recs(0)
            <Record {'action': 'edit',
             'change': '609',
             'changeType': 'public',
             'client': 'computer_p4dlg',
             'depotFile': '//dev/p4dlg/__init__.py',
             'desc': 'Retyping new module to ktext.\n',
             'idx': 1,
             'path': '//dev/p4dlg/...',
             'rev': '2',
             'time': '2025/02/21 03:36:09',
             'type': 'ktext',
             'user': 'zerdlg'}>

            usage example (no join), groupby after records are selected:
                    >>> qry = (jnl.domain.type=='99')
                    >>> records = jnl(qry).select(*["idx","type","name","mount","owner","options"])
                    >>> records
                    <Records (500)>
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

                     >>> records = oJnl(qry).select(*["id","type","name","mount","owner","options"], as_groups=False)

        res = filter(lambda rec: min <= rec.idx <= max, changes)
        '''
        #self.as_groups = as_groups
        if (records is None):
            records = self
        if (len(records) == 0):
            '''  we have no records! return an empty set
            '''
            return records

        if (len(self.cols) == 0):
            if (as_groups is False):
                self.cols = records(0).getkeys()
            elif (as_groups is True):
                if (is_recordType(self.records(0)) is True):
                    keys = self.records(0).getkeys()
                    table1 = self.records(0)[keys(0)]
                    if (
                            (is_recordType(table1) is True) |
                            (is_dictType(table1) is True)
                    ):
                        table2 = self.records(0)[keys(1)]
                        self.cols = table1.getkeys().union(table2.getkeys())
                    else:
                        self.cols = records(0).getkeys()
        records_by_group = ZDict()
        recordslen = len(records)

        def group(record, num):
            if (num <= (len(fields) - 1)):
                try:
                    rec = group(record, (num + 1)) or Lst([record])
                    name = str(record[fields[num].fieldname]) \
                        if (is_fieldType(fields[num]) is True) \
                        else str(record[fields[num]])
                    if (
                            (isinstance(rec, dict) is True) |
                            (is_recordType(rec) is True)
                    ):
                        records_by_group[name].append(rec)
                    elif (
                            ((isinstance(rec, list) is True) |
                            (is_recordsType(rec) is True)) &
                            (name in records_by_group)
                    ):
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
            records_by_group[groupname] = Records(records_by_group[groupname])
            self.counts.update(**{groupname: len(records_by_group[groupname])})
            if (orderby is not None):
                recordgroup = self.orderby(
                    orderby,
                    reverse=reverse,
                    records=records_by_group[groupname]
                )
                records_by_group.update(
                    **{
                        groupname: Records(
                            recordgroup,
                            self.cols,
                            self.objp4
                        )
                    }
                )
            if (sortby is not None):
                recordgroup = self.sortby(
                    sortby,
                    limitby=limitby,
                    reverse=reverse,
                    records=records_by_group[groupname]
                )
                records_by_group.update(
                    **{
                        groupname: Records(
                            recordgroup,
                            self.cols,
                            self.objp4
                        )
                    }
                )

        ''' return grouped records as a dict , eg.
                {
                    group_name: <Records(
                                        <{Record}>
                                        <{Record}>,
                                        <{Record}>,
                                        <{Record}>
                                        )>,
                   ,group_name2: <Records(
                                        <{Record}>,
                                        <{Record}>
                                        )>
                }
            
            or return as a single list of grouped records
                <Records(
                        <{Record}>
                        <{Record}>,
                        <{Record}>,
                        <{Record}>,
                        <{Record}>,
                        <{Record}>
                        )>
                        
        '''
        if (as_groups is True):
            ''' Expose instance's `count` & `as_groups` methods so as 
                to easily access these values when return value is type ZDict.
            '''
            #[setattr(records_by_group, att, getattr(self, att)) for att in ('count', 'as_groups')]
            return records_by_group

        outrecords = Lst()
        for rgroup in records_by_group.getkeys():
            outrecords += records_by_group[rgroup]

        return Records(outrecords, self.cols, self.objp4)

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

    def get_tableoptions(self, tablename, *args, **kwargs):
        options = Lst()
        ''' do something to validate user input & apply to sdearch as well!
        '''
        optionsmap = self.objp4.memoizetable(tablename).tableoptions.optionsmap
        for (key, value) in ZDict(kwargs).items():
            key = re.sub('\-', '', key).lower()
            if (key in optionsmap.keys()):
                optkey = f'--{key}'
                if (isinstance(value, bool)):
                    if (value == True):
                        options.append(optkey)
                    kwargs.pop(key)
                elif (noneempty(value) is False):
                    (
                        options.append(optkey),
                        options.append(value)
                    )

        '''
                Usage as per `p4 help`: 
                    >>> print [-a -A -k -o localFile -q -m max] [-U] files...

                table/cmcd option defaults on a file/revision:
                '''
        fileoptions = ZDict(
            {
                'unload': False,            # (-U) enables access to unloaded objects.
                'quiet': False,             # (-q) suppresses normal information output.
                'max': None,                # (-m) specifies the max number of objects.
                'file': None,               # (-o) specifies the name of the output file.
                'leavekeywords': False,     # (-k) specifies that RCS keywords are not to be expanded in the output.
                'archive': False,           # (-A) enables access to the archive depot.
                'all': False                # (-a) specifies that the command should include all objects.
            }
        )
        [fileoptions.update(**{kwarg:value}) for (kwarg, value) in kwargs if (kwarg in fileoptions.items())]
        return options

    def sync(
            self,
            *options,
            query=None,
            limitby=None,
            action=None,
            include_deleted_files=False,
            #
            # cmd line args             # Usage: sync [ -f -k -n -N -p -q -r -s ] [-m max] [files...]
            #
            estimates=False,            # (-N) specifies to display estimates about the work required.
            parallel=None,              # parallel=threads=N,batch=N,batchsize=N,min=N,minsize=N
                                        # specify file transfer parallelism.
                                        #
            uselist=True,               # (-L) collects multiple file arguments into an in-memory list.
            safe=False,                 # (-s) performs a safety check before sending the file.
            publish=False,              # (-p) populates the client workspace but does not update the server.
            preview=False,              # (-n) specifies that the command should display a preview of the results.
            quiet=False,                # (-q) suppresses normal information output.
            max=None,                   # (-m) specifies the maximum number of objects.
            keepclient=False,           # (-k) leaves the client's copy of the files untouched.
            force=False,                # (-f) overrides the normal safety checks.
            **kwargs
    ):
        if (is_P4Jnl(self.objp4) is True):
            bail('`sync` attribute is of no use here... dropping.')

        kwargs = ZDict(kwargs)
        records = self
        (start, end) = limitby \
            if (noneempty(limitby) is False) \
            else (0, len(records))
        records = records.limitby((start, end))

        if (len(records) == 0):
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
            syncfiles = Lst()
            for record in records:
                if (
                        (action is not None) &
                        (record.action != action)
                ):
                    exclude_actions.append(record.action)

                record_action = Lst(re.split('/', record.action))(1)
                if (not record_action in exclude_actions):
                    if (record.code is not None):
                        record.delete('code')
                    intersect = self.cols.intersect(
                        [
                            'depotFile',
                            'clientFile',
                            'path'
                        ]
                    )
                    if (len(intersect) > 0):
                        syncFile = f'{record[intersect(0)]}'
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
                        syncfiles.append(syncFile)
            options = self.get_tableoptions('sync', **kwargs)

            """
            ''' do something to validate user input & apply to sdearch as well!
            '''
            for (key, value) in ZDict(kwargs.copy()).items():
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
                if (
                        (
                                optionitem(0) is True
                        ) &
                        (
                                len(Lst(optionitem(1), optionitem(2)).intersect(options)) == 0
                        )
                ):
                    options.append(optionitem(1))
            """
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
            return Records(syncrecords, cols, self.objp4)
        except Exception as err:
            bail(err)

    def search(
            self,
            *term,
            query=None,
            limitby=None,
            **kwargs
    ):
        ''' [(rec.name, col) for rec in oSchema.p4schema.recordtypes.record for col in rec.column \
        if ((col.name in ('depotFile', 'clientFile')) & (col.type == 'File'))]

            USAGE:
            >>> searchrecords = Records([], cols=self.cols, objp4=self.objp4)

            >>> filename = '//dev/p4dlg/p4/libpy4/py4IO.py'
            >>> printed = oP4.print(filename)
            >>> content = ''.join([p.data for p in printed[1:] if (p.data is not None)])
            >>> results = printed.search(content, "administrationlist")
            >>> records = [results.sortby('score')]
            >>>

            or
            >>> files = p4(p4.files, *['//dev/p4dlg/libconnect/...']).select()
            >>> results = files.search('zerdlg')
            >>> results(0)
            <Record {'action': 'add',
                        'change': '480',
                        'code': 'stat',
                        'context': '... [$Author: zerdlg $]',
                        'depotFile': '//dev/p4dlg/libconnect/__init__.py',
                        'fileSize': '311',
                        'linenumber': 8,
                        'rev': '1',
                        'score': '0.7071067811865475',
                        'search_terms': ['zerdlg'],
                        'time': '1726902220',
                        'type': 'text'}>
        '''
        records = self
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

        if (len(records) == 0):
            return self

        recordcols = records(0).getkeys()

        term = Lst([term]) \
            if (isinstance(term, str)) \
            else Lst(term)

        (
            cols,
            idx,
            metadata
        ) = \
            (
                  recordcols or self.cols,
                0,
                None
            )

        searchresults = []
        oSearch = Search()
        for record in records:
            if (re.search('text', record.type) is not None):
                intersect = cols.intersect(
                    [
                        'depotFile',
                        'clientFile',
                    ]
                )
                p4File = intersect(0)
                if (p4File is not None ):
                    ''' defaults to head revision
                    '''
                    searchFile = f'{record[p4File]}'
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
                            searchFile = f'{searchFile}{specchar}{record[specitem]}'
                            ''' we have something, break free & exec!
                            '''
                            break

                    filechunks = self.objp4.print(searchFile)
                    if (len(filechunks) > 1):
                        metadata = filechunks(0)
                        datachunks = filechunks[1:]
                        data = '\n'.join(Lst(chunk.data for chunk in datachunks if (chunk.code == 'text')))
                        if (len(data) > 0):
                            results = oSearch(data, *term)
                            for result in results:
                                #context = re.sub('^\s*', '... ', result.context).rstrip('\n')
                                context = result.context.rstrip('\n')
                                searchdata = ZDict(
                                    {
                                        'score': str(result.score),
                                        'search_terms': result.terms,
                                        'linenumber': result.idx,
                                        'context': context
                                    }
                                )
                                if (start <= idx):
                                    if (metadata is not None):
                                        searchdata.merge(metadata)
                                    searchresults.append(searchdata)
                                idx += 1
                                if (idx == end):
                                    break
        return Records(searchresults, [], objp4=self.objp4)