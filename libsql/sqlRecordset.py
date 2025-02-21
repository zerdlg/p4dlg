import re
from types import *
from pprint import pformat
import timeit

from libsql.sqlCount import Count
from libsql.sqlSelect import Select
from libsql.sqlRecords import Records
from libdlg.dlgStore import ZDict, Lst, objectify
from libdlg.dlgUtilities import noneempty, bail
from libfs.fsFileIO import ispath, loadspickle
from libdlg.dlgSearch import Search
from libsql.sqlValidate import is_field_tableType
from libsql import is_fieldType
from libjnl.jnlFile import JNLFile

__all__ = ['RecordSet']

'''  [$File: //dev/p4dlg/libsql/sqlRecordset.py $] [$Change: 609 $] [$Revision: #5 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
'''

class RecordSet(object):
    __hash__ = lambda self: hash(
        (frozenset(self),
         frozenset(self.recordset))
         #frozenset(ZDict(self.__dict__).getvalues()))
    )
    __bool__ = lambda self: True
    __copy__ = lambda self: self
    __repr__ = lambda self: f'<RecordSet ({type(self.recordset)}) >'
    __str__ = __repr__


    def error(self, left, right, op, err):
        ''' badly formatted queries & other errors, return False
        '''
        print(f"Invalid query '({left} {op} {right})'\nError: {err}")
        return False

    def case(self, true=1, false=0):
        try:
            return CASE(true, false)
        except Exception as err:
            ''' NOT YET IMPLEMENTED'''

    def __xor__(self, left, right):
        try:
            return (
                    left ^ right
            )
        except Exception as err:
            return self.error(left, right, '^', err)

    def __and__(self, left, right):
        try:
            return (
                    left and right
            )
        except Exception as err:
            return self.error(left, right, '&', err)

    def __or__(self, left, right):
        try:
            return (
                    left | right
            )
        except Exception as err:
            return self.error(left, right, '|', err)

    __rand__ = __and__
    __ror__ = __or__

    def __eq__(self, left, right):
        try:
            return (
                    str(left) == str(right)
            )
        except Exception as err:
            return self.error(left, right, '=', err)

    def __ne__(self, left, right):
        try:
            return (
                    str(left) != str(right)
            )
        except Exception as err:
            return self.error(left, right, '!=', err)

    def __lt__(self, left, right):
        try:
            return (
                    float(left) < float(right)
            )
        except Exception as err:
            return self.error(left, right, '<', err)

    def __gt__(self, left, right):
        try:
            return (
                    float(left) > float(right)
            )
        except Exception as err:
            return self.error(left, right, '>', err)

    def __le__(self, left, right):
        try:
            return (
                    float(left) <= float(right)
            )
        except Exception as err:
            return self.error(left, right, '<=', err)

    def __ge__(self, left, right):
        try:
            return (
                    float(left) >= float(right)
            )
        except Exception as err:
            return self.error(left, right, '>=', err)

    def __contains__(self, left, right):
        try:
            return (
                    self.memoizefield(left).search(str(right)) is not None
            )
        except Exception as err:
            return self.error(left, right, '#', err)

    def __invert__(self, left):
        try:
            return ~left
        except Exception as err:
            return self.error(left, None, 'NOT', err)

    ''' Basic String Query Syntax :

        Think tables and fields! Like SQL for python

        1 rule: no spaces within the expression, unless you mean it!
            <table>.<field><operator>value
            'user.name=zerdlg'            

                    *   ' ' a space (\s) between queries means __and__
                    *   # signals a regex (search) (or 'contains')
                    *   !# not / 'not contains')
    '''
    '''      (op in ('#^', '#$', '#')) / (op == '!#')
    '''
    env = ZDict()

    def __init__(
            self,
            objp4=None,
            records=Lst(),
            query=None,
            *args,
            **tabledata
    ):
        (args, tabledata) = (Lst(args), objectify(tabledata))
        self.objp4 = objp4

        ''' logging
        '''
        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.objp4.logger \
                        if (hasattr(self.objp4, 'logger')) \
                        else tabledata.logger,
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
        )
        ]

        ''' set as attributes whatever is left in kwargs
        '''
        [
            setattr(self, tdata, tabledata[tdata]) for tdata in tabledata
        ]

        self.oSchema = self.objp4.oSchema or ZDict()
        self.cols = self.fieldnames if (hasattr(self, 'fieldnames')) else Lst()
        ''' recordset is the reference to jnlFile.JNLFile
            should be re-used for the constraint_recordset
        '''
        self.recordset = records
        ''' records
        '''
        self.records = self.defineRecords(records)
        ''' queries
        '''
        self.query = query
        ''' schema resources
        '''
        self.oSchemaType = self.objp4.oSchemaType \
            if (hasattr(self.objp4, 'oSchemaType') is True) \
            else None

        compute = tabledata.compute or []
        self.compute = Lst(item.strip().split('=') for item \
                in Lst(compute.split(';')).clean()).clean() \
            if (isinstance(compute, str)) \
            else compute
        self.oSearch = Search()#maxrows=self.maxrows)
        self.querytables = set()
        self.fieldmemo = {}
        self.oTimer = timeit.timeit
        self.reference = None
        self.__dict__ = objectify(self.__dict__)

    def updateenv(self, **kwargs):
        [
            [
                self.env.merge(
                    {
                        i: eval(f'{k}.{i}')
                    }
                )
                for i in dir(v)
            ]
                for (k, v) in kwargs.items()
        ]

    def validate_fieldname(self, fname, tname):
        if (
                (len(self.fieldsmap) > 0) &
                (self.fieldsmap[fname.lower()] is not None)
        ):
            fname = self.fieldsmap[fname.lower()]
        if (fname is None):
            ''' if we are here, its because the initial safegard that should
                have forced bail on this Query has been turned off...
                no such fieldname in this tablename, no sense in moving formard
            '''
            bail(
                f"No such field ({fname}) in Table ({tname})"
            )
        return fname

    def define_queries(self, *queries, **kwargs):
        (
            queries,
            kwargs
        ) = \
            (
                Lst(queries),
                ZDict(kwargs)
            )
        if (
                (len(queries) == 1) &
                (isinstance(queries(0), tuple) is True)
        ):
            queries = Lst(queries(0))
        if (len(queries) == 0):
            for kwarg in kwargs:
                if (kwarg.startswith(r'^quer')):
                    queries += kwargs.pop(kwarg)
                    break
        self.loginfo(f'defined: {len(queries)} query(ies)')
        return queries

    def __call__(self, *queries, **kwargs):
        kwargs = ZDict(kwargs)
        if (
                (self.query is None) &
                (len(queries) > 0)
        ):
            self.query = self.define_queries(*queries, **kwargs) \
                if (len(queries) > 0) \
                else Lst()
        self.reference = kwargs.reference
        return self

    def defineCols(self, records):
        recs = (rec[1] for rec in records)
        cols = Lst()
        record = ZDict(next(recs))
        if (len(record) > 0):
            cols = record.getkeys()
        else:
            regex = re.compile('\W+')
            cols = Lst(
                [
                    regex.sub('_', col.strip().lower()) for col in record
                ]
            )
            for i in range(len(cols)):
                try:
                    while cols[i] in cols[:i]:
                        cols[i] = cols[i] + '2'
                except StopIteration:
                    pass
        self.loginfo(pformat(cols))
        return cols

    def literalValue(self, val):
        try:
            val = val.strip()
            if (str(int(val)) == str(val)):
                return int(val)
            return float(val)
        except ValueError:
            return val
        except Exception as err:
            bail(err)

    def defineRecords(self, records):
            if (hasattr(records, 'loads')):
                records = loadspickle(records)
                if (len(self.cols) == 0):
                    self.cols = records.first().tostorage().getkeys()
                return enumerate(records, start=1)
            if (isinstance(records, str)):
                records = objectify(loadspickle(records))
                if (len(self.cols) == 0):
                    self.cols = records.first().getkeys()
                return enumerate(records, start=1)
            if (isinstance(records, Records) is True):
                if (len(records) > 0):
                    record = records.first()
                    if (
                            (record is not None) &
                            (len(self.cols) == 0)
                    ):
                        self.cols = records.cols or record.getkeys()
                    return enumerate(records)
                else:
                    return records

            ''' Nothing so far?
            
                Define cols in the same way for any 
                of the following record types. 
            '''
            if (len(self.cols) == 0):
                self.cols = Lst()#self.defineCols(records)

            ''' alright, what do we have?
            '''
            if (type(records) is enumerate):
                return records
            elif (type(records) is GeneratorType):
                return enumerate(records, start=1)
            elif (isinstance(records, (Lst, list)) is True):
                return enumerate(records, start=1)
            elif (hasattr(records, 'readlines')):
                return enumerate(records.readlines(), start=1)
            elif (isinstance(records, JNLFile) is True):
                return enumerate(records.reader(), start=1)
            elif (ispath(records)):
                ''' NOT YET IMPLEMENTED
                    TODO: this. 
                '''
            elif (type(records).__name__ == 'Py4Run'):
                if (hasattr(records, 'options')):
                    records = getattr(records, '__call__')(*records.options)
                return ZDict(records) \
                    if (isinstance(records, dict)) \
                    else enumerate(records, start=1)

    def memoizefield(self, value):
        try:
            fielditem = self.fieldmemo[value]
        except KeyError:
            fielditem = self.fieldmemo[value] = re.compile(value)
        return fielditem

    def truncate(self, text, length):
        try:
            return text[:length].strip()
        except Exception as err:
            self.logwarning(err)
            return text

    def clean(self, data):
        try:
            if str(int(data)) == str(data):
                return int(data)
            return float(data)
        except ValueError:
            return data.strip()

    def close(self):
        try:
            self.records = Lst()
            self.query = None
            self.cols = Lst()
            self.reference = None
        finally:
            self.loginfo('RecordSet records & query have been reset')

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
                 *options ,
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
        (options, kwargs) = (Lst(options), ZDict(kwargs))
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
                if (
                        (action is not None) &
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
                            if (isinstance(q.right, ZDict)):
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
                                Lst(force,      '-f', '--force'),
                                Lst(preview,    '-n', '--preview'),
                                Lst(estimates,  '-N', '--estimates'),
                                Lst(safe,       '-s', '--safe'),
                                Lst(publish,    '-p', '--publish'),
                                Lst(max,        '-m', '--max'),
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
        finally:
            self.close()

    def search(self, *term, limitby=None):
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
                        if (
                                (code == 'text') &
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
                            if (isinstance(q.right, ZDict)):
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
                        ZDict()
                    )
                if (isinstance(item, dict)):
                    item = ZDict(item)
                    if (item.Description is not None):
                        source = item.Description
                    elif (item.desc is not None):
                        source = item.desc
                    metadata = item
                elif (isinstance(item, Lst)):
                    metadata = ZDict(item(0))
                    if (len(out) == 2):
                        source = out(1).data
                    else:
                        source = ''
                        for idx in range(1, len(out)):
                            source += out(idx).data
                results = self.oSearch(source, *term)
                for result in results:
                    context = re.sub('^\s*', '... ', result.context)
                    searchdata = ZDict(
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
            return Records(searchrecords, cols, self.objp4)
        except Exception as err:
            bail(err)
        finally:
            self.close()

    def fetch(self, *fields, **kwargs):
        try:
            kwargs.update(**{'limitby':(0,1)})
            records = self.select(*fields, **kwargs)
            return records.first()
        except Exception as err:
            bail(err)

    def update_record(self, *args, **kwargs):
        (
            args,
            kwargs
        ) = \
            (
                Lst(args),
                ZDict(kwargs)
            )
        ''' 
            TODO: Include this  
        '''

    def delete_record(self, *args, **kwargs):
        (
            args,
            kwargs
        ) = \
            (
                Lst(args),
                ZDict(kwargs)
            )
        ''' 
            TODO: include this. 
        '''

    def update(self, **update_fields):
        records = self.select(update_fields=update_fields)
        #indices = self.select('idx')
        #records.update(**update_fields)
        records.update(**update_fields)
        self.records = records
        return records

    def delete(self, *args, **kwargs):
        indices = self.select()
        for idx in indices:
            self.records.delete()
        return self.records

    def _select(
                self,
                *fields,
                query=None,
                records=None,
                cols=None,
                **kwargs
    ):
        (
            objField,
            fieldvalues,
            fields
        ) = \
            (
                None,
                (),
                Lst(fields)
            )
        outrecords = self.select(
                                    *fields,
                                    query=query,
                                    records=records,
                                    cols=cols,
                                    close_session=False,
                                    **kwargs
        )
        if (len(fields) == 0):
            fields = self.cols
        sifields = Lst((field.fieldname
                        if (is_fieldType(field) is True)
                        else field) for field in fields)
        try:
            if (len(sifields) == 1):
                key = sifields(0)
                return tuple(set(record[key] for record in outrecords))
            return Lst(record.getvalues() for record in outrecords)
        except Exception as err:
            print(err)
        return ()

    def select(
                self,
                *fieldnames,
                query=None,
                records=None,
                cols=None,
                close_session=True,
                **kwargs
    ):
        kwargs = ZDict(kwargs)

        (
            tablename,
            fieldnames,
            fieldsmap
        ) = \
            (
                kwargs.tablename,
                Lst(fieldnames),
                kwargs.fieldsmap or ZDict()
            )

        if (hasattr(self, 'tablename')):
            tablename = self.tablename
        elif (
                (len(fieldnames) == 1) &
                (is_field_tableType(fieldnames(0)) is True) &
                 ~(hasattr(self, 'tablename')
                 )
        ):
            tablename = fieldnames(0).tablename
            self.tablename = tablename

        if (noneempty(cols) is True):
            cols = self.cols = self.objp4.tablememo[tablename].fieldnames

        if (tablename is not None):
            if (len(fieldnames) == 0):
                fieldnames = self.fieldnames \
                    if (hasattr(self, 'fieldnames')) \
                    else self.objp4.tablememo[tablename].fieldnames
            if (noneempty(fieldsmap) is True):
                if (len(self.fieldsmap) > 0):
                    fieldsmap = self.fieldsmap
                else:
                    self.fieldsmap = fieldsmap = self.objp4.tablememo[tablename].fieldsmap
            #if (len(cols) == 0):
            #    self.cols = cols = self.objp4.tablememo[tablename].fieldnames
        if (
                (len(fieldnames) > 0) &
                (len(self.fieldnames) != len(fieldnames))
        ):
            self.fieldnames = fieldnames

        if (query is None):
            query = self.query
        if (records is None):
            records = self.records
        objp4 = self.objp4 \
            if (hasattr(self, 'objp4')) \
            else self
        tabledata = self.objp4.memoizetable(tablename)

        oSelect = Select(
            objp4,
            records=records,
            cols=cols,
            query=query,
            **tabledata
        )

        if (self.reference is not None):
            kwargs.update(
                join=self.reference.right._table.on(self.reference),
            )

        outrecords = oSelect.select(
            *fieldnames,
            close_session=close_session,
            **kwargs
        )
        return outrecords

    def isempty(self):
        return not self.select(limiy=(0, 1))

    def count(
            self,
            query=None,
            records=None,
            cols=None,
            distinct=None
    ):
        if (cols is None):
            cols = self.cols
        if (records is None):
            records = self.records

        if (query is None):
            query = self.query or []

        objp4 = self.objp4 \
            if (hasattr(self, 'objp4')) \
            else self
        oCount = Count(
            objp4,
            records,
            cols,
            query,
            tablename=self.tablename
        )
        return oCount.count(distinct=distinct)

    def computecolumns(self, record):
        try:
            for (key, value) in self.compute:
                self.cols.merge(key)
                self.updateenv(**record)
                record.update(**{key: eval(value, ZDict(), self.env)})
                self.loginfo(f'computed new column ({key})')
            return record
        except Exception as err:
            bail(err)

    def autocaster(self, data):
        data = data.strip()
        try:
            return int(data) \
                if (str(int(data)) == str(data)) \
                else float(data)
        except ValueError:
            return data