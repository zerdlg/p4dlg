import re
from types import *
from pprint import pformat
import timeit
from libsql.sqlSelect import Select
from libsql.sqlRecords import Records
from libdlg.dlgStore import Storage, Lst, objectify
from libdlg.dlgUtilities import noneempty, bail
from libfs.fsFileIO import ispath, loadspickle
from libdlg.dlgSearch import Search
from libsql.sqlValidate import *
from libjnl.jnlFile import JNLFile
from libsh import varsdata
from libsh.shVars import clsVars

__all__ = ['RecordSet']

'''  [$File: //dev/p4dlg/libsql/sqlRecordset.py $] [$Change: 724 $] [$Revision: #36 $]
     [$DateTime: 2025/05/19 20:19:42 $]
     [$Author: zerdlg $]
'''

class RecordSet(object):
    __iter__ = lambda self: self.__dict__.__iter__()

    __hash__ = object.__hash__
    __bool__ = lambda self: True
    __copy__ = lambda self: self
    __repr__ = lambda self: f'<RecordSet ({type(self.recordset)}) >'
    __str__ = __repr__

    def error(self, left, right, op, err):
        ''' badly formatted queries & other errors, return False
        '''
        print(f"Invalid query '({left} {op} {right})'\nError: {err}")
        return False

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
    env = Storage()

    def __init__(
            self,
            objp4=None,
            records=Lst(),
            query=None,
            maxrows=0,
            recordchunks=15000,
            compute=Lst(),
            *args,
            **tabledata
    ):
        (args, tabledata) = (Lst(args), objectify(tabledata))
        self.tabledata = tabledata
        self.objp4 = objp4
        self.varsdata = varsdata
        self.p4recordvars = clsVars(self, 'p4recordvars')
        self.jnlrecordsvars = clsVars(self, 'jnlrecordvars')
        self.recordchunks = recordchunks
        self.maxrows = maxrows
        self.compute = compute
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
        self.oSchema = self.objp4.oSchema or Storage()
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
        self.oSearch = Search()
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
                Storage(kwargs)
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
        kwargs = Storage(kwargs)
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
        record = Storage(next(recs))
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

    def defineRecords(self, records, **kwargs):
        if (hasattr(records, 'loads')):
            records = loadspickle(records)
            if (len(self.cols) == 0):
                self.cols = records.first().tostorage().getkeys()
        elif (isinstance(records, JNLFile) is True):
            records = records.reader()
        elif (isinstance(records, str) is True):
            records = JNLFile(records).reader() \
                if (ispath(records) is True) \
                else objectify(loadspickle(records))
            if (len(self.cols) == 0):
                self.cols = records.first().getkeys()
        elif (
                (isinstance(records, list) is True) |
                (is_recordsType(records) is True)
        ):
            if (len(records) > 0):
                record = records.first()
                if (record is not None):
                    if (
                            (record.code == 'error') &
                            (record.severity is not None)
                    ):
                        bail(record)
                    if (len(self.cols) == 0):
                        self.cols = records.cols or record.getkeys()
            elif (is_P4Jnl(self.objp4) is True):
                oJNLFile = JNLFile(
                    self.objp4.journal,
                    reader=self.objp4.reader
                )
                records = oJNLFile.reader()
        elif (hasattr(records, 'readlines')):
            records = records.readlines()
        elif (type(records).__name__ == 'Py4Run'):
            records = getattr(records, '__call__')(*records.options, **kwargs)
            if (is_recordType(records) is True):
                ''' records is a single record, wrap it up 
                    as a set of records and move on. 
                '''
                records = Records([records])
        if (noneempty(self.cols) is True):
            self.cols = Lst()
        if (type(records) in (enumerate, GeneratorType)):
            return records
        return enumerate(records, start=1)

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
                Storage(kwargs)
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
                Storage(kwargs)
            )
        ''' 
            TODO: Include this  
        '''

    def update(self, **updateargs):
        ''' This `update` method updates a set of records (or
            a single record, like a spec) but does NOT save/submit
            anything to the Server. To do so, as with any reliable
            DB system, the records (rows) must be committed.

            Since we should be able to commit when ever the user is ready,
            as opposed to simply being committed after a successful records
            update, we dump the updated records in a `vars` object (a pickle/cache).
            This way, we can commit when ready. See documentation for committing records.
        '''
        if (is_Py4(self.objp4) is True):
            recordsvars = self.p4recordvars
            if (recordsvars(self.tablename) is None):
                recordsvars(self.tablename, Records())
            if (self.is_spec is True):
                ''' In the case of a spec, we update one record at a time.
                    Which requires that `self.records` be a singular reference
                    to class Record().
                    
                    The `update` workflow for a spec.
                    
                    1) make sure self,.records *is* a Record()
                    2) remove fields that would otherwise prevent
                       the spec to be saved to the server.
                    3) Save/dump the record to `recordsvars`.
                       
                       * All recordvars keys are tablenames, while
                         all recordsvars values are Records objects.
                         Therefor singular specs are then inserted into
                         the Records object. As follows:
                         
                         {tablename: Records(), }
                         
                         VIEW a recordsvars table Records object:
                            >>> recordsvars(tablename)
                            <Records (n)>
                         CREATE a new empty Records object:
                            >>> recordsvars(tablename, Records())                     
                         DELETE a row:
                            >>> recordsvars(tablename, None)   
                         ADD a record to an existing table Records object:
                            >>> recordsvars(tablename).insert(-1, myRecord)
                         DELETE a single record from a Records object:
                            likely something like this:
                            >>> record = next(filter(lambda record: (recordsvars.altarg) == <specname>, recordsvars(tablename)))
                            >>> recordsvars(tablename).pop(record)
                            <spec record>
                            
                            wrapped in an easy to access func, like this:
                            
                            def delete_vars_record(tablename, specname):
                                record = next(
                                            filter(
                                                lambda record: (recordsvars.altarg == <specname>), 
                                                recordsvars(tablename)
                                                )
                                            )
                                popped_record = recordsvars(tablename).pop(record)
                                return popped_record
                    
                    TODO: Setup a decorator to enforce stuff like
                          minimum `admin` access rights, otherwise bail
                          this action. eg.:
                          
                          @requires_admin
                        
                          * For now, assuming the user has enough access to delete specs.
                '''
                crecord = self.records.copy()
                if (is_recordType(self.records) is True):
                    self.records.merge(**updateargs)
                    [self.records.delete(key) for key in crecord.keys() \
                     if key.lower() in ('access', 'update', 'code')]
                    if (recordsvars(self.tablename) is None):
                        recordsvars(self.tablename, Records())
                    recordsvars(self.tablename).insert(-1, self.records)
            else:
                ''' to be selective, select your target records.
                
                    TODO: Trim down this list to make sure the cmd 
                          creates a new rev and requires to be submitted
                          to the server.
                '''
                eor = False
                records = (record for record in self.select())
                while (eor is False):
                    try:
                        rec = next(records)
                        rec.merge(**updateargs)
                        recordsvars(self.tablename).insert(-1, rec)
                    except Exception as err:
                        bail(err)
                    except StopIteration:
                        eor = True

    def delete(self, *args, **kwargs):
        if (is_Py4(self.objp4) is True):
            recordsvars = self.p4recordvars
            if (recordsvars(self.tablename) is None):
                recordsvars(self.tablename, Records())
            if (self.is_spec is True):
                if (is_recordType(self.records) is True):
                    specname = self.records[self.tablename]
                    if (recordsvars(self.tablename) is not None):
                        recordsvars(self.tablename, None).insert(-1, self.records)
                    res = self.objp4[self.tablename]('-df', specname)
                    return res
            else:
                eor = False
                recids = (id for i in len(self.select()))
                while (eor is False):
                    try:
                        recid = next(recids)
                        recordsvars(self.tablename).pop(recid)
                    except Exception as err:
                        bail(err)
                    except StopIteration:
                        eor = True

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
                limitby=None,
                maxrows=0,
                recordchunks=15000,
                compute=Lst(),
                close_session=True,
                **kwargs
    ):
        (fieldnames, kwargs) = (Lst(fieldnames), Storage(kwargs))
        if (isinstance(fieldnames(0), list) is True):
            fieldnames = Lst(fieldnames(0))

        tablename = self.tablename \
            if (hasattr(self, 'tablename')) \
            else self.tabledata.tablename \
            if (self.tabledata.tablename is not None) \
            else query.left.tablename \
            if (query is not None) \
            else kwargs.tablename \
            if (kwargs.tablename is not None) \
            else fieldnames(0).tablename \
            if (
                (is_fieldType_or_expressionType(fieldnames(0)) is True) |
                (is_tableType(fieldnames(0)) is True)
        ) \
            else None

        if (not hasattr(self, 'tablename')):
            self.tablename = tablename

        objp4 = self.objp4 \
            if (hasattr(self, 'objp4')) \
            else self

        tabledata = self.tabledata = self.objp4.memoizetable(tablename)

        if (
                (noneempty(tabledata) is False) &
                (noneempty(cols) is True)
        ):
            cols = self.cols = tabledata.fieldnames

        if (noneempty(query) is True):
            query = self.query \
                if (noneempty(self.query) is False) \
                else fieldnames.pop(0) \
                if (is_queryType(fieldnames(0)) is True) \
                else None

        if (records is None):
            records = self.records

        oSelect = Select(
            objp4,
            records=records,
            cols=cols,
            query=query,
            **tabledata
        )
        if (
                (len(self.tabledata) in (0, 3)) |
                (self.tabledata is None)
        ):
            self.tabledata = tabledata
        if (self.reference is not None):
            kwargs.update(
                join=self.reference.right._table.on(self.reference),
            )

        outrecords = oSelect.select(
            *fieldnames,
            maxrows=maxrows,
            recordchunks=recordchunks,
            compute=compute,
            limitby=limitby,
            close_session=close_session,
            **kwargs
        )
        return outrecords

    def isempty(self):
        return (not self.select(limiy=(0, 1)))

    def defineAggregateKWargs(self, *fieldnames, name=None, query=None, **kwargs):
        if (name is None):
            bail('sqlRecordset.defineAggregateKWargs() requires a valid name (`count`, `sum` or `avg`)')
        (fieldnames, kwargs) = (Lst(fieldnames), Storage(kwargs))
        (
            tabledata,
            tablename,
            fieldname
        ) = (
            None,
            None,
            None
        )

        tabledata = getattr(self, 'tabledata')
        tablename = getattr(self, 'tablename')

        if (len(fieldnames) > 0):
            for fld in fieldnames:
                if (is_fieldType(fld) is True):
                    ''' TODO: should we pop the fieldname so as to only construct the expression, 
                        or should it stay so as to also define the output fieldlist.
                        
                        * think about it!
                    
                    fidx = fieldnames.index(fld)
                    fld = getattr(fieldnames.pop(fidx), name)()
                    '''
                    fld = getattr(fld, name)()
                if (is_expressionType(fld) is True):
                    kwargs[name] = fld
                    if (kwargs.distinct is None):
                        kwargs.distinct = False
                    break

        else:
            if (
                (query is None) &
                (self.query is not None)
            ):
                query = self.query

            qry = query(0) \
                if (isinstance(query, list) is True) \
                else query
            if (qry is not None):
                tablename = qry.left.tablename
                fieldname = qry.left.fieldname
                if (self.tabledata is None):
                    self.tabledata = self.objp4.memoizetable(tablename)
        if (kwargs[name] is None):
            exp = self.objp4[tablename][fieldname].count() \
                if (fieldname is not None) \
                else self.objp4[tablename].count()
            if (kwargs.distinct is None):
                kwargs.distinct = False
            kwargs[name] = exp
        return (fieldnames, kwargs)

    def count(
            self,
            *fieldnames,
            query=None,
            records=None,
            cols=None,
            close_session=True,
            **kwargs
    ):
        (fieldnames, kwexp) = self.defineAggregateKWargs(
            *fieldnames,
            name='count',
            query=query,
            **kwargs
        )
        outrecords = self.select(
            *fieldnames,
            query=query,
            records=records,
            cols=cols,
            close_session=close_session,
            **kwexp
        )
        return len(outrecords)

    def sum(
            self,
            *fieldnames,
            query=None,
            records=None,
            cols=None,
            close_session=True,
            **kwargs
    ):
        (fieldnames, kwexp) = self.defineAggregateKWargs(
            *fieldnames,
            name='sum',
            query=query,
            **kwargs
        )
        outrecords = self.select(
            *fieldnames,
            query=query,
            records=records,
            cols=cols,
            close_session=close_session,
            **kwexp
        )
        return outrecords

    def avg(
            self,
            *fieldnames,
            query=None,
            records=None,
            cols=None,
            close_session=True,
            **kwargs
    ):
        (fieldnames, kwexp) = self.defineAggregateKWargs(
            *fieldnames,
            name='avg',
            query=query,
            **kwargs
        )
        outrecords = self.select(
            *fieldnames,
            query=query,
            records=records,
            cols=cols,
            close_session=close_session,
            **kwexp
        )
        return outrecords