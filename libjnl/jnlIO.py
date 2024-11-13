import os
from os.path import dirname
from types import *
import datetime
from itertools import tee

from libdlg.dlgStore import Storage, Lst, objectify
from libdlg.dlgControl import DLGControl
from libdlg.dlgQuery_and_operators import *
from libdlg.dlgUtilities import (
    annoying_ipython_attributes,
    bail,
    isnum,
    noneempty,
    queryStringToStorage,
    fix_name
)
from libdlg.dlgRecordset import *
from libdlg.dlgSchemaTypes import *
from libdlg.dlgFileIO import loadpickle
from libdlg.dlgSchema import SchemaXML
from libdlg.dlgInvert import invert
from libjnl.jnlFile import JNLFile
#from libjnl.jnlGuess import GuessRelease
from libjnl.jnlSqltypes import JNLTable, JNLField

import schemaxml
schemadir = dirname(schemaxml.__file__)

'''  [$File: //dev/p4dlg/libjnl/jnlIO.py $] [$Change: 477 $] [$Revision: #45 $]
     [$DateTime: 2024/09/18 06:23:30 $]
     [$Author: mart $]
'''

'''     Journals (and checkpoints) are the textual representation of of the p4 db (the metadata). 
        In a nutshell, a journal is a textual representation of the metadata stored in a p4 DB. 

        P4Q is an program that tries to make sense of the rules, references, lookups, conversions, 
        guesses, etc. needed to interact with checkpoint or journal (or set of journals).  

        As an example, I use it for reading (query), writing (create), updating (edit/modify), and 
        deleting journal records. Beware and be careful as modified journals and checkpoints can 
        then be replayed against (like `imported` into) a P4D instance, thereby, and effectively, 
        committing changes the DB.  

        *Note: Every software release correlates to a dedicated DB schema. Make sure your server 
        version and schema version line up (though P4Q manages that well enough for us).
        +-----------------------------------------------------------------------------------------+
        
        Handle operators (first field of every journal record):
        
                +-----------+-------+---------------------------------------+
                | <= r97.3  | @put@ | 'put value' (insert a record)         |
                |           +-------+---------------------------------------+
                |           | @del@ | 'delete value' (delete a record)      |
                |-----------+-------+---------------------------------------+
                | >= r98.1  | @pv@  | @put@ is depricated, use this instead |
                |           +-------+---------------------------------------+
                |           | @dv@  | @del@ is depricated, use this instead |
                |           +-------+---------------------------------------+ 
                |           | @rv@  | 'replace' a value                     |
                |-----------+-------+---------------------------------------+
                | >= r99.2  | @vv@  | 'verify' a value                      |
                |-----------+-------+---------------------------------------+
                | >= 2006.2 | @ex@  | useless transaction records, does     |
                |           +-------+ not impact us at all                  |
                |           | @mx@  |                                       |
                |-----------+-------+---------------------------------------+
                | >= 2010.1 | @nx@  | as above                              |
                |-----------+-------+---------------------------------------+
                | >= 2011.1 | @dl@  | 'delete librarian'                    |
                |-----------+-------+---------------------------------------+
        
        I.e.:

        @pv@ 6 @db.domain@ @depot@ 100 @@ @@ @@ @@ @@ 1337399100 1337399100 0 @Default depot@ @@ @@ 0

        when replayed, P4D will add (@put@ / @pv@) a record of type 'domain', in table db.domain, 
        specifically a 'depot'. The value (100) to the right of @depot@ is the domain type.
'''
now = datetime.datetime.now
journal_actions = [
                    "put",
                    "del",
                    "pv",
                    "dv",
                    "rv",
                    "vv",
                    "ex",
                    "mx",
                    "nx",
                    "dl"
]
ignore_actions = [
                   "ex",
                   "mx",
                   "nx"
]
fixep4names = Storage({
                   'group': 'p4group',
                   'db.group': 'p4group',
                   'type': 'p4type',
                   'user': 'p4user',
                   'db.user': 'p4user',
                   'db.desc': 'describe',
                   'desc': 'describe'
})

__all__ = [
            'P4Jnl',
            'journal_actions',
            'ignore_actions',
            'fixep4names'
]

''' Notes:
        myQuery = (oJnl.change.user == 'mart')

            is transformed into

        {"left": {"tablename": "change",
                  "fieldname": "user"},
         "op":'EQ",
         "objp4": <libconnect.conJnl.ObjJnl at 0x13e61b010>, 
         "right": "mart"})
       
            or
       
        <DLGQuery {'left': <JNLField user>,
                  'objp4': <P4Jnl /Users/gc/anastasia/dev/p4dlg/resc/journals/journal.8>,
                  'op': <function EQ at 0x10387cd60>,
                  'right': 'mart'}>

        
        The journal object (oJnl)
        
        >>> oJnl = P4Jnl(journal_file, oSchema)
        
        oJnl is the journal reference to class P4Jnl
        
        It takes a journal file as args[0] and a schema object as args[1].

        oJnl is a callable and takes queries as arguments (and other keyword args)

        The following queries are equivalent:

            >>> query1 = (lambda rec: rec.user == 'mart')
            >>> query2 = (oJnl.change.user == 'mart')
            >>> query = "change.user=mart"                     
'''
class P4Jnl(object):
    __str__ = __repr__ = lambda self: f"<P4Jnl {self.journal}>"

    def __iter__(self):
        iter(self)

    def __and__(self, other):
        return DLGQuery(self.objp4, AND, self, other)

    def __or__(self, other):
        return DLGQuery(self.objp4, OR, self, other)

    def __xor__(self, other):
        return DLGQuery(self.objP4, XOR, self, other)

    __rand__ = __and__
    __ror__ = __or__

    def __init__(self, *args, **kwargs):
        '''     required args:  args(0) -> journal file or journalReader
                                args(1) -> a schema object or schemadir and p4dlg will try to
                                           guess the correct version (though yet untested)
        '''
        (args, kwargs) = (Lst(args), objectify(kwargs))
        ''' logging stuff
        '''
        self.loglevel = kwargs.loglevel or 'DEBUG'
        logfile = kwargs.logfile
        loggername = Lst(__name__.split('.'))(-1)
        self.logger = DLGControl(
            loggername=loggername,
            loglevel=self.loglevel,
            logfile=logfile
            )
        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.logger \
                        if (hasattr(self, 'logger')) \
                        else kwargs.logger or 'INFO',
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
            )
        ]
        #self.loginfo = self.logger.loginfo
        #self.logwarning = self.logger.logwarning
        #self.logerror = self.logger.logerror
        #self.logcritical = self.logger.logcritical
        ''' Journal and SchemaXML class reference
        '''
        try:
            self.journal = args(0)
            if (self.journal is None):
                bail(
                    f"MISSING: Journal file: {self.journal}"
                )
            else:
                self.loginfo(f"Journal file: {self.journal}")
            self.oSchema = args(1) \
                if AND(
                        (args(1) is not None),
                        (isinstance(args(1), SchemaXML))
                    ) \
                        else kwargs.oSchema
            if (self.oSchema is None):
                bail(
                    "could not define an SchemaXML class reference"
                )
            else:
                self.loginfo(f"schema version: : {self.oSchema.version}")
        except Exception as err:
            bail(err)
        ''' tablememo houses all and complete table/command definitions
        '''
        self.tablememo = {}
        ''' *** schemaversions:
                * Both formats are valid: 'r16.2'  or  '2016.2'
                * Default: could be 'latest' (doh! if you want to risk anything higher than r16.2)
                * IMHO, I wouldn't recommend it, so setting the default @r16.2            
        '''
        self.schemaversion = kwargs.schemaversion or self.oSchema.version #or self.guessVersion() -> I think this is broken
        self.schemadir = kwargs.schemadir or schemadir
        self.excludetables = Lst()
        ''' A list of tables to exclude.
            
            * let's start with db.have (because, really, who cares?)
        '''
        if (isinstance(self.excludetables, str)):
            self.excludetables = Lst([tbl for tbl in self.excludetables.split(',')])
        if (not 'have' in self.excludetables):
            self.excludetables.append('have')
        self.tables = [tblname for tblname in self.oSchema.p4model.getkeys() \
                       if (not tblname in self.excludetables)]
        self.tablenames = self.tables
        self.reader = kwargs.reader or 'csv'
        self.recordchunks = kwargs.recordchunks or 15000
        self.recCounter = Storage(
            {
                'threshhold': self.recordchunks,
                'recordcounter': 0
            }
        )
        self.tablepath = kwargs.tablepath or os.path.abspath('../../../storage')
        self.serialtables = os.path.join(self.tablepath, 'serialtables')
        self.oSchemaType = SchemaType(self)
        ''' Spec types belonging to db.domain
        '''
        self.domaintypes = self.oSchemaType.values_names_bydatatype('DomainType')
        self.tabletypesmap = self.oSchemaType.datatype_namemap()
        self.oNameFix = fix_name(kwargs.tableformat or 'remove')

    def __getattr__(self, tablename):
        tablename = str(tablename)
        invalidAttributeError = f'{tablename} is not a valid attribute or tablename'
        ''' Those strange IPython attributes again :(
        '''
        if (len(annoying_ipython_attributes(tablename)) > 0):
            return
        if (tablename in self.tables):
            try:
                if (not tablename in self.tablememo):
                    tabledata = self.memoizetable(tablename)
                    setattr(self, tablename, JNLTable(
                        self,
                        tablename,
                        self.oSchema,
                        **tabledata
                    )
                            )
            except KeyError as err:
                self.logerror (err)
        try:
            return self.__dict__[tablename]
        except KeyError as err:
            bail(invalidAttributeError)

    __getitem__ = __getattr__

    def guessVersion(self):
        '''    "GuessRelease" will parse the target journal until it has collected a sufficient
                amount of table records to determine which version the journal belongs to. Though
                it is quite possible that too few records are available to single out one single
                version. In this case, all versions compatible with the journal are returned
                as a list of "compatible_releases" where the default behaviour is to assume that
                the latest release is likely the correct version. In case of errors (I.e. after
                an analysis of an updated journal, it determines that another version should be
                selected), we can always re-run against an updated schema.
        '''
        try:
            compatible_releases = GuessRelease().guess(self.journal)
            likely_release = max(compatible_releases)
            self.loginfo(f'schemaxml likely release version: {likely_release}')
            return likely_release
        except Exception as err:
            self.loginfo(err)

    def get_tablename_fieldname_from_qry(self, qry):
        ''' Like self.breakdown_query(qry) but
            returns only tablename & fieldname.
        '''
        def getleft(q):
            (
                op,
                left,
                right
            ) = \
                (
                    q.op,
                    q.left,
                    q.right
                )
            operators = andops + orops + xorops + notops
            opname = op.__name__ \
                if (not isinstance(op, str)) \
                else op
            if (opname in operators):
                left = getleft(left)
            return left
        left = getleft(qry)
        return (left.tablename, left.fieldname) \
            if (left.tablename is not None) \
            else (None, None)

    def breakdown_query(self, qry, tabledata=None):
        (
            op,
            left,
            right,
            inversion
        ) = \
            (
                qry.op,
                qry.left,
                qry.right,
                qry.inversion or False
            )
        ANOX = andops + orops + xorops + notops
        opname = op.__name__ \
            if (callable(op) is True) \
            else op
        if (opname in ANOX):
            (
                q,
                left,
                right,
                op,
                tablename,
                inversion,
                tabledata
            ) = \
                self.breakdown_query(left, tabledata)

        if (isnum(right) is True):
            right = str(right)

        (
            tablename,
            fieldname
        ) \
            = \
            (
                left.tablename,
                left.fieldname
            )

        if (
                (tablename, fieldname) == (None, None)
        ):
            (tablename, fieldname) = (self.get_tablename_fieldname_from_qry(qry))
        if (not tablename in self.tables):
            bail(
                f'tablename `{tablename}` does not belong to schema ({self.schemaversion})'
            )
        if (not None in (tablename, fieldname)):
            if (noneempty(tabledata) is True):
                tabledata = self.memoizetable(tablename)
        else:
            bail(
                f"Fieldname `{fieldname}` & `{tablename}` may not be None."
            )
        if (fieldname.lower() not in tabledata.fieldsmap):
            bail(
                f"Fieldname '{fieldname}' does not belong to JNLTable '{tablename}'.\n\
Select among the following fieldnames:\n{tabledata.fieldnames}\n"
            )
        return (
                qry,
                left,
                right,
                op,
                tablename,
                inversion,
                tabledata
        )

    def resolve_datatype_value(self, qry):
        ''' datatypes of type `flag` have a number as a value. Though efficient, we,
            as users of journal data, don't necessarily have them memorized. However,
            we may know the name values of the target datatype.

            That said, P4Q accepts either the type name or the type number. It will
            simply resolve the name (if given) to its associated number. As in,

                >>> qry = (oP4.domain.type == 'client')

            As far as the fourna is concerns, it really knows about its type identifyer.
            In this case, the domain type representing a client is actually '99'.
            If you can remember these identifiers, then great! otherwize, the type name
            will work just as well.

            the journal record will look like this:

            eg.

            {'idx': '143992',
             'db_action': 'pv',
             'table_revision': '6',
             'table_name': 'db.domain',
             'name': 'fredclient',
             'type': '99',                                  <-- the domain's type
             'extra': 'charlotte.local',
             'mount': '/Users/mart/Perforce/fredclient',
             'mount2': '',
             'mount3': '',
             'owner': 'fred',
             'updateDate': '2015/05/07',
             'accessDate': '2015/05/07',
             'options': '0',
             'description': 'Created by fred.\n',
             'stream': '',
             'serverid': '',
             'partition': '0'}
        '''
        (
            left,
            right,
            op
        ) = \
            (
                qry.left,
                qry.right,
                qry.op
            )

        if (not None in (left, right)):
            ''' There is a left & a right side, 
                so definitely not a table.
            '''
            if (is_fieldType(left) is True):
                if (is_fieldType(right) is True):
                    ''' Both left AND right sides are fields,
                        nothing for us here, just get out.
                    '''
                    return qry
                ''' a query | expression, let's make 
                    sure the datatype is correct.
                '''
                fieldtype = left.type
                if (self.oSchemaType.validate_datatype_name(fieldtype) is not None):
                    if (fieldtype in self.oSchemaType.flagnames()):
                        right = self.oSchemaType.datatype_flag(right, fieldtype)
                        if (isinstance(right, int)):
                            right = str(right)
                        qry.right = right
                    if AND(
                            (hasattr(right, 'left')),
                            (hasattr(right, 'right'))
                    ):
                        if (right.left is not None):
                            qry = self.resolve_datatype_value(right)
            elif (
                    (left.tablename, left.fieldname) != (None, None)
            ):
                ''' Left is well formed, moving on.
                '''
                if (isinstance(left, dict) is True):
                    if (type(left).__name__ != 'Storage'):
                        left = Storage(left)
                    if (isnum(right) is True):
                        right = int(right)
                        left.type = 'Int'
                    else:
                        left.type = 'String'
                    qry = DLGQuery(
                        self,
                        op,
                        left,
                        right,
                        inversion=qry.inversion or False
                    )
                if (is_query_or_expressionType(left) is True):
                    left = self.resolve_datatype_value(left)
                    qry.left = left
                    right = self.resolve_datatype_value(right)
                    qry.right = right
                elif AND(
                        (hasattr(left, 'left')),
                        (hasattr(left, 'right'))
                ):
                    qry = self.resolve_datatype_value(left)
        return qry

    def __call__(
            self,
            query=None,
            **kwargs
    ):
        jnlQueries = Lst()
        kwargs = Storage(kwargs)
        (tablename, tabledata) = (kwargs.tablename, Storage())
        (queries, qries) = (Lst(), Lst())
        self.maxrows = kwargs.maxrows or 0
        self.compute = kwargs.compute or Lst()
        constraint = None
        ''' If query is a JNLTable, DLGQuery or DLGExpression, 
            define both tablename and tabledata. Otherwise,
            make sure it is typed correctly (it should be list)
        '''
        if (query is not None):
            if (isinstance(query, (list, Lst, tuple)) is False):
                if (query_is_constraint(query) is True):
                    constraint = query
                    ''' re-define query as the query's left 
                        side's Table object (_table)
                    '''
                    query = constraint.left._table
                    tablename = query.tablename
                    tabledata = self.memoizetable(tablename)
                elif (is_tableType(query) is True):
                    tablename = query.tablename
                    tabledata = self.memoizetable(tablename)
                else:
                    if (is_query_or_expressionType(query) is True):
                        qries =  Lst([query])
                    elif (isinstance(query, str)):
                        qries = objectify(Lst(queryStringToStorage(q) for q in query.split()).clean())
                    elif OR(
                            (isinstance(query, dict)),
                            (type(query) is LambdaType)
                    ):
                        qries =  objectify(Lst([query]))
            else:
                qries = objectify(Lst(query))

            for qry in qries:
                if (is_queryType(qry) is True):
                    if (tablename is None):
                        ''' grab the tablename and move on!
                        '''
                        (
                            q,
                            left,
                            right,
                            op,
                            tablename,
                            inversion,
                            tabledata
                        ) = (
                            self.breakdown_query(qry, tabledata=tabledata)
                        )
                    inversion = False
                    ''' we can't all remember obscure datatype values
                        jnl.domain.type == 'client' --> will replace 
                        'client' with its expected value of '99' for us.
                    '''
                    qry = self.resolve_datatype_value(qry)

                    if (isinstance(qry, dict) is True):
                        if (not 'inversion' in qry):
                            qry.inversion = inversion
                        qry = DLGQuery(
                            self,
                            qry.op,
                            qry.left,
                            qry.right,
                            qry.inversion
                        )
                    ''' try to invert, otherwise leave a is.
                    '''
                    qry = invert(qry)
                    ''' as of this point, all queries (except for Table 
                        or contraint) should be membe rof jnlQueries.
                    '''
                    jnlQueries.append(qry)

        if (tablename is not None):
            tabledata.merge(kwargs)
            for item in \
                    [
                        'recordchunks',
                        'schemadir',
                        'oSchemaType',
                        'logger'
                    ]:
                tabledata.merge({item: getattr(self, item)})
            tabledata.merge(
                                {
                                    'maxrows': self.maxrows,
                                    'compute': kwargs.compute or '',
                                    'tablename': tablename,
                                    'tabletype': P4Jnl
                                }
            )

        oJNLFile = JNLFile(self.journal, reader=self.reader)
        oRecordSet = DLGRecordSet(
            self,
            oJNLFile,
            **tabledata
        )

        if (len(jnlQueries) == 0):
            if (is_tableType(query) is True):        # A single query is defined, its just a _table.
                if (constraint is None):             # No constraints in this run
                    return oRecordSet                # Bypass DLGRecordSet.__call__ altogether!
                return oRecordSet(                   # A single query is defined, its really a constraint
                    constraint=constraint
                )
        return oRecordSet(*jnlQueries)               # jnlQueries > 0, pass them onto DLGRecordSet.__call__

    def getfieldmaps(self, tablename):
        ''' mapping of all table names  -> {lower_case_fieldname: actual_fieldname}
        '''
        initialfields = self.oSchema.p4model[tablename].fields
        fielddicts = initialfields.storageindex(reversed=True)
        fieldsmap = Storage({value.name.lower(): value.name for (key, value) in fielddicts.items()})
        fieldnames = fieldsmap.getvalues()
        fieldtypesmap = Storage({value.type.lower(): value.type for (key, value) in fielddicts.items()})
        return (
                fieldnames,
                fieldsmap,
                fieldtypesmap
        )

    def memoizetable(self, tablename):
        tabledata = Storage()
        if (tablename is not None):
            try:
                tabledata = self.tablememo[tablename]
            except KeyError:
                if (tablename is not None):
                    (
                        fieldnames,
                        fieldsmap,
                        fieldtypesmap
                    ) = \
                        (
                            self.getfieldmaps(tablename)
                    )
                    tabledata = self.tablememo[tablename] = Storage(
                                        {
                                            'fieldsmap': fieldsmap,
                                            'fieldtypesmap': fieldtypesmap,
                                            'fieldnames': fieldnames,
                                            'logger': self.logger,
                                        }
                    )
                    '''  table attributes & specify keying fields
                    '''
                    tableschema = self.oSchema.p4model[tablename]
                    attributes = tableschema.getkeys()
                    attributes.remove('tablename')
                    for tableattribute in attributes:
                        if (tableattribute != 'fields'):
                            value = self.oSchema.p4model[tablename][tableattribute]
                            if (tableattribute == 'keying'):
                                value = value.split(',')
                            elif (tableattribute == 'name'):
                                tabledata.merge({'_rname': value})
                            tabledata.merge({tableattribute: value})
                    self.loginfo(f'jnltable defined: {tablename}')
        return tabledata

    def dropTable(self, tablename):
        ''' * delete table from tablememo
            * delete table attribute from self
            * remove table file (if so generated)
        '''
        try:
            tidx = self.tables.index(tablename)
            self.tables.pop(tidx)
            if (self.tablememo[tablename] is not None):
                self.tablememo.pop(tablename)
            delattr(self, tablename)
        except (
                KeyError,
                OSError,
                AttributeError,
                Exception,
                IndexError
        ) as err:
            self.logwarning(err)

    def dropTables(self):
        [self.droptable(tablename) for tablename in os.listdir(self.tablepath)]

    def memoize_tables(self):
        [self.memoizetable(tablename) for tablename in self.oSchema.p4model.getkeys()]

    def tableLoad(self):
        sTables = loadpickle(self.serialtables) \
            if (os.path.exists(self.serialtables)) \
            else self.tablememo
        return sTables