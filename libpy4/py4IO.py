import logging
import re
from marshal import loads, load, dump, dumps
from types import *
from subprocess import Popen, PIPE, STDOUT
from pprint import pformat

from libdlg.dlgError import *
from libpy4.py4Options import Py4Options
from libpy4.py4Run import Py4Run
from libpy4.py4Sqltypes import Py4Table, Py4Field
from libdlg.dlgStore import (
    Storage,
    objectify,
    Lst
)
from libdlg.dlgQuery_and_operators import *
from libdlg.dlgUtilities import (
    noneempty,
    bail,
    decode_bytes,
    isnum,
    queryStringToStorage,
    Flatten,
    annoying_ipython_attributes,
    reg_envvariable,
    fieldType,
    set_localport,
    reg_filename,
    reg_rev_change_specifier,
    reg_p4global,
    is_marshal,
    Plural,
    table_alias
)
from libdlg.dlgControl import DLGControl
from libdlg.dlgFileIO import *
from libdlg.dlgRecordset import DLGRecordSet
from libdlg.dlgRecords import DLGRecords
from libdlg.dlgRecord import DLGRecord
from libdlg.dlgInvert import invert
from libhelp.hlpCmds import DLGHelp
from libdlg.dlgSchemaTypes import SchemaType
from libdlg.dlgSchema import SchemaXML, to_releasename
import schemaxml
from os.path import dirname
schemadir = dirname(schemaxml.__file__)

(
    mloads,
    mload,
    mdump,
    mdumps
    ) \
    = (
        loads,
        load,
        dump,
        dumps
)

'''  [$File: //dev/p4dlg/libpy4/py4IO.py $] [$Change: 474 $] [$Revision: #69 $]
     [$DateTime: 2024/09/09 06:39:06 $]
     [$Author: mart $]
'''

'''     a perforce client program.

        BASIC USAGE:
        
        The class reference (the connector) and the usual commands & cmd line options 
            >>>  oP4 = Py4(**{'user': 'mart',
                              'port':  'anastasia:1777',
                              'client': 'gc.depot',
                              'password':'Unset'}) --> value can be a password, a p4ticket, 
                                                       '[Uu]nset' or omit completely (in
                                                       it will check the environment (I.e. 
                                                       P4USER, etc.)
 
            The constructor takes any valid p4 global which will persist so long as the instance is alive.
                see `p4 help usage`   

            The class reference is callable, so any valid p4 global can be added here as well, though only 
            for the duration of a single cmd.
 
                >>> oP4(options=['-I', '-d', ])

        oP4 exposes all known p4 commands as attributes       
            
            >>> files = oP4.files('//depot/...')
        
        However, the fun is in the SQL functionality it supports (like queries). 
        For example:
        
            >>> qry = (oP4.clients.client.contains('fred'))
            >>> clients = oP4(qry).select()
            >>> clients
'''

__all__ = ['Py4', 'p4connector']

class Py4(object):
    '''
    A class that wraps the p4 cmdline API in an SQL abstraction.
    '''
    commands = lambda self: self.commandslist()
    usage = lambda self: self.usagelist()
    administration = lambda self: self.administrationlist()
    charset = lambda self: self.help('charset')
    configurables = lambda self: self.help('configurables')
    environment = lambda self: self.help('environment')
    filetypes = lambda self: self.help('filetypes')
    jobview = lambda self: self.help('jobview')
    networkaddress = lambda self: self.help('networkaddress')
    revisions = lambda self: self.help('revisions')
    streaminfo = lambda self: self.help('streaminfo')
    views = lambda self: self.help('views')
    replication = lambda self: self.help('replication')
    dvcs = lambda self: self.help('dvcs')
    legal = lambda self: self.help('legal')
    undoc = lambda self: self.help('undoc')
    credits = lambda self: self.help('credits')

    def __and__(self, other):
        return DLGExpression(self.objp4, AND, self, other)

    def __or__(self, other):
        return DLGExpression(self.objp4, OR, self, other)

    def __xor__(self, other):
        return DLGExpression(self.objp4, XOR, self, other)

    __rand__ = __and__
    __ror__ = __or__
    __str__ = __repr__ = lambda self: f"<Py4 {self._port} >"

    def __iter__(self):
        iter(self)

    def __init__(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        loglevel = kwargs.pop('loglevel') \
            if (kwargs.loglevel is not None) \
            else 'DEBUG'
        logfile = kwargs.logfile
        loggername = Lst(__name__.split('.'))(-1)
        self.logger = DLGControl(
            loggername=loggername,
            loglevel=loglevel,
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

        (
            self.p4credential_globals,
            self.supglobals
        ) = \
            (
                Lst(),
                Lst()
            )
        self.usage_items = []

        #(version, oSchema) = (kwargs.version, kwargs.oSchema)
        #if AND(
        #        (oSchema is None),
        #        (version is not None)
        #):
        #    oSchema = SchemaXML(schemadir, version)
        #''' oSchema is still None? no worry, we'll resolve that below :)
        #'''


        self.user_defined_globals = Lst()
        [kwargs.delete(kw) for kw in self.define_p4globals(**kwargs)]
        self.tablenames = self.commands = Lst()
        self.p4args = Lst()
        (
             self.is_spec,
             self.is_specs,
             self.is_command,
             self.is_help,
             self.is_explain,
             self.is_info
        ) \
            = \
            (
                False, False, False, False, False, False
        )
        self.oPlural = Plural()
        ''' spec collections & fields
        '''
        self.spec_takes_no_lastarg = [
            'license',
            'protect',
            'triggers',
            'typemap'
        ]
        self.domain_specs = [
            'branch',
            'client',
            'depot',
            'label',
            'stream',
            'typemap'
        ]
        self.p4spec_requires_lastarg = Lst(
                            [
                                'change',
                                'changelist',
                                'server',
                                'group',
                                'job',
                                'workspace',
                                'ldap',
                                'user',
                                'spec',
                                'remote'
                            ] + self.domain_specs
        )
        self.p4spec = (
                self.p4spec_requires_lastarg +
                self.domain_specs +
                self.spec_takes_no_lastarg
        )

        self.p4specs = self.pluralize_specs(
            exclude=self.spec_takes_no_lastarg
        )
        self.specfield_headers = [
            'code',
            'name',
            'type',
            'length',
            'attribute'
        ]
        self.specfield_attributes = [
            'key',
            'required',
            'optional',
            'always'
        ]
        self.specfield_types = [
            'word',
            'wlist',
            'line',
            'llist',
            'select',
            'date',
            'text',
        ]
        self.has_plural = self.p4spec + [
            'review',
            'counter',
            'key'
        ]
        self.fetchfirst = [
                              'help',
                              'info',
                              'tickets',
                              None
        ]
        self.nocommands = [
            None,
            '--explain',
            'explain'
        ]
        self.initcommands = [
            'set',
            'spec',
            'help'
        ]
        self.p4globals = Lst(['p4', '-G'] + self.p4credential_globals)
        (
            self.tablememo,
            self.specmapmemo,
            self.helpmemo
            ) = \
                (
                    {}, {}, {}
                )

        ''' we may need the server version and, definitively, the schema
        '''
        (
            version,
            oSchema
        ) = \
            (
                kwargs.version,                                 # the user may have passed in either (or both)
                kwargs.oSchema
            )
        if AND(
                (oSchema is None),
                (version is not None)
        ):
            oSchema = SchemaXML(schemadir, version)
        if (version is None):                                   # the user provided no such thing
            try:
                info = Py4Run(self, tablename='info')()(0)      # do p4 info
                strServerversion = info.serverVersion           # we just need the version string
                version = (                                     # the version string has way too much info
                    '.'.join(                                   # start breaking it up
                        re.split(
                            '\.',
                            Lst(
                                re.split(
                                    '/',
                                    strServerversion
                                )
                            )(2)
                        )[0:2]
                    )
                )
                if (oSchema is None):                           # we have the version!
                    oSchema = SchemaXML(version)                # create an instance of SchemaXML
            except Exception as err:
                print(err)
        (
            self.oSchema,
            self.version
        ) = \
            (
                oSchema,
                version
            )

        ''' help info
        '''
        self.oHelp = DLGHelp(self)
        self.usage_items = self.usagelist()
        self.commands = self.tables = self.tablenames = self.get_allcmds()
        self.valid_globals = self.get_validglobals()
        self.recordchunks = kwargs.recordchunks or 1500
        self.maxrows = kwargs.maxrows or 0
        configpairs = self.p4configs()
        for configkey in configpairs.keys():
            attname = f'_{configkey}' \
                if (re.match(r'^_[az]?$', configkey) is None) \
                else configkey
            if (not configkey in self.p4globals):
                setattr(self, attname, configpairs[configkey])

        self.recCounter = Storage(
                                    {
                                        'threshhold': self.recordchunks,
                                        'recordcounter': 0
                                    }
        )
        self.oSchemaType = SchemaType(self)

    def __call__(self, query=None, *options, **kwargs):
        if AND(
                (len(options) + len(kwargs) == 0),
                (query is None)
        ):
            return self
        kwargs = Storage(kwargs)
        (
            tablename,
            lastarg,
            tabledata,
            reference,
        ) \
            = (
            kwargs.tablename,
            None,
            Storage(),
            None,
        )

        if (tablename is None):
            try:
                tablename = self.tablename
            except: pass

        (
            p4Queries,
            options,
            qries
        ) = \
            (
                Lst(),
                Lst(options),
                Lst()
            )

        if (query is not None):
            if (isinstance(query, (list, Lst, tuple)) is False):
                if (query_is_reference(query) is True):
                    reference = query
                    reference.flat = kwargs.flat or False
                    ''' re-define query as the query's left 
                        side's Table object (_table)
                    '''
                    query = reference.left._table
                    tablename = query.tablename
                    tabledata = self.memoizetable(tablename)
                elif (isinstance(query, Py4Table)):
                    tablename = query.tablename
                    tabledata = tabledata.merge(query.__dict__).delete('objp4')
                else:
                    if (is_query_or_expressionType(query) is True):
                        qries = Lst([query])
                    elif (isinstance(query, str)):
                        qries = objectify(Lst(queryStringToStorage(q) for q in query.split()).clean())
                    elif OR(
                            (isinstance(query, dict)),
                            (type(query) is LambdaType)
                    ):
                        qries = objectify(Lst([query]))
            else:
                qries = objectify(Lst(query))

            for qry in qries:
                if (qry.inversion is None):
                    qry.inversdion = False
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
                qry = invert(qry)#, inversion=inversion)
                if (tablename is None):
                    ''' grab the tablename and go!
                    '''

                    (
                        q,                  # the query, though it might have been altered
                        left,               # the left side of the query (or of the left side of 2 queries)
                        right,              # the right side of the query (or of the right side of 2 queries)
                        op,                 # the operator
                        tablename,          # the name of the target table
                        options,            # cmd line options, etc.
                        lastarg,            # the lastarg (AKA a field)
                        inversion,          # bool -> if True, invert the query's result
                        specifier,          # revision specifier can be `#`, `@`
                        specifier_value,    # the file's rev or changelist number
                        tabledata           # useful data about this table
                    ) \
                        = self.breakdown_query(qry, *options, **tabledata)
                p4Queries.append(qry)

        if (noneempty(tabledata) is True):
            tabledata = self.memoizetable(tablename)
        (
            options,
            globaloptions,
            cmdoptions
        ) = \
            (
            Lst(options),                   # neither globaloptions, neither cmdoptions
            kwargs.globaloptions or Lst(),  # an alternate place to stash a few p4 globals
            kwargs.cmdoptions or Lst(),     # xtra cmd options
        )
        ''' Have we picked up any queries?
        '''
        if (len(p4Queries) > 0):
            if (not tablename in self.tablememo.keys()):
                self.memoizetable(tablename)
            ''' lastarg (| filename | specname)
            '''
            if (lastarg is None):
                for item in (
                        'filename',
                        'specname',
                        'lastarg'
                ):
                    if (kwargs[item] is not None):
                        lastarg = kwargs[item]
                        break
                if (lastarg is None):
                    (lastarg, options) = self.define_lastarg(tablename, *options, query=p4Queries)
            if AND(
                    (isinstance(lastarg, str) is True),
                    (not lastarg in options)
            ):
                options.append(lastarg)
        ''' No p4Queries, a straight up p4 cmd (with or without options)

                e.g.
                >>> oP4.files('-a', '//depot/my_projects/project/...lin64...')

            __call__ accepts any p4 global argument.

            P4 Syntax:
                >>> p4 [global options] command [cmd options] [arg/lastarg ...]

                executable                                                    command
                    |                           `                               |
                >>> p4 -u mart -P martspassword -p localhost:1666 -c clientname files //depot/...
                       |                                                      |         |
                       |_________________global arguments >___________________|       arg/lastarg

            Generally, global args are passed in and defined when we instantiate 
            a reference to class Py4. I.e.:

                >>> objp4 = Py4(*args, **{'user': 'mart',
                                          'password': 'martspassword',
                                          'port': 'localhost:1666',
                                          'client': 'defaultclient'})

            Globals persist so long as the class reference exists (regardless of any 
            environmental interference).

            P4 Globals as args to __call__:
            they persist only for the life of this cmd, after which they will be dropped.

                I.e.    tablename = None                <-- name of the current cmd call
                        is_spec = False                 <-- True, cmd is a spec
                        closed = True                   <-- True until cmd has completed execution
                        p4globals = ['p4', '-G']        <-- hard coded
                        credentialglobals = [**kwargs]  <-- those passed in on instantiation, these persist
                        supglobals = []                 <-- dropped
        '''
        ''' add any global options to supglobals for temporary use
        '''
        self.supglobals += globaloptions
        self.p4args += options
        ''' just in case p4args has a lastarg arg AND in the wrong position, 
            force it the last position.
        '''
        if AND(
                (lastarg is not None),
                (isinstance(lastarg, str) is True)
        ):
            if (lastarg in self.p4args):
                self.p4args.pop(self.p4args.index(lastarg))
                self.p4args.append(lastarg)
            elif (lastarg not in self.p4args):
                self.p4args.append(lastarg)
        ''' these keywords are unrelated to cmd fields, get them out of the way!
        '''
        deloptions = (
            'options',
            'cmdoptions',
            'globaloptions',
            'lastarg'
        )
        kwargs.delete(*deloptions)
        ''' Any kwargs left? they must be valid table (cmd) options! 

            eg. at the cmd line, we would type something like the following should 
                we want to preview (-n) a retype operation (-t <type>). 

                    e.g.
                    >>> p4 edit --preview/-n --filetype/-t ktext myfilename

                A p4 command's cmdline options can be either specified in the same 
                way as we typically would (except here we pass them in as parameters)

                    >>> oP4.edit('-n', '-t', 'ktext' 'myfilename')

                Another way would be to pass the cmd line options as **kwargs for 
                the __call__ method

                    >>> (oP4(**{'preview': True, 'filetype': 'ktext'}).edit('myfilename')

                    *note: options that don't typically have corresponding values (like
                           '--preview' or '-n') can be set by providing a boolean value 
                           to the key.  
        '''

        ''' user defined configs to apply on p4Queries (piggyback on tabledata):       

            compute                 --> new columns 
                                        |-> single `;` separated string. eg. colname=value
                                        |-> or a list of strings. ['colname1=value1, 'colname2=value2,] 
            maxrows                 --> max # of records to process (default is 1000) 
        '''
        tabledata.merge(
            {
                'compute': kwargs.compute or '',
                'maxrows': self.maxrows,
                'tabletype': Py4,   # set apart from the JNLFile type, given that Py4 has no schema to guide it
                'tablename': tablename,
            }
        )
        delkwargs = (
            'compute',
            'maxrows'
        )
        kwargs.delete(*delkwargs)
        ''' Py4IO's callable returns a set of records (P4QRecordset). 
        '''
        [
            setattr(self, item, kwargs[item]) for item in
            ('maxrows', 'compute') if (kwargs[item] is not None)
        ]
        ''' Define a DLGRecordSet based on the result of running 
            a p4 cmd (Py4Run), or an empty RecordSet if tablename
            happens to be None (in which case, we will rely on the
            DLGRecordSet methods being invoked by the user.
        '''
        oRecordSet = DLGRecordSet(self, DLGRecords(), **tabledata) \
            if (tablename is None) \
            else DLGRecordSet(
            self,
            Py4Run(
                self,
                *cmdoptions,
                **tabledata
            ),
            **tabledata
        )
        ''' depending on the values we have so far for query, 
            p4Queries and reference, return the RecordSet (
            swinging by __call__ if need be).  
        '''
        if AND(
                (len(p4Queries) == 0),
                (is_tableType(query) is True)
        ):
            if (reference is None):
                return oRecordSet
            return oRecordSet(
                reference=reference
            )
        return oRecordSet(*p4Queries, **kwargs)

    ''' 
    +-----------------+------------------------------------------------+
    | Dumb-Query Type | Query Statement                                |
    +=================+================================================+
    | recQuery        | query = lambda record: record.user=='mart'     |
    +-----------------+------------------------------------------------+
    | funcQuery       | query = lambda record: EQ(record.user, 'mart') |
    +-----------------+------------------------------------------------+
    | strQuery        | query = "change.user=mart"                     |
    +-----------------+------------------------------------------------+
    | attQuery        | query = oJnl.change.user == 'mart'             |
    +-----------------+------------------------------------------------+

    def formatQuery(self, qry, qtype=None):
        if (qtype is None):
            qtype = self.queryformat
        if (qtype == 'strQuery'):
            if (hasattr(qry, 'left') is True):
                objname = qry.left.tablename
                fieldname = qry.left.fieldname
                value = qry.right
                op = qry.op
                qry = (f"{objname}.{fieldname}{op}{value}") \
                    if (objname is not None) \
                    else (f"{fieldname}{op}{value}")
        elif (qtype == 'funcQuery'):
            opfunc = optable(qry.op)[0]
            field = qry.left.fieldname
            value = qry.right
            qry = lambda record: opfunc(record[field], value)
        elif (qtype == "recQuery"):
            field = qry.left.fieldname
            value = qry.right
            qry = (lambda record: record[field] == value)
        return qry
    '''

    ''' Create p4globals for our user from this system's environment (P4USER, etc.)
    '''
    def p4configs(self):
        ''' a minimalist tabledata with default values for the benefit of PyRun
        '''
        cdata = {
                 'is_specs': False,
                 'is_spec': False,
                 'fieldnames': [],
                 'is_info': False,
                 'tablename': 'set',
                 'is_help': False,
                 'fieldsmap': {},
                 'is_command': True,
                 'is_explain': False,
                 'logger': self.logger
                }
        envfilenames = [
                        'config',
                        'ignore',
                        'tickets'
        ]
        envvariables = Storage()
        p4UserConfig = Py4Run(self, **cdata)()
        if (type(p4UserConfig).__name__ == 'DLGRecords'):
            p4UserConfig = p4UserConfig(0).data
        if (len(p4UserConfig) > 0):
            for line in re.split('\n', p4UserConfig):
                if (line.startswith('4')):
                    line = f'P{line}'
                if (reg_envvariable.match(line) is not None):
                    (key, value) = re.sub('P4', '', line).lower().split('=')
                    if (not key in envfilenames + self.user_defined_globals):
                        value = re.split('\s', value)[0]
                        if (noneempty(value) is False):
                            envvariables.update(**{key: value})
        return envvariables

    def define_p4globals(self, **kwargs):
        kwargs = Storage(kwargs)
        res = set()
        RSH_PORT_Error = "Can not define RSH port because the RSH string could not be built"
        RSH_MISSING_ROOT_ERROR = "Can not define RSH port, `p4droot` is required!"
        for (key, value) in kwargs.items():
            if (
                    not key in [
                        'oSchema',
                        'p4droot',
                        'release'
                    ]
            ):
                for item in ('-', 'p4'):
                    key = re.sub(item, '', key)
                if (key == 'password'):
                    if (
                            value in (
                            '',
                            None,
                            'Unset',
                            'unset'
                            )
                    ):
                        kwargs.pop(key)
                    res.add(key)
                elif (key == 'port'):
                    ''' set a RSH port 
                        
                        Note that `p4droot` (the path that leads to the p4d executable) is required. 
                    '''
                    p4droot = kwargs.p4droot
                    if (value in ('localport', 'rsh')):
                        res.add(key)
                        if (p4droot is not None):
                            ''' time to build the port
                            '''
                            value = set_localport(p4droot)
                            if (value in ('unset', None)):
                                bail(RSH_PORT_Error)
                            else:
                                res.add('p4droot')
                        else:
                            bail(RSH_MISSING_ROOT_ERROR)
                    if (re.match(r'^rsh:.*-+.*$', value) is not None):
                        oPort = Popen(['p4', '-p', value], stdout=PIPE, stderr=PIPE).stdout
                        try:
                            ''' make sure the RSH port is valid
                            '''
                            strRsh = oPort.read()
                            if (len(strRsh) == 0):
                                bail(f"No such port: {value}")
                        finally:
                            oPort.close()
                    if (p4droot is not None):
                        res.add('p4droot')
                self.user_defined_globals.append(key)
                setattr(self, f'_{key}', value)
                self.p4credential_globals += [
                    f"-{key}" \
                        if (len(key) == 1) \
                        else f"--{key}", value]
        return res

    def pluralize_specs(self, exclude=[]):
        specs = Lst(self.p4spec).diff(exclude)
        pSpecs = Lst(
            self.oPlural.pluralize(specitem) for specitem in
            specs if (self.oPlural.is_plural(specitem) is False)
        )
        return pSpecs

    ''' probably no longer needed
    '''
    def set_table(self, tablename):
        tabledata = self.memoizetable(tablename)
        oRun = Py4Run(self, **tabledata)
        oCmdTable = Py4Table(
            self,
            tablename,
            oRun,
            **tabledata
        )
        Py4Table(self, tablename, oRun, **tabledata)
        setattr(self, tablename, oCmdTable)

    def __getitem__(self, key):
        value = self.__dict__.get(str(key))
        return value or self.__getattr__(str(key))

    def __getattr__(self, tablename):
        valid_tablenames = self.commands.diff(self.nocommands)
        #invalidAttributeError = f'{tablename} is not a valid attribute or tablename'
        ''' Strange IPython attributes. 
        '''
        if (len(annoying_ipython_attributes(tablename)) > 0):
            return
        while True:
            if (tablename in table_alias):
                tablename = table_alias[tablename]
            if (tablename == 'explain'):
                try:
                    return self.p4OutPut_noCommands(tablename, *self.p4globals + ['--explain'])
                except Exception as err:
                    self.logerror(err)
                    break
            elif OR(
                    OR(
                        (tablename in self.commands + self.initcommands),
                        ('--explain' in self.p4args)),
                    (not '-G' in self.p4globals)
            ):
                try:
                    return self.__dict__[tablename]
                except:
                    tabledata = self.memoizetable(tablename)
                    if (tabledata.error is not None):
                        bail(tabledata.error)
                    oRun = Py4Run(self, **tabledata)
                    oCmdTable = Py4Table(
                        self,
                        tablename,
                        oRun,
                        **tabledata
                    )
                    setattr(self, tablename, oCmdTable)
            elif (tablename in self.usage_items):
                try:
                    usageinfo = self.helpmemo[tablename]
                except KeyError:
                    usageinfo = self.helpmemo[tablename] = self.help(tablename)
                return usageinfo
            elif AND(
                        (len(valid_tablenames) > 0),
                    (tablename not in valid_tablenames)
            ):
                #self.logerror(invalidAttributeError)
                break
            try:
                if (hasattr(self, tablename)):
                    return getattr(self, tablename)
                elif (tablename in self.tablememo.keys()):
                    return self.tablememo[tablename]
                #else:
                #    self.logerror(invalidAttributeError)
            except (KeyError, AttributeError):
                #self.logerror(invalidAttributeError)
                break

    def truncate_table(self, tablename):
        try:
            delattr(self, tablename)
            self.tablememo.pop(tablename)
            self.loginfo(f'truncated table `{tablename}` from instance.')
        except Exception as err:
            self.logwarning(err)

    def define_lastarg(self, tablename, *cmdargs, **kwargs):
        if (tablename is None):
            return (None, None)
        (cmdargs, kwargs) = (Lst(cmdargs), Storage(kwargs))
        query = kwargs.query
        if (query is not None):
            query = query(0) \
                if (isinstance(query, Lst)) \
                else query.left \
                if (type(query.left) is DLGQuery) \
                else query

        lastarg = None
        tabledata = self.memoizetable(tablename)

        if OR(
                (tablename in self.nocommands),
                (noneempty(tabledata.tableoptions) is True)
        ):
            return (None, None)

        usage = tabledata.tableoptions.usage
        requires_filearg = (reg_filename.search(usage) is not None)

        if (requires_filearg is False):
            if (not tablename in self.spec_takes_no_lastarg):
                is_spec = True if (tablename in self.p4spec) else False
                if (is_spec is True):
                    altarg = tabledata.altarg
                    if (altarg is not None):
                        if (altarg.lower() in LOWER(kwargs.getkeys())):
                            altarg = tabledata.fieldsmap[altarg.lower()]
                            lastarg = kwargs[altarg]
                        elif (query is not None):
                            if AND(
                                    (isinstance(query.right, str)),
                                    (query.left.fieldname.lower() == altarg.lower())
                            ):
                                lastarg = query.right
                    arg = getattr(self, tablename)
                    if (len(cmdargs) > 0):
                        lastarg = cmdargs.pop(-1)
                    if (lastarg is None):
                        if (arg is not None):
                            lastarg = arg
                return (lastarg, cmdargs)
        else:
            ''' filearg is required!

                search priority:
                    1) cmdargs
                    2) check kwargs.queries as they may contain a clue about a specified lastarg
                    3) otherwise, grab the user's client View  (//CLIENT_NAME/...)              
            '''
            if (cmdargs(-1) is not None):
                if (isanyfile(cmdargs(-1)) is True):
                    lastarg = cmdargs.pop(-1)
                    ''' stop looking, we  have it!
                        * at least we have a file/dir on the local FS, 
                            - or we have a clientFile 
                            - or we have a depotFile
                    '''
                    return (lastarg, cmdargs)
            ''' priorities 1 & 2
            '''
            if (len(cmdargs) > 0):
                fileitem = cmdargs.pop(-1)
                if (fileitem is not None):
                    if (isanyfile(fileitem) is True):
                        return (fileitem, cmdargs)
            ''' priority 3
            '''
            if (isinstance(query, str) is True):
                if (isanyfile(query.right) is True):
                    if (query.left.fieldname.lower() in (
                            'depotfile',
                            'clientfile',
                            'path'
                    )
                    ):
                        if (query.op in (
                                EQ,
                                NE,
                                '=',
                                '!='
                            )
                        ):
                            p4args = self.p4globals + ['where', query.right]
                            out = self.p4OutPut(tablename, *p4args)
                            lastarg = out.depotFile
                    else:
                        lastarg = None
            ''' lastly... use the clientFile
            '''
            if (noneempty(lastarg) is True):
                lastarg = f'//{self._client}/...'
            return (lastarg, cmdargs)

    '''     notes and query usage

            arguments: 1. query=None     --> a query, a list of queries string, dict, object
                       2. *[options,]

                       Qry1 = oP4.files.depotFile == '//dev/projects/projname/release/...'
                       Qry2 = oP4.files.depotFile.contains('release') 
                       Qry3 = 'files.depotFile#release'

                       Qry = (Qry1, Qry2)

                       oP4(Qry, *[], filename='//fifa/dev/ml/...')
                       Note: has an experimental filename guesser (still needs work/testing though) 
    '''

    def get_recordsIterator(self):
        return enumerate(self.records, start=1) \
                if (type(self.records) is not enumerate) \
                else self.records

    def get_sourcefiles(self, sourceFile):
        ''' unused... but why?
            TODO: figure it out!
        '''
        def get_source(sourcefile):
            args = ['print', sourcefile]
            cmdargs = self.objp4.p4globals + args
            out = Lst(self.objp4.p4OutPut('print', *cmdargs))
            metadata = Storage(out(0))
            if (len(out) == 2):
                source = out(1).data
            else:
                source = ''
                for idx in range(1, len(out)):
                    source += out(idx).data
            return (metadata, source)
        sources = []
        sourceFiles = []
        if (sourceFile is not None):
            query = list(self.query) \
                if not (isinstance(self.query, list)) \
                else self.query
            for q in query:
                specifiers = []
                if OR(
                        (isinstance(q.right, Storage)),
                        (q.right.__name__ == fieldType(self.objp4))
                ):
                    q = q.left
                if (q.right == sourceFile):
                    specifier = q.left.specifier
                    specifier_value = q.left.specifier_value
                    if (noneempty(specifier) is False):
                        if (',' in specifier_value):
                            [specifiers.append(specifier_item) for
                             specifier_item in specifier_value]
                        else:
                            specifiers.append(specifier_value)
                        for spcf in specifiers:
                            specifier_appended = ''.join(
                                [
                                    specifier,
                                    spcf
                                ]
                            )
                            sourceFiles.append(''.join(
                                [
                                    sourceFile,
                                    specifier_appended
                                    ]
                                )
                            )
                    break
            [sources.append(get_source(srcfile)) for srcfile in sourceFiles]
        return sources

    def get_tablename_fieldname_from_qry(self, qry):
        ''' Like self.breakdown_query(qry) but
            returns only tablename & fieldname.
        '''
        def getleft(q):
            (
                op,
                left,
                right,
                inversion
            ) = \
                (
                    q.op,
                    q.left,
                    q.right,
                    q.inversion
            )
            opname = op.__name__ \
                if (callable(op) is True) \
                else op
            if OR(
                  AND(
                      (hasattr(left, 'left') is True),
                      (is_query_or_expressionType(left.left) is True)
                  ),
                    (opname in (andops + orops + xorops))
            ):
                left = getleft(left)
            return left
        left = getleft(qry)
        if (left.tablename is not None):
            return (left.tablename, left.fieldname)

    def breakdown_query(self, qry, *options, **tabledata):
        options=Lst(options)
        (
            op,
            left,
            right,
            inversion
        ) \
            = (
                qry.op,
                qry.left,
                qry.right,
                qry.inversion or False
        )
        try:
            (
                tablename,
                fieldname
            ) = \
                (
                    left.tablename,
                    left.fieldname
                )
        except:
            (
                tablename,
                fieldname
            ) = \
                (
                    None,
                    None
                )
        (
            lastarg,
            specifier,
            specifier_value
        ) = \
            (
                None,
                None,
                None
        )
        opname = op.__name__ \
            if (callable(op) is True) \
            else op
        if (opname in (andops + orops + xorops + notops)):
            (
                q,
                left,
                right,
                op,
                tablename,
                options,
                lastarg,
                inversion,
                specifier,
                specifier_value,
                tabledata
            ) = self.breakdown_query(left, *options, **tabledata)
        if (isnum(right) is True):
            right = str(right)
        elif (right is None):
            if (is_queryType(left) is True):
                qry = left
        if (is_tableType(qry) is True):
            return (
                    qry,
                    left,
                    right,
                    op,
                    tablename,
                    options,
                    lastarg,
                    inversion,
                    specifier,
                    specifier_value,
                    tabledata
            )
        if (None in (tablename, fieldname)):
            if (hasattr(left, 'tablename')):
                if (left.tablename is not None):
                    (
                        tablename,
                        fieldname
                    ) = \
                        (
                            left.tablename,
                            left.fieldname
                        )
            elif OR(
                    OR(
                        (is_queryType(left) is True),
                        (is_expressionType(left) is True)
                    ),
                (is_fieldType(left) is True)
            ):
                (
                    tablename,
                    fieldname
                ) = (
                    self.get_tablename_fieldname_from_qry(qry)
                )
        if (isinstance(right, str)):
            if AND(
                    (isdepotfile(right) is True),
                    (tablename is not None)
            ):
                (lastarg, options) = self.define_lastarg(tablename, *options, query=qry)
        if (tablename is None):
            tablename = qry.tablename
        if (noneempty(tabledata) is True):
            if (tablename is not None):
                tabledata = self.memoizetable(tablename)
        if (fieldname is not None):
            if (fieldname.lower() not in tabledata.fieldsmap.getkeys()):
                bail(
                    f"Fieldname '{fieldname}' does not belong to Py4Table '{tablename}'.\n\
                Select among the following fieldnames:\n{tabledata.fieldnames}\n"
                )
        if (lastarg is None):
            qkwargs = {}
            if (qry is not None):
                qkwargs = {'query': qry}
            (lastarg, options) = self.define_lastarg(tablename, *options, **qkwargs)
        if (lastarg is not None):
            ''' is a rev | changelist specified in qry.right?
            '''
            if (isinstance(right, str) is True):
                if (reg_rev_change_specifier.match(right) is not None):
                    for item in ('#', '@'):
                        right_bits = re.split(item, right, maxsplit=1)
                        if (len(right_bits) == 2):
                            qry.right = right_bits[0]
                            specifier = item
                            specifier_value = right_bits[1]
                            ''' the query's q.right value cannot contain any
                                revision specifiers because the value will
                                be validated against the record's own value 
                                thereby considering the record as bening 
                                non-matching.
                            '''
                            if (isinstance(qry.left, Py4Field)):
                                for (name, val) in {
                                                    'specifier': specifier,
                                                    'specifier_value': specifier_value
                                }:
                                    setattr(qry.left, name, val)
                                ''' if not #head, try to use relative revision specifiers instead!
                                '''
                            break
            """ 
            if (specifier is not None):
                for fldname in ('rev', 'change', 'revision', 'changelist'):
                    if (fldname in self.tablememo[tablename].fieldsmap.getkeys()):
                        ''' might need a relative operator
                        '''
                        if (re.search(',', specifier_value) is not None):
                            ''' we have a range!
                            '''
                            (s, e) = specifier_value.split(',')
                            
                        '''    
                        s: >2   e: <4
                        s: op must be > (GT)                        
                        s: right must be 2

                        e:  <4
                        e: op must be < (LT)                        
                        e: right must be 4
                        '''

                        p4table = getattr(self, tablename)
                        p4field = getattr(p4table, fldname)
                        right_q = (p4field == specifier_value)
                        right_q = right_q.as_dict()
                        q = AND((q), (right_q))
                        break
                        """
        return (
                qry,
                left,
                right,
                op,
                tablename,
                options,
                lastarg,
                inversion,
                specifier,          # can be `#`, `@`
                specifier_value,    # is either a file's rev or changelist number
                tabledata
            )

    def close(self):
        (
            self.is_spec,
            self.is_specs,
            self.is_command,
            self.is_help,
            self.is_explain,
            self.is_info
        ) \
            = (
                False,
                False,
                False,
                False,
                False,
                False
        )

        (
            self.supglobals,
            self.p4globals,
            self.p4args
        ) \
            = (
                Lst(),
                Lst(['p4', '-G'] + self.p4credential_globals),
                Lst()
        )

    def parseInputKeys(self, tabledata, specname, **specinput):
        specinput = Storage(specinput)
        if (type(specname).__name__ == 'Py4Table'):
            specname = specname.tablename
        try:
            altarg = tabledata.altarg
            speckeys = specinput.getkeys()
            for speckey in speckeys:
                if (speckey.lower() in tabledata.fieldsmap.getkeys()):
                    if AND(
                            (specname is None),
                            (speckey.lower() == altarg)
                    ):
                        specname = specinput[speckey]
                    rspeckey = tabledata.fieldsmap[speckey.lower()]
                    if (rspeckey != speckey):
                        specinput.rename(speckey, rspeckey)
                elif (specname in specinput.getvalues()):
                    for (key, value) in specinput.items():
                        if (value == specname):
                            speckey = tabledata.fieldsmap[key.lower()]
                            if (key != speckey):
                                specinput.rename(key, speckey)
            if (altarg is not None):
                if (specname is None):
                    specname = specinput[altarg] or specinput[altarg.lower()]
                #if (specname is not None):
                #    specinput[tabledata.fieldsmap[altarg.lower()]] = specname
            self.loginfo(f'parsing input for spec {specname}: {specinput}')
            return (specname, specinput, altarg)
        except Exception as err:
            self.logerror(err)
            return (None, None, None)

    def p4OutPut_noCommands(self, tablename, *p4args, **kwargs):
        (
            p4args,
            kwargs,
            records,
            EOR
        ) = \
            (
                Lst(p4args),
                Storage(kwargs),
                Lst(),
                False
        )
        if (not '--explain' in p4args):
            (lastarg, p4args) = self.define_lastarg(tablename, *p4args)
            if isinstance(lastarg, str):
                p4args.append(lastarg)
        if (
                OR(
                    OR(
                        (tablename in self.nocommands),
                        ('--explain' in p4args)
                    ),
                        (not '-G' in self.p4globals)
                )
        ):
            p4opener = Popen(p4args, stdout=PIPE, stderr=PIPE)
            try:
                if (hasattr(p4opener, 'read') is True):
                    out = p4opener.read()
                elif (hasattr(p4opener, 'stdout')):
                    (outFile, errFile) = p4opener.communicate()
                    out = outFile \
                        if (len(outFile) > 0) \
                        else errFile
                else:
                    outFile = p4opener.stdout
                    out = outFile.read()
                out = decode_bytes(out)
                return out
            finally:
                ''' this is bad! please fix! '''
                if AND(
                        (hasattr(p4opener, 'close') is True),
                        (hasattr(p4opener, 'closed') is True)
                ):
                    if (p4opener.closed is False):
                        p4opener.close()

    def p4OutPut(self, tablename, *p4args, **kwargs):
        (
            p4args,
            kwargs,
            records,
            EOR
        ) = \
            (
                Lst(p4args),
                Storage(kwargs),
                Lst(),
                False
        )
        ''' remember, it is entirely possible that p4args be empty...
        '''
        if AND(
                (len(p4args) > 0),
                (not '--explain' in p4args)
        ):
            if AND(
                    (kwargs.lastarg is not None),
                    (kwargs.lastarg != p4args(-1))
            ):
                (specname, p4args) = self.define_lastarg(tablename, *p4args)
                if AND(
                        (isinstance(specname, str) is True),
                        (specname != p4args[-1])
                ):
                    p4args.append(specname)
        ''' process the thing
        '''
        oFile = Popen(p4args, stdout=PIPE, stderr=PIPE).stdout
        loader = mload \
            if (hasattr(oFile, 'read')) \
            else mloads
        while EOR is False:
            ''' Time to get out! haha
            '''
            try:
                out = objectify(loader(oFile))
                if (isinstance(out, Lst) is True):
                    if (isinstance(out(0), tuple) is True):
                        out = Storage(out)
                out = decode_bytes(out)
                records.append(out)
            except (StopIteration, EOFError):
                EOR = True
            except ValueError as err:
                output = oFile.read() \
                    if (hasattr(oFile, 'read')) \
                    else oFile
                output = decode_bytes(output)
                records.append(output)
            except Exception as err:
                self.logger.error(f"err: {err}")
        ''' all done, close the file descriptor
        '''
        if AND(
                (hasattr(oFile, 'close') is True),
                (oFile.closed is False)
        ):
            oFile.close()
        self.close()
        if (len(records) > 0):
            if (isinstance(records(0), str)):
                return records(0)
            return records
        tabledata = self.memoizetable(tablename)
        return DLGRecords(
            records=Lst(rec for rec in records),
            objp4=self,
            **tabledata
        )

    def p4Input(self, tablename, *p4args, **specinput):
        (
            p4args,
            specinput
        ) \
            = \
            (
                Lst(p4args),
                Storage(specinput)
            )
        objFile = Popen(
            p4args,
            stdout=PIPE,
            stdin=PIPE,
            stderr=STDOUT
        )
        loader = mloads
        if ('-G' in p4args):
            if (hasattr(objFile, 'read')):
                loader = mload
            specinput.delete(*[
                item for item in (
                    'code',
                    'Access',
                    'Update',
                    'Date',
                    'Suffix'
                    )
                ]
            )
            specinput = decode_bytes(specinput)
            try:
                specinput = mdumps(dict(specinput), 0)
            except ValueError as err:
                bail(err)
        else:
            specinput = specinput.input
        try:
            out = objFile.communicate(input=specinput)
            objOutput = Lst(out)(0)
            if (is_marshal(objOutput) is True):
                out = loader(objOutput)
                if (isinstance(out, list)):
                    out = Lst(out)
                    out = dict(out(0)) \
                        if (isinstance(out(0), (list, tuple))) \
                        else dict(out)
                if OR(
                        (isinstance(out, dict)),
                        (
                            set(
                                map(
                                    type,
                                    out
                                )
                            ) == {bytes}
                        )
                    ):
                    out = decode_bytes(out)
            return Flatten(**Storage(out)).reduce()
        finally:
            if (hasattr(objFile, 'close')):
                objFile.close()

    def memoizetable(self, tablename):
        try:
            tabledata = self.tablememo[tablename]
        except KeyError:
            (
                altarg,
                spectype,
                tablename,
                specfield,
                specmap,
                fieldsmap,
                fieldnames,
                keying,
                options,
                keywords,
                oOptions,
                usage
            ) = \
                (
                    None,
                    None,
                    tablename,
                    None,
                    Storage(),
                    Storage(),
                    Lst(),
                    Lst(),
                    Storage(),
                    Lst(),
                    Storage(),
                    ''
                )
            tabledata = self.tablememo[tablename] = objectify(
                {
                    'tablename':    tablename,
                    'is_spec':      (tablename in self.p4spec),
                    'is_specs':     (tablename in self.p4specs),
                    'is_command':   (tablename in self.commands.diff(self.p4spec + self.p4specs)),
                    'is_help':      (tablename == 'help'),
                    'is_set':       (tablename == 'set'),
                    'is_explain':   (tablename == 'explain'),
                    'is_info':      (tablename == 'info'),
                    'fieldsmap':    fieldsmap,
                    'fieldnames':   fieldnames,
                    'keying':       keying,
                    '_rname':       tablename,
                }
            )
            tabledata.spectype = tablename \
                if (tabledata.is_spec is True) \
                else None
            oOptions = Py4Options(self, tablename, **tabledata)()
            optionsdata = oOptions.optionsdata
            for tditem in ('fieldnames', 'fieldsmap'):
                tdvalue = optionsdata.pop(tditem)
                ''' since perforce has keyed tables, and field name ID is actually 
                    used for some command fields, we can resort to a record's index
                    as a fake id (at run time only and while the Table reference exists
                '''
                if (len(tdvalue) > 0):
                    if (tditem == 'fieldnames'):
                        tdvalue.insert(0, 'idx')
                    elif (not 'idx' in tdvalue.getkeys()):
                        tdvalue.merge({'idx': 'idx'})
                elif AND(
                            (tditem == 'fieldsmap'),
                            (len(tabledata.fieldnames) > 0)
                ):
                    tdvalue = Storage(
                        zip(
                            [
                                fname.lower() for fname in fieldnames
                            ],
                            fieldnames
                        )
                    )
                    if (not 'idx' in tdvalue.getkeys()):
                        tdvalue.merge({'idx': 'idx'})
                tabledata.merge({tditem: tdvalue})
            ''' fieldnames & fieldsmap is taken care of, time to 
                pillage optionsdata for anything left that might 
                be useful
            '''
            for (okey, ovalue) in optionsdata.items():
                if (not okey in tabledata.getkeys()):
                    tabledata.merge({okey:optionsdata[okey]})
                elif AND(
                        (noneempty(ovalue) is False),
                        (noneempty(tabledata[okey]) is True)
                ):
                    tabledata.merge({okey: ovalue})
            self.loginfo(f'p4table memoized: {tablename}')
        return tabledata

    def get_validglobals(self):
        ''' Ask p4 for a list of valid cmd line globals
        '''
        validglobals = Lst()
        for line in re.split('\n', self.explain):
            if (reg_p4global.match(line) is not None):
                items = Lst(re.split(':', line))
                item = items(0).strip()
                description = items(1).strip()
                try:
                    (kwarg, arg) = re.split(' \(', item)
                    arg = re.sub('\)', '', arg)
                except:
                    (kwarg, arg) = (item, None)
                argitems = (kwarg, arg, description)
                validglobals.append(argitems)
        return validglobals

    def helpusage(self):
        return self.oHelp.helpusage()

    def usagelist(self):
        return self.oHelp.usagelist()

    def commandslist(self):
        return self.oHelp.commandslist()

    def administrationlist(self):
        return self.oHelp.administrationlist()

    def get_allcmds(self):
        return self.oHelp.get_allcmds()

    def parseundoc(self):
        return self.oHelp.parseundoc()

def p4connector(*args, **kwargs):
    try:
        return Py4(**kwargs)
    except Exception as err:
        print(err)