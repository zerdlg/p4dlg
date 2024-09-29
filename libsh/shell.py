import sys
from pprint import pprint
from datetime import datetime, time
from importlib import __import__
from socket import gethostname
try:
    import cPickle as pickle
except ImportError:
    import pickle
import csv
import calendar
from subprocess import check_output
import math
import random
from pprint import pformat
from io import StringIO
from marshal import load as mload
from marshal import loads as mloads
from marshal import dump as mdump
from marshal import dumps as mdumps
from lz4 import frame as lz4frame
from lz4 import block as lz4block

''' qtconsole imports
'''
from qtconsole.rich_jupyter_widget import *
from qtconsole.inprocess import QtInProcessKernelManager
from IPython.lib import guisupport
# from IPython.core.debugger import Pdb
''' p4dlg import
'''
from libdlg import *
from libjnl.jnlInit import JnlInitialize
from libsh.shVars import *
''' connectors to objects P4, JNL, DB & NO
'''
from libconnect import (
    ObjP4,
    ObjP4db,
    ObjJnl,
    ObjDB,
    ObjNO
)

#from libsh.shModules.shQuery import RunQuery

''' absolute path to schemaxml directory
'''
from os.path import dirname
import schemaxml
from resc import journals, db

schemadir = dirname(schemaxml.__file__)
journaldir = dirname(journals.__file__)
projectdir = dirname(schemadir)
dbdir = dirname(db.__file__)
varsdir = os.path.join(
    *[
        projectdir,
        'libsh',
        'Vars'
    ]
)

''' Steph's braider program
'''
# from resc.p4_braider import *

__all__ = [
            'Serve', 
            'ServeLib',
            'DLGShell'
]
mdata = """
        --[$File: //dev/p4dlg/libsh/shell.py $]
        --[$Change: 473 $] 
        --[$Revision: #33 $]
        --[$DateTime: 2024/09/08 08:15:23 $]
        --[$Author: mart $]
        """

(
    dumps,
    loads
) = \
    (
        pickle.dumps,
        pickle.loads
    )
(
    now,
    numregex,
    localhostname
) = \
    (
        datetime.now,
        re.compile('\d+$'),
        gethostname()
    )

''' p4 -F %depotFile% describe -s 210 | p4 -F %depotFile% -x - grep -e bananas
'''
def createdir(directory):
    try:
        os.mkdir(directory)
    except Exception as err:
        pass

class DLGShell(object):
    cmd_schema = lambda self, version: self.memoize_schema(version)
    cmd_initializeenv = lambda self: self.cmd_initialize_jnlENV()
    cmd_updatelocals = lambda self: self.instlocals()

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls)

    def __call__(self, *args, **kwargs):
        return self

    def cmd_initialize_jnlENV(self):
        oInit = JnlInitialize(schemadir)()
        print("JNL INITIALIZE - Done.")
        return oInit

    def __init__(self, *args, **kwargs):
        self.clsVars = clsVars
        self.shFuncs = Storage()
        self.shClasses = Storage()
        self.locals_cfg = Storage()
        self.schemaMemo = {}

        self.varsdata = objectify(
            {
             'configvars': {'prefix': 'var', 'Vars': {}, 'objvars': None},
             'jnlvars':    {'prefix': 'jnl', 'Vars': {}, 'objvars': None},
             'p4vars':     {'prefix': 'p4', 'Vars': {}, 'objvars': None},
             'p4dbvars':   {'prefix': 'p4db', 'Vars': {}, 'objvars': None},
             'qtvars':     {'prefix': 'qt', 'Vars': {}, 'objvars': None},
             'dbvars':     {'prefix': 'db', 'Vars': {}, 'objvars': None},
             'novars':     {'prefix': 'no', 'Vars': {}, 'objvars': None}
             }
        )

        self.ignoredvars = []
        self.var_projectdir = projectdir
        self.var_journaldir = journaldir 
        self.var_schemadir = schemadir
        self.var_dbdir = dbdir
        self.var_varsdir = varsdir
        self.var_excludelibdirs = [
                                    self.var_schemadir,
                                    self.var_varsdir
        ]
        self.var_libsearchdirs = [
                                    self.var_projectdir
                                  ]
        self.var_dirs_to_create = [
                                    self.var_varsdir,
                                    self.var_journaldir,
                                    self.var_dbdir
        ]

        [createdir(vardir) for vardir in self.var_dirs_to_create]
        [
            self.varsdata[vkey].merge(
                {
                    'path': os.path.join(self.var_varsdir, vkey)
                }
            ) for vkey in self.varsdata.keys()
        ]

        self.shellcmds = objectify({
                                    'cmdcls': {
                                                'prefix': 'cmdcls',
                                                'storesin': 'shClasses'
                                    },
                                    'cmd': {
                                                'prefix': 'cmd',
                                                'storesin': 'shFuncs'
                                    }
                    }
        )
        self.prefixes = [self.varsdata[pkey].prefix for pkey in self.varsdata.getkeys()] \
                      + [self.shellcmds[skey].prefix for skey in self.shellcmds.getkeys()]
        ''' vars for dbconnection
        '''
        self.db_dbroot = f'{projectdir}/dbstorage'
        self.db_dbdir = ''
        self.db_dbname = ''
        self.db_datasource = ''
        self.db_user = ''
        self.db_password = ''
        self.db_host = ''
        self.db_port = ''
        self.db_dlglfilename = 'storage.sqlite'
        self.db_dbengine = 'sqlite'
        self.db_auto_import = False
        self.db_check_reserved = False
        self.db_fake_migrate_all = False
        self.db_lazy_tables = True
        self.db_migrate = True
        self.db_migrate_enabled = True
        self.db_pool_size = 5

        ''' vars for QTConsole (default configs)
            
             default styles:
             ['linux', 'lightbg', 'nocolor']

             line completion can be set to one of the following:
             ['plain', 'droplist', ['ncurses']]

             'kind' define the type of underlying text widget to use
             [['plain'], 'rich']

             valide options for paging
             ['inside', 'hsplit', 'vsplit', 'custom', 'none']        
        '''
        self.qt_set_default_style = 'lightbg'
        self.qt_banner = f"DLGShell {mdata}\n\n"
        self.qt_ansi_codes = True
        self.qt_buffer_size = '1200'
        self.qt_execute_on_complete_input = True
        self.qt_gui_completion = 'droplist'
        self.qt_kind = 'rich'
        self.qt_enable_calltips = True
        self.qt_paging = 'vsplit'
        self.qt_font_family = 'monaco'
        self.qt_font_size = 13
        self.qt_width = 2400
        self.qt_height = 1500
        self.qt_override_shortcuts = False
        self.qt_executable = 'python'
        self.qt_backend = 'matplotlib'
        self.qt_gui = 'qt4'

        ''' populate cmd_ & cmdcls_ attributes
        '''
        self.var_editor = 'vim'
        self._editor = os.environ.get('EDITOR', self.var_editor)
        self.var_operators = Lst(
            '=',
            '>',
            '<',
            '>=',
            '<=',
            '?',
            '@',
            '?',
            '%',
            '#'
        )

        '''      
                    enables add/modify/delete instance variables @runtime, as well
                    as the ability to have these Vars persist from one session to the next

                    populate varsdata.<varsname>.Vars with the names of affected instance
                    variables, so we know not to delete them when a request is given to
                    remove them from scope and to reset them with their default values
        '''
        self.instlocals()

    def cmd_ktxt(self):
        ''' header for new files
        '''
        ktxtwords = ('File', 'Change', 'Revision', 'DateTime', 'Author')
        return '\n'.join([f'\t[${w}  $]' for w in ktxtwords])

    def memoize_schema(self, version=None):
        '''  force version format as release name (r16.2) so
             we don't end up with a memo with duplicate-like
             keys  (r16.2 = 2016.2)
        '''
        if (version is not None):
            version = to_releasename(version)
            try:
                schemamem = self.schemaMemo[version]
            except KeyError:
                if (schemadir is None):
                    bail(f'schemadir is None - cannot retrieve xmlschema version {version}')
                oSchema = SchemaXML(schemadir=schemadir)(version=version)
                schemamem = self.schemaMemo[version] = oSchema
            return schemamem
        else:
            return self.schemaMemo

    ''' set & unset session Vars (will not persist beyond the session's life span)
    '''
    def cmd_unsetvar(self, configname, *args, **kwargs):
        self.clsVars(self, configname).unset(*args, **kwargs)

    def cmd_setvar(self, configname, *args, **kwargs):
        self.clsVars(self, configname).set(*args, **kwargs)

    def validatevars(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        allinstvars = self.shFuncs.getkeys() \
                    + self.shClasses.getkeys() \
                    + self.locals_cfg.getkeys()

        if (len(args) > 1):
            if AND(
                    (args(0) in allinstvars),
                    (args(1) is not None)
            ):
                return False
        for key in kwargs.keys():
            if AND(
                    (key in allinstvars),
                    (kwargs[key] is not None)
            ):
                return False
        return True

    def funcVars(
            self,
            configname,
            *args,
            **kwargs
    ):
        if (self.validatevars(*args, **kwargs) is True):
            try:
                return clsVars(self, configname)(*args, **kwargs)
            except Exception as err:
                print(f"{configname} name conflict: {err}")

    def cmd_configvars(self, *args, **kwargs):
        return self.funcVars(
            'configvars',
            *args,
            **kwargs
        )
    def cmd_p4vars(self, *args, **kwargs):
        return self.funcVars(
            'p4vars',
            *args,
            **kwargs
        )
    def cmd_p4dbvars(self, *args, **kwargs):
        return self.funcVars(
            'p4dbvars',
            *args,
            **kwargs
        )
    def cmd_qtvars(self, *args, **kwargs):
        return self.funcVars(
            'qtvars',
            *args,
            **kwargs
        )
    def cmd_jnlvars(self, *args, **kwargs):
        return self.funcVars(
            'jnlvars',
            *args,
            **kwargs
        )
    def cmd_novars(self, *args, **kwargs):
        return self.funcVars(
            'novars',
            *args,
            **kwargs
        )
    def cmd_dbvars(self, *args, **kwargs):
        return self.funcVars(
            'dbvars',
            *args,
            **kwargs
        )

    def is_prefix(self, cname, pref=None):
        prefixes = [pref] or self.prefixes
        for pfx in prefixes:
            pfmatch = (f'({pfx}_.*)+', cname)
            if (pfmatch is not None):
                return True
        return False

    def instlocals(self):
        self.locals_cfg = Storage()
        keys = '\n'.join([key for key in self.__dict__.keys() \
                          if (not key.startswith('__'))])
        for varskey in self.varsdata:
            varspath = os.path.join(self.var_varsdir, varskey)
            self.varsdata[varskey].merge({'path': varspath})
            prefix = self.varsdata[varskey].prefix
            self.varsdata[varskey].merge({'objvars': VARSObject(self, varskey)})
            if (prefix in self.prefixes):
                prefix_vars = re.findall(f'({prefix}_.*)+', keys)
                localvars = {pfx: self.__dict__[pfx] for pfx in prefix_vars}
                initvars = self.varsdata[varskey].objvars.init_vars(**localvars)
                self.varsdata[varskey].Vars.merge(initvars)
                setattr(self.locals_cfg, varskey, Storage())
                if (varskey not in self.ignoredvars):
                    for (keyname, vdata) in self.varsdata[varskey].Vars.items():
                        castedvalue = casttype(
                                                vdata.vartype,
                                                vdata.varvalue
                        )
                        self.locals_cfg[varskey].merge({keyname: castedvalue})
                        setattr(self, keyname, castedvalue)

    '''     instantiate & update locals for the benefit of the interactive QTShell
            The purpose is to expose them at the cmd shell so as to interact with
            imported modules and classes
    '''
    def initialize(self):
        '''  import & set attributes so they could be invoked @ the cmd shell
        '''
        localattributes = {}

        def dirImport(qdir):
            if AND(
                AND(
                    (os.path.isdir(qdir)),
                    (qdir not in self.var_excludelibdirs)
                ),
                    (re.match('^.*/__.*__$', qdir) is None)
            ):
                if (not qdir in sys.path):
                    sys.path.insert(0, qdir)
                searchPrefixes = [
                                  'objp4',
                                  'ObjP4db',
                                  'misc',
                                  'smp',
                                  'log',
                                  'con',
                                  'no',
                                  'hlp',
                                  'jnl',
                                  'p4dlg',
                                  'dlg',
                                  'py4',
                                  'db',
                                  'qry',
                                  'sh'
                ]
                searchdirs = os.listdir(qdir)
                for _file in searchdirs:
                    if (_file not in ['__PYCACHE__']):
                        filename = os.path.join(qdir, _file)
                        if (os.path.isfile(filename)):
                            if (_file == '.DS_Store'):
                                try:
                                    os.remove(os.path.join(qdir, _file))
                                except:
                                    pass
                            else:
                                sPfx = '|'.join(searchPrefixes)
                                sPrefix = f"^({sPfx}).*\.py$"
                                if (re.match(sPrefix, _file) is not None):
                                    try:
                                        mod = __import__(os.path.splitext(_file)[0])
                                        if (hasattr(mod, '__all__')):
                                            for item in mod.__all__:
                                                value = getattr(mod, item)
                                                setattr(self, item, value)
                                                localattributes.update(**{item: value})
                                    except Exception as err:
                                        print(err)
                        else:
                            dirImport(filename)
        [dirImport(moddir) for moddir in self.var_libsearchdirs]

        otherthings = {
                        'term': self.cmdcls_shterm(self),
            # Python modules
                        'pprint': pprint,
                        'os': os,
                        'sys': sys,
                        'pickle': pickle,
                        'calendar': calendar,
                        'time': time,
                        'datetime': datetime,
                        'pformat': pformat,
                        'StringIO': StringIO,
                        're': re,
                        'csv': csv,
                        'math': math,
                        'random': random,
            # Contrib
                        'PrettyTable': PrettyTable,
                        'DAL': DAL,
                        'Table': Table,
                        'Query': Query,
                        'Field': Field,
                        'Rows': Rows,
                        'Row': Row,
                        'dmp': diff_match_patch,
                        'lz4frame': lz4frame,
                        'lz4block': lz4block,
                        'six': six,
            # shortcuts
                        'execute': check_output,
                        'mload': mload,
                        'mloads': mloads,
                        'mdump': mdump,
                        'mdumps': mdumps
        }
        [localattributes.update(**{okey: otherthings[okey]}) for okey in otherthings.keys()]

        for (configname, cfg) in self.locals_cfg.items():
            if (not configname in self.ignoredvars):
                cfg.update(**{configname: getattr(self, f'cmd_{configname}')})
                localattributes.update(**cfg)

        for (cmdprfx, pfxdata) in (self.shellcmds.items()):
            (
                eos,
                enum
            ) = \
                (
                    False,
                    enumerate(dir(self))
                )
            while (eos is False):
                try:
                    (n, i) = next(enum)
                except StopIteration:
                    eos = True
                if (i.startswith(f'{pfxdata.prefix}_')):
                    (
                        k, v
                    ) = \
                        (
                            Lst(i.split('_'))(1),
                            getattr(self, i)
                        )
                    setattr(self, k, v)
                    localattributes.update(**{k: v})
            eos = True

        connectors = {
                        ObjP4(self): Lst(
                            'p4c',
                            'p4con',
                            'p4connect'
                        ),
                        ObjP4db(self): Lst(
                            'p4dbc',
                            'p4dbcon',
                            'p4dbconnect'
                        ),
                        ObjJnl(self): Lst(
                            'jnlc',
                            'jnlconnect',
                            'jnlcon'
                        ),
                        ObjDB(self): Lst(
                            'dbc',
                            'dbcon',
                            'dbconnect'
                        ),
                        ObjNO(self): Lst(
                            'noc',
                            'nocon',
                            'noconnect'
                        )
        }
        [
            localattributes.update(**{connectors[key][idx]: key})
            for key in connectors.keys() for idx in (0, 1, 2)
         ]

        locals().update(**localattributes)
        return locals()

    ''' shell specific functions to:
           * clear the screen
           * gracefully close the qtconsole
           * restart the qtconsole
    '''

    def cmd_clear(self, keep_input=True, numlines=50):
        def clearconsole(num):
            os.system('clear') \
                if (
                    os.name in (
                "posix",
                "darwin",
                "lin32",
                "lin64"
            )
            ) \
                else os.system('CLS') \
                if (
                    os.name in (
                "nt",
                "dos",
                "ce",
                "win32"
            )
            ) \
                else print('\n' * num)

        self.qtWidget.clear(keep_input=keep_input) \
            if (hasattr(self.qtWidget, 'clear')) \
            else clearconsole(numlines)

    class cmdcls_shterm(object):
        def __init__(self, obj):
            self.obj = obj

        def __call__(self, *args, **kwargs):
            return self

        def styles(self): return [
            'nocolor',
            'linux',
            'lightbg'
        ]

        def setstyle(self, _style): self.obj.qtWidget.set_default_style(
            _style \
                 if (_style in self.styles) \
                 else 'linux')

        def buffersize(self, size): self.obj.qtWidget.buffer_size(
            size \
                if (isinstance(size, int)) \
                else int(size))

        def restart(self): self.obj.qtWidget.request_restart_kernel()

        # def stacktracer(self): return Pdb
        def close(self):
            self.obj.qtWidget.kernel_client.stop_channels()
            self.obj.qtWidget.kernel_manager.shutdown_kernel()
            self.obj.app.exit()

        def updatelocals(self, *args, **kwargs):
            [self.obj.qtWidget.locals.merge(**{key: value}) for (key, value) in kwargs.items()]

        def updatekernellocals(self, *args, **kwargs):
            [self.obj.kernel.shell.push({key: value}) for (key, value) in kwargs.items()]

    '''  kernel manager & client
    '''
    def initKernel(self, *args, **kwargs):
        self.kernel_manager = QtInProcessKernelManager()
        self.kernel_manager.start_kernel()
        self.kernel = self.kernel_manager.kernel
        self.kernel.gui = 'qt'
        inits = Storage(self.initialize())
        self.kernel.shell.push(inits)
        self.kernel_client = self.kernel_manager.client()
        self.kernel_client.start_channels()
        return self.kernel

    '''  Jupiter widget instance
    '''
    def get_widget(self, *args, **kwargs):
        qtWidget = RichJupyterWidget()
        qtinstvars = self.cmd_qtvars()
        qtWidget.locals = self.kernel.shell.ns_table
        qtWidget.kernel_manager = self.kernel_manager
        qtWidget.kernel_client = self.kernel_client
        ''' shell configs
        '''
        qtWidget.banner = qtinstvars.banner
        qtWidget.ansi_codes = qtinstvars.ansi_codes
        qtWidget.set_default_style(qtinstvars.set_default_style)
        qtWidget.buffer_size = int(qtinstvars.buffer_size)
        qtWidget.execute_on_complete_input = qtinstvars.execute_on_complete_input
        qtWidget.gui_completion = qtinstvars.gui_completion
        qtWidget.kind = qtinstvars.kind
        qtWidget.paging = qtinstvars.paging
        qtWidget.font_family = qtinstvars.font_family
        qtWidget.font_size = qtinstvars.font_size
        qtWidget.width = qtinstvars.width
        qtWidget.height = qtinstvars.height
        qtWidget.override_shortcuts = qtinstvars.override_shortcuts
        ''' TODO: verify why calltips hang when auto-completing args :( 
        '''
        qtWidget.enable_calltips = False #qtinstvars.enable_calltips
        qtWidget.editor = "vim"
        return qtWidget

    '''  DLGQuery wrapped in an IPython QTShell requires IPython & PyQT4 | 5 (SIP)
    '''
    def initshell(self, strBanner='DLGShell', *args, **kwargs):
        self.app = guisupport.get_app_qt4()
        self.initKernel(*args, **kwargs)
        self.qtWidget = self.get_widget(*args, **kwargs)
        self.qtWidget.show()
        guisupport.start_event_loop_qt4(self.app)

class ServeLib(DLGShell):
    def __init__(self, *args, **kwargs):
        super(ServeLib, self).__init__(*args, **kwargs)
        self.initKernel()

    def __call__(self, *args, **kwargs):
        return self

class Serve(DLGShell):
    def __init__(self, *args, **kwargs):
        super(Serve, self).__init__(*args, **kwargs)

    def __call__(self, *args, **opts):
        self.initshell(**opts)

def _query(**opts):
    '''     BROKEN :(   TODO: fix

            #>>> python dlg.py query -j ./journals/journal2 -q domain.type=99 -v r16.2

           {'orderby': '',
            'dialect': 'journal',
            'newcolumn': '',
            'journal': './journals/journal2',
            'limitby': '',
            'delimiter': '',
            'maxrows': 10,
            'which': 'query',
            'query': '(domain.type=99)',
            'schemadir': 'r15.2',
            'groupby': ''}
    '''
    Storage(opts).delete('which')
    # RunQuery is in the shop!
    #RunQuery(**opts).__call__()

def initprog(**opts):
    opts = objectify(opts)
    if (opts.which is None):
        opts.which = 'shell'
    if (len(opts) > 0):
        if (opts.which == 'shell'):
            Serve()(**opts)
        elif (opts.which == 'query'):
            _query(**opts)
        else:
            print(f'options are:\n{opts}')
    else:
        return Serve()()

if (__name__ == '__main__'):
    if (len(sys.argv) > 1):
        '''  arg options from cmd line
        '''
        opts = Storage(
            {
                k: v for (k, v) in dict(
                vars(ArgsParser()().parse_args())).items() if (v not in (None, False, 'unset'))
            }
        )
        initprog(**opts)
    else:
        initprog()