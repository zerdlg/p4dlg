import os, sys
import re
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
from qtconsole.rich_jupyter_widget import RichJupyterWidget
from qtconsole.inprocess import QtInProcessKernelManager
from IPython.lib import guisupport
from IPython.core.debugger import Pdb


''' p4dlg import
'''
from libdlg import *
#from libdlg.dlgStore import *
#from libdlg.dlgSchema import *
#from libdlg.dlgUtilities import *

from libjnl.jnlInit import JnlInitialize
from libsh import varsdata
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

from libsh import (
    varsdata,
    default_xmlschema_version,
    dbdir,
    schemadir,
    journaldir,
    projectdir,
    journals,
    varsdir,
    db
)

#''' absolute path to schemaxml directory
#'''
#from os.path import dirname
#import schemaxml
#from resc import journals, db

#default_xmlschema_version = 'r15.2'
#schemadir = dirname(schemaxml.__file__)
#journaldir = dirname(journals.__file__)
#projectdir = dirname(schemadir)
#dbdir = dirname(db.__file__)
#varsdir = os.path.join(
#    *[
#        projectdir,
#        'libsh',
#        'Vars'
#    ]
#)

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
        --[$Author: zerdlg $]
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

    jupiter qtconsole options:  >>> jupyter qtconsole -h
'''
def createdir(directory):
    try:
        os.mkdir(directory)
    except Exception as err:
        pass

class DLGShell(object):
    cmd_schema = lambda self, version: self.memoize_schema(version)
    #cmd_initializeenv = lambda self: self.cmd_initialize_jnlENV()
    cmd_updatelocals = lambda self: self.instlocals()

    def __new__(cls, *args, **kwargs):
        return object.__new__(cls)

    def __call__(self, *args, **kwargs):
        return self

    #def cmd_initialize_jnlENV(self):
        #oInit = JnlInitialize(schemadir, default_xmlschema_version)
        #print("JNL INITIALIZE - Done.")
        #return oInit

    def __init__(self, *args, **kwargs):
        self.clsVars = clsVars
        self.shFuncs = ZDict()
        self.shClasses = ZDict()
        self.locals_cfg = ZDict()
        self.schemaMemo = {}
        self.varsdata = objectify(varsdata)
        self.ignoredvars = []
        (
            self.var_projectdir,
            self.var_journaldir,
            self.var_schemadir,
            self.var_dbdir,
            self.var_varsdir
        ) = \
            (
                projectdir,
                journaldir,
                schemadir,
                dbdir,
                varsdir
            )

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

        self.var_test = 'charlotte'

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
        self.qt_set_default_style = 'linux'
        self.qt_banner = f"DLGShell {mdata}\n\n"
        self.qt_ansi_codes = True
        self.qt_buffer_size = '1200'
        self.qt_execute_on_complete_input = True
        self.qt_gui_completion = 'ncurses'
        self.qt_default_style = 'linux'
        self.qt_kind = 'rich'
        self.qt_enable_calltips = True
        self.qt_paging = 'vsplit'
        self.qt_font_family = 'monaco'
        self.qt_font_size = 13
        self.qt_console_width = 150
        self.qt_console_height = 75
        self.qt_override_shortcuts = False
        self.qt_backend = 'matplotlib'
        self.qt_gui = 'qt'

        ''' here's a good spot to populate cmd_ & cmdcls_ attributes - whatever, whenever
        '''

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

    def memoize_schema(self, version=default_xmlschema_version):
        '''  force version format as release name (r16.2) so
             we don't end up with a memo with duplicate-like
             keys  (r16.2 = 2016.2)
        '''
        version = to_releasename(version)
        try:
            schemamem = self.schemaMemo[version]
        except KeyError:
            if (schemadir is None):
                bail(f'schemadir is None - cannot retrieve xmlschema version {version}')
            oSchema = SchemaXML(version=version)
            schemamem = self.schemaMemo[version] = oSchema
        return schemamem

    ''' set & unset session Vars (will not persist beyond the session's life span)
    '''
    def cmd_unsetvar(
            self,
            configname,
            *args,
            **kwargs
    ):
        self.clsVars(self, configname).unset(*args, **kwargs)

    def cmd_setvar(
            self,
            configname,
            *args,
            **kwargs
    ):
        self.clsVars(self, configname).set(*args, **kwargs)

    def validatevars(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), ZDict(kwargs))
        allinstvars = self.shFuncs.getkeys() \
                    + self.shClasses.getkeys() \
                    + self.locals_cfg.getkeys()

        if (len(args) > 1):
            if (
                    (args(0) in allinstvars) &
                    (args(1) is not None)
            ):
                return False
        for key in kwargs.keys():
            if (
                    (key in allinstvars) &
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
        self.locals_cfg = ZDict()
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
                setattr(self.locals_cfg, varskey, ZDict())
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
            if (
                    (os.path.isdir(qdir)) &
                    (qdir not in self.var_excludelibdirs) &
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
                            'p4con',
                            'p4connect'
                        ),
                        ObjP4db(self): Lst(
                            'p4dbcon',
                            'p4dbconnect'
                        ),
                        ObjJnl(self): Lst(
                            'jnlconnect',
                            'jnlcon'
                        ),
                        ObjDB(self): Lst(
                            'dbcon',
                            'dbconnect'
                        ),
                        ObjNO(self): Lst(
                            'nocon',
                            'noconnect'
                        )
        }
        [
            localattributes.update(**{connectors[key][idx]: key})
            for key in connectors.keys() for idx in (0, 1)
         ]
        locals().update(**localattributes)
        return locals()

    ''' shell specific functions to:
           * clear the screen
           * gracefully close the qtconsole
           * restart the qtconsole
           * etc.
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
            self.styles = [
            'nocolor',
            'linux',
            'lightbg'
        ]
            self.qtvars = self.obj.cmd_qtvars()

        def __call__(self, *args, **kwargs):
            return self

        def set_term(self, terminal='xterm-color', location='/etc/terminfo'):
            if (os.environ['TERM'] is False):
                os.environ.update(
                    **{
                        'TERM': terminal,
                        'terminfo': location
                    }
                )

        def setstyle(self, style):
            if (not style in self.styles):
                style = self.qtvars.default_style
            self.obj.qtWidget.set_default_style(style)

        def buffersize(self, size):
            if (isinstance(size, int) is False):
                size = int(size)
            self.obj.qtWidget.buffer_size(size)

        def restart(self):
            self.obj.qtWidget.request_restart_kernel()

        # def stacktracer(self): return Pdb

        def close(self):
            self.obj.qtWidget.kernel_client.stop_channels()
            self.obj.qtWidget.kernel_manager.shutdown_kernel()
            self.obj.app.exit()

        def updatelocals(self, *args, **kwargs):
            [self.obj.qtWidget.locals.merge(**{key: value}) for (key, value) in kwargs.items()]

        def updatekernellocals(self, *args, **kwargs):
            [self.obj.kernel.shell.push({key: value}) for (key, value) in kwargs.items()]

    def cmd_scriptedit(self, filename, line=None):
        self.qtWidget.editor = "gnome-terminal -- vim"
        self.qtWidget._edit(filename, line=None)

    '''  kernel manager & client
    '''
    def initKernel(self, *args, **kwargs):
        self.kernel_manager = QtInProcessKernelManager()
        self.kernel_manager.start_kernel()
        self.kernel = self.kernel_manager.kernel
        self.kernel.gui = 'qt'
        inits = ZDict(self.initialize())
        self.kernel.shell.push(inits)
        self.kernel_client = self.kernel_manager.client()
        self.kernel_client.start_channels()
        return self.kernel

    '''  Jupiter widget instance
    '''
    def get_widget(self, *args, **kwargs):
        qtinstvars = self.cmd_qtvars()
        RJWidget = RichJupyterWidget()
        #RJWidget.set_default_style(qtinstvars.default_style)
        RJWidget._set_completion_widget(qtinstvars.gui_completion)
        RJWidget.locals = self.kernel.shell.ns_table
        RJWidget.kernel_manager = self.kernel_manager
        RJWidget.kernel_client = self.kernel_client
        ''' shell configs        
        '''
        #RJWidget.editor
        #RJWidget._edit()
        #RJWidget.set_default_style('linux')

        RJWidget.console_height = qtinstvars.console_height
        RJWidget.console_width = qtinstvars.console_width
        RJWidget.banner = qtinstvars.banner
        RJWidget.ansi_codes = qtinstvars.ansi_codes
        RJWidget.buffer_size = int(qtinstvars.buffer_size)
        RJWidget.kind = qtinstvars.kind
        RJWidget.paging = qtinstvars.paging
        RJWidget.font_family = qtinstvars.font_family
        RJWidget.font_size = qtinstvars.font_size
        RJWidget.override_shortcuts = qtinstvars.override_shortcuts
        ''' TODO: verify why calltips hang when auto-completing args :( 
        '''
        RJWidget.execute_on_complete_input = qtinstvars.execute_on_complete_input
        RJWidget.enable_calltips = False#qtinstvars.enable_calltips
        RJWidget.editor = "gnome-terminal -- vim"
        return RJWidget

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
    '''     BROKEN :(   TODO: fix this.

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
            'version': 'r15.2',
            'groupby': ''}
    '''
    ZDict(opts).delete('which')
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
        opts = ZDict(
            {
                k: v for (k, v) in dict(
                vars(ArgsParser()().parse_args())).items() if (v not in (None, False, 'unset'))
            }
        )
        initprog(**opts)
    else:
        initprog()