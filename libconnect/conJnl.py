from libjnl.jnlIO import P4Jnl
from libdlg.dlgControl import DLGControl
from libdlg.dlgStore import Storage, Lst
from libfs.fsFileIO import is_writable, make_writable
from libsql.sqlSchema import get_schemaObject

'''  [$File: //dev/p4dlg/libconnect/conJnl.py $] [$Change: 729 $] [$Revision: #25 $]
     [$DateTime: 2025/06/13 15:49:54 $]
     [$Author: zerdlg $]
'''

__all__ = ['ObjJnl']


class ObjJnl(DLGControl):
    helpstr = """
Manage connections to journals and checkpoints.
        
    Create a connection to a journal with jnlconnect.create.
        
        `jnlconnect` attributes: create    create and store a connection to a journal
                                                - requires the connection name

                                 load      load an existing connector
                                            - requires the connection name

                                 update    update a connector's values
                                            - requires the connection name & the key/value pairs to update

                                 unload    unload a connectior from the current scope
                                            - requires the connection name

                                 destroy   destroy an existing connector
                                            - requires the connection name

                                 purge     combines unload/destroy
                                            - requires the connection name

                                 show      display the data that defined the object
                                            - requires the connection name

                                 help      display help about p4connect 

        
        parameters: args[0]  -> name,
                    keyword  -> journal = journal_path
                    keyword  -> version = release of the p4d instance
                                          that created the journal
                    keyword  -> oSchema = the schema that that defines the p4db

                    Note that keywords `version` & `oSchema` are mutually exclusive.
                    Pass in one or the orther.
                    * a bit more on schemas further down. 

        eg.
        
        Requirements
        -- requires a name, a journal file & a reference to a schema object (or a p4 release number)
            >>> journal = '../../resc/journals/journal2'
            >>> oSchema = schema('r16.2')
            
        Methods:
        -- create
            >>> jnlconnect.create('oJnl', journal=journal, oSchema=oSchema)
            >>> oJnl
            <P4Jnl ./resc/journals/journal2>

        -- load
            >>> jnlconnect.load('oJnl')
            >>> oJnl
            <P4Jnl ./resc/journals/journal2>

        -- update
            >>> jnlconnect.update('oJnl', **{'jounral': '../../resc/journals/checkpoint.24'})
            >>> oJNl
            <P4Jnl ./resc/journals/journal.24>

        -- unload
            >>> jnlconnect.unload('oJnl')
            >>> oJnl
            None

        -- destroy
            >>> jnlconnect.unload('oJnl')

        -- show
            * with name argument

            >>> jnlconnect.show('oJnl')
            {'journal': './resc/journals/journal.24',
             'oSchema': <libdlg.dlgSchema.SchemaXML at 0x10773da50>}

            * without name argument ( equivalent to jnlconnect.help() )

            >>> jnlconnect.show()
            ... returns this help string

        -- help
            >>> jnlconnect.help()
            ... returns this help string

        -- get list of stored jnl objects
            >>> jnlconnect.stored
            [oJnl, other_jnlobject, freds_jnlobject,]
            """

    def __init__(self, shellObj, loglevel='INFO'):
        self.shellObj = shellObj
        if (loglevel is not None):
            loglevel = loglevel.upper()
        self.loglevel = loglevel
        self.stored = None
        self.varsdef = self.shellObj.cmd_jnlvars
        self.setstored()

    def setstored(self):
        self.stored = Lst(key for key in self.varsdef().keys() \
                          if (key != '__session_loaded_on__'))

    def show(self, name=None):
        if (name is not None):
            return self.varsdef(name)
        return self.help()

    def __call__(self, loglevel=None):
        self.loglevel = loglevel.upper() or self.loglevel
        return self

    def create(
            self,
            name,
            journal=None,
            oSchema=None,
            version=None
    ):
        if (journal is None):
            return
        (oSchema, version) = get_schemaObject(journal, oSchema, version)
        if (oSchema is not None):
            value = Storage({'journal': journal, 'oSchema': oSchema})
            self.varsdef(name, value)
            ojnl = P4Jnl(journal, oSchema, loglevel=self.loglevel)
            self.shellObj.kernel.shell.push({name: ojnl})
            print(f'Reference ({name}) created')
        else:
            print('Missing reference to class SchemaXML and/or version, bailing... ')

    def update(self, name, **kwargs):
        if (self.varsdef(name) is None):
            print(f'UpdateError:\nNo such key "{name}"')
        if (len(kwargs) > 0):
            kwargs = Storage(kwargs)
            self.unload(name)
            jnlkwargs = self.varsdef(name).merge(kwargs)
            ''' this deletes any kwargs keys with a value of None
            '''
            dkeys = list(filter(lambda kw: (jnlkwargs[kw] is None), jnlkwargs))
            jnlkwargs.delete(*dkeys)
            ''' save it back to p4vars()
            '''
            self.varsdef(name, jnlkwargs)
            print(f'Reference ({name}) updated')
            ojnl = self.load(name)
            self.shellObj.kernel.shell.push({name: ojnl})
            self.setstored()
            return ojnl
        self.setstored()
        return self.load(name)

    def load(self, name):
        ret = self.varsdef(name)
        if (ret is not None):
            journal = ret.journal
            oSchema = ret.oSchema
            p4jnl = P4Jnl(journal, oSchema, loglevel=self.loglevel)
            self.shellObj.kernel.shell.push({name: p4jnl})
            print(f'Reference ({name}) loaded')
            return p4jnl
        self.setstored()
        return self

    def unload(self, name):
        try:
            [self.shellObj.kernel.shell.all_ns_refs[idx][name] for idx in range(0, 2)]
        except KeyError as err:
            print(err)
        Storage(self.shellObj.__dict__).delete(name)
        self.setstored()
        print(f'Reference ({name}) unloaded')

    def purge(self, name):
        if (self.varsdef(name) is None):
            print(f'KeyError:\nNo such key "{name}"')
        else:
            filename = self.shellObj.varsdata.jnlvars.path
            self.unload(name)
            if (is_writable(filename) is False):
                make_writable(filename)
            self.varsdef(name, None)
            try:
                self.shellObj.kernel.shell.del_var(name)
                globals().__delattr__(name)
                self.shellObj.__delattr__(name)
            except:pass
            self.setstored()
            print(f'Reference ({name}) destroyed')

    def help(self):
        return self.helpstr
