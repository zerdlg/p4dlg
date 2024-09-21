from libjnl.jnlIO import P4Jnl
from libdlg.dlgControl import DLGControl
from libdlg.dlgStore import Storage, Lst
from libdlg.dlgQuery_and_operators import AND
from libdlg.dlgFileIO import is_writable, make_writable


'''  [$File: //dev/p4dlg/libconnect/conJnl.py $] [$Change: 476 $] [$Revision: #12 $]
     [$DateTime: 2024/09/13 01:55:06 $]
     [$Author: mart $]
'''

'''    Journal stuff & usage:

                methods:    create - load - - update - unload - destroy - purge 

                # requires a journal file & a reference to a schema object (or a p4 release number)
                >>> journal = '../../resc/journals/journal2'
                >>> oSchema = schema('r15.2')

                # the class reference
                >>> jnl = Jnl(self)

                #create
                >>> jnl.create('oJnl',journal=journal,oSchema=oSchema)
                >>> oJnl
                <jnlIO.P4Jnl at 0x11dfab990>

                #load
                >>> jnl.load('oJnl')
                >>> oJnl
                <jnlIO.P4Jnl at 0x11dfab990>

                #unload
                >>> jnl.unload('oJnl')
                >>> oJnl
                None

                # get list of stored jnl objects
                >>> jnl.stored()
                [oJnl, other_jnlobject, freds_jnlobject,]
'''

__all__ = ['ObjJnl']

class ObjJnl(DLGControl):
    def __init__(self, shellObj, loglevel='INFO'):
        self.shellObj = shellObj
        self.loglevel = loglevel.upper()
        self.setstored()

    def setstored(self):
        self.stored = Lst(key for key in self.shellObj.cmd_jnlvars().keys() \
                          if (key != '__session_loaded_on__'))

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
        if AND(
                (oSchema is None),
                (version is not None)
        ):
            oSchema = self.shellObj.cmd_schema(version)
        value = Storage({'journal': journal, 'oSchema': oSchema})
        self.shellObj.cmd_jnlvars(name, value)
        ojnl = P4Jnl(journal, oSchema, loglevel=self.loglevel)
        self.shellObj.kernel.shell.push({name: ojnl})
        print(f'Reference ({name}) created')

    def update(self, name, **kwargs):
        if (len(kwargs) > 0):
            self.unload(name)
            old_value = self.shellObj.cmd_jnlvars(name)
            kwargs = Storage(kwargs)
            journal = kwargs.journal or old_value.journal
            oSchema = kwargs.oSchema or old_value.oSchema
            new_value = Storage({'journal': journal, 'oSchema': oSchema})
            self.shellObj.cmd_jnlvars(name, new_value)
            print(f'Reference ({name}) updated')
            ojnl = P4Jnl(journal, oSchema, loglevel=self.loglevel)
            self.shellObj.kernel.shell.push({name: ojnl})
            self.setstored()
            return ojnl
        self.setstored()
        return self.load(name)

    def load(self, name):
        ret = self.shellObj.cmd_jnlvars(name)
        if (ret is not None):
            journal = ret.journal
            oSchema = ret.oSchema
            p4jnl = P4Jnl(journal, oSchema, loglevel=self.loglevel)
            self.shellObj.kernel.shell.push({name: p4jnl})
            print(f'Reference ({name}) loaded')
            return p4jnl
        self.setstored()
        return self

    def reload(self, name):
        ret = self.load(name)
        print(f'Reference ({name}) reloaded')

    def unload(self, name):
        try:
            [self.shellObj.kernel.shell.all_ns_refs[idx][name] for idx in range(0, 2)]
        except KeyError as err:
            print(err)
        Storage(self.shellObj.__dict__).delete(name)
        self.setstored()
        print(f'Reference ({name}) unloaded')

    def purge(self, name):
        filename = self.shellObj.varsdata.jnlvars.path
        self.unload(name)
        if (is_writable(filename) is False):
            make_writable(filename)
        self.shellObj.cmd_jnlvars(name, None)
        try:
            globals().__delattr__(name)
        except:pass
        self.setstored()
        print(f'Reference ({name}) destroyed')