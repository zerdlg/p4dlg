from io import StringIO

from libdlg.dlgStore import Storage, Lst
from libdlg.dlgFileIO import is_writable, make_writable

'''  [$File: //dev/p4dlg/libconnect/conNO.py $] [$Change: 472 $] [$Revision: #9 $]
     [$DateTime: 2024/09/03 03:46:02 $]
     [$Author: mart $]
'''

__all__ = ['ObjNO']

# JUST a template for now...

class ObjNO(object):
    def __init__(self, shellObj, loglevel='INFO'):
        self.shellObj = shellObj
        self.loglevel = loglevel.upper()
        self.stored = None
        self.varsdef = self.shellObj.cmd_novars
        self.setstored()

    def __call__(self, loglevel=None):
        self.loglevel = loglevel.upper() or self.loglevel
        return self

    def show(self, name):
        return self.varsdef(name)

    def setstored(self):
        self.stored = Lst(key for key in self.varsdef().keys() \
                          if (key != '__session_loaded_on__'))

    def create(self, name, **kwargs):
        value = Storage(kwargs)
        self.varsdef(name, value)
        self.shellObj.kernel.shell.push({name: value})
        print(f'Reference ({name}) created')

    def load(self, name):
        ret = self.varsdef(name)
        if (ret is not None):
            self.shellObj.kernel.shell.push({name: Lst()})
            print(f'Reference ({name}) loaded')
            return Lst()
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

    def update(self, name, **kwargs):
        kwargs = Storage(kwargs)
        if (len(kwargs) > 0):
            self.unload(name)
            old_value = self.varsdef(name)
            new_value = old_value.merge(kwargs)
            self.varsdef(name, new_value)
            print(f'Reference ({name}) updated')
            self.shellObj.kernel.shell.push({name: new_value})
            self.setstored()
            return new_value
        self.setstored()
        return self.load(name)

    def purge(self, name):
        filename = self.shellObj.varsdata.basevars.path
        self.unload(name)
        if (is_writable(filename) is False):
            make_writable(filename)
        self.varsdef(name, None)
        try:
            globals().__delattr__(name)
        except:
            pass
        self.setstored()
        print(f'Reference ({name}) destroyed')