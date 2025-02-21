import os
from libpy4.py4IO import Py4
from libdlg.dlgStore import ZDict, Lst
from libdlg.contrib.pydal.pydal import DAL
from libfs.fsFileIO import is_writable, make_writable
from libdlg.dlgUtilities import set_localport

'''  [$File: //dev/p4dlg/libconnect/conDB.py $] [$Change: 609 $] [$Revision: #9 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
'''

__all__ = ['ObjDB']

''' create a PyDal instance (Massimo's DB stuff)

    THIS IS STILL JUST A TEMPLATE - Still needs to get done.
'''

class ObjDB(object):
    def __init__(self, shellObj, *args, **kwargs):
        (agr, kwargs) = (Lst(args), ZDict(kwargs))
        self.db = DAL(('sqlite://storage.sqlite', 'mysql://a:b@localhost/x'), folder=kwargs.folder)
        self.shellObj = shellObj
        self.stored = None
        self.varsdef = self.shellObj.cmd_dbvars
        self.setstored()

    def show(self, name):
        return self.varsdef(name)

    def delete(self, name, key):
        if (self.varsdef(name) is None):
            print(f'Key Error - No such key "{key}"')
        else:
            try:
                kwargs = self.varsdef(name)
                kwargs.delete(key)
                objp4 = Py4(**kwargs)
                self.varsdef(name, kwargs)
                self.load(name)
                self.shellObj.kernel.shell.push({name: objp4})
                self.setstored()
                print(f"key ({key}) deleted")

                kwargs = self.varsdef(name)
                kwargs.delete(key)
                objp4 = Py4(**kwargs)
                self.varsdef(name, kwargs)
                self.load(name)
                self.shellObj.kernel.shell.push({name: objp4})
                self.setstored()
                print(f'key ({key}) deleted')
            except Exception as err:
                print(f'KeyError: {err}')

    def delete_new(self, name, key):
        try:
            [self.shellObj.kernel.shell.all_ns_refs[idx][name] for idx in range(0, 2)]
        except KeyError as err:
            print(err)
        ZDict(self.shellObj.__dict__).delete(name)
        self.setstored()

    def setstored(self):
        self.stored = Lst(key for key in self.varsdef()\
                .keys() if (key != '__session_loaded_on__'))

    def __call__(self, *args, **kwargs):
        return self

    def update(self, name, **kwargs):
        kwargs = ZDict(self.fixkeys(**kwargs))
        if (kwargs.port is not None):
            kwargs.port = self.setlocalport(kwargs.port, kwargs.p4droot)
        try:
            p4kwargs = self.varsdef(name).merge(kwargs)
            self.varsdef(name, p4kwargs)
            objp4 = self.load(name)
            self.shellObj.kernel.shell.push({name: p4kwargs})
            self.setstored()
            print('Reference ({}) updated!'.format(name))
            return objp4
        except TypeError as err:
            print('TypeError:\n{}'.format(err))

    def create(self, name, *args, **kwargs):
        if (self.shellObj.cmd_dbvars(name) is not None):
            print(f'CreateError:\n Name already exists "{name}" - use op4.update({name}) instead')
        try:
            (args, kwargs) = (Lst(args), ZDict(self.fixkeys(**kwargs)))
            StopError = None
            if (False in ((kwargs.user is not None),
                          (kwargs.client is not None),
                          (kwargs.port is not None))):
                StopError = 'p4 globals (user, port, client) are required!'
            if (kwargs.port == 'localport'):
                (port, p4droot) = (kwargs.port, kwargs.p4droot)
                if (p4droot is None):
                    StopError = 'p4droot must be set to use an RSH port!'
                else:
                    p4droot = os.path.abspath(p4droot)
                    if (port == 'localport'):
                        if (p4droot is not None):
                            if (os.path.exists(p4droot) is True):
                                ''' check that path to /p4d is valid'''
                                kwargs.port = set_localport(p4droot)
                            else:
                                StopError = f"p4droot path is invalid ({p4droot})"
                        else:
                            StopError = "An RSH port requires that p4droot be set"
        except Exception as err:
            StopError = f"Error: {err}"

        if (StopError is not None):
            print(f'Error:\n{StopError}')
        else:
            self.varsdef(name, kwargs)
            self.setstored()
            '''  time to load the thing
            '''
            objp4 = self.load(name)
            return objp4
            msgbox(name)('Reference Created!')
            print(f'Reference ({name}) create!')

    def load(self, name):
        if (self.varsdef(name) is None):
            print(f'KeyError:\n No such key "{name}"')
        else:
            try:
                kwargs = self.varsdef(name)
                objp4 = Py4(**kwargs)
                self.shellObj.kernel.shell.push({name: objp4})
                self.setstored()
                print(f'Reference ({name}) loaded')
                return objp4
            except Exception as err:
                print(f'KeyError: \n{err}')

    def unload(self, name):
        if (self.varsdef(name) is None):
            print(f'Attribute Error:\n No such attribute "{name}"')
        else:
            try:
                [self.shellObj.kernel.shell.all_ns_refs[idx][name] for idx in range(0, 2)]
                ZDict(self.shellObj.__dict__).delete(name)
                self.shellObj.kernel.shell.push(self.shellObj.__dict__)
                self.setstored()
                print(f'Reference ({name}) unloaded')
                return
            except KeyError as err:
                print(f'KeyError:\n{err}')

    def purge(self, name):
        if (self.varsdef(name) is None):
            print(f'KeyError:\n No such key "{name}"')
        else:
            filename = self.shellObj.varsdata.p4vars.path
            self.unload(name)
            if (is_writable(filename) is False):
                make_writable(filename)
            self.varsdef(name, None)
            try:
                self.shellObj.kernel.shell.del_var(name)
                globals().__delattr__(name)
                self.shellObj.__delattr__(name)
            except: pass
            self.setstored()
            print(f'Reference ({name}) destroyed')