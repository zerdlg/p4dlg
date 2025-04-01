import os
import re
from subprocess import PIPE, Popen

from libdlg.dlgStore import ZDict, Lst
from libfs.fsFileIO import is_writable, make_writable
from libdlg.dlgUtilities import (
    decode_bytes,
    set_localport
)
from libsql.sqlSchema import SchemaXML
from libpy4.py4IO import Py4
from libdlg import bail

__all__ = ['ObjP4']

'''  [$File: //dev/p4dlg/libconnect/conP4.py $] [$Change: 678 $] [$Revision: #16 $]
     [$DateTime: 2025/04/01 04:47:46 $]
     [$Author: zerdlg $]
'''

class ObjP4(object):
    helpstr = """
        Manage connections to journals and checkpoints.

        `p4connect` attributes:  create    create and store a connection to a journal
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

        Create a connection to a p4d instance (or to a RSH port) with p4connect.create
        parameters: args[0]  -> name,
                    keyword  -> user=USERNAME
                    keyword  -> port = P4PORT
                    or any valid global

        eg.
            >>> user = 'zerdlg'
            >>> port = 'anastasia.local:1777'
            >>> client = 'my_client'

        -- create
            >>> p4connect.create('oP4', 
                    **{
                        'user': user,
                        'port': port,
                        'client': client
                        }
                )
            >>> oP4
            <Py4 anastasia.local:1777 >

        -- load
            >>> p4connect.load('oP4')
            >>> oP4
            <Py4 anastasia.local:1777 >

        -- update
            >>> p4connect.update('oP4', **{''user': 'bert'})
            >>> oP4
            <Py4 anastasia.local:1777 >

        -- unload
            >>> p4connect.unload('oP4')
            >>> oP4
            None

        -- destroy
            >>> p4connect.unload('oP4')

        -- purge
            >>> p4connect.purge('oP4')

        -- show
            * with name argument

            >>> p4connect.show('oP4')

            * without name argument ( equivalent to p4connect.help() )

            >>> p4connect.show()
            ... returns this help string

        -- help
            >>> p4connect.help()
            ... returns this help string

        -- get list of stored jnlconnect objects
            >>> p4connect.stored
            [oP4, other_p4object, freds_p4object,]
            """

    def help(self):
        return self.helpstr

    def __init__(self, shellObj, loglevel='DEBUG'):
        self.shellObj = shellObj
        self.loglevel = loglevel.upper()
        self.stored = None
        self.varsdef = self.shellObj.cmd_p4vars
        self.setstored()

    def help(self):
        return self.helpstr

    def show(self, name=None):
        if (name is not None):
            return self.varsdef(name)
        return self.helpstr

    def fixkeys(self, **kwargs):
        def fixkey(key):
            return re.sub(r'^p4', '', key) if (key != 'p4droot') else key
        return {fixkey(kkey): value for (kkey, value) in kwargs.items()}

    def delete(self, name, key):
        if (self.varsdef(name) is None):
            print(f'DeleteError - No such name "{name}"')
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
            except KeyError as err:
                print(f'{err}')

    def delete_new(self, name, key):
        if (self.varsdef(name) is None):
            print(f'DeleteError:\nNo such key "{name}"')
        else:
            try:
                [self.shellObj.kernel.shell.all_ns_refs[idx][name] for idx in range(0, 2)]
            except KeyError as err:
                print(err)
            ZDict(self.shellObj.__dict__).delete(name)
            self.setstored()

    def setstored(self):
        self.stored = Lst(key for key in self.varsdef().keys() if (key != '__session_loaded_on__'))

    ''' temporarily set or change any valid p4 globals
    '''
    def __call__(self, loglevel=None):
        self.loglevel = loglevel.upper() or self.loglevel
        return self

    def update(self, name, **kwargs):
        if (self.varsdef(name) is None):
            print(f'UpdateError:\nNo such key "{name}"')
        kwargs = ZDict(self.fixkeys(**kwargs))
        if (kwargs.port is not None):
            kwargs.port = set_localport(kwargs.p4droot)
        try:
            p4kwargs = self.varsdef(name).merge(kwargs)
            self.varsdef(name, p4kwargs)
            print(f'Reference ({name}) updated')
            objp4 = self.load(name)
            self.shellObj.kernel.shell.push({name: p4kwargs})
            self.setstored()
            return objp4
        except TypeError as err:
            print('TypeError:\n{}'.format(err))
        except Exception as err:
            print(err)

    def create(self, name, *args, **kwargs):
        if (self.varsdef(name) is not None):
            print(f'CreateError:\nName already exists "{name}" - use p4con.update({name}, **kwargs) instead')
        try:
            (args, kwargs) = (Lst(args), ZDict(self.fixkeys(**kwargs)))
            (
                objp4,
                oSchema,
                StopError
            ) = \
                (
                    None,
                    None,
                    None
                )
            if (
                    (kwargs.user is None) |
                    (kwargs.port is None)
            ):
                StopError = 'p4 globals `user` & `port` are required!'
            if (kwargs.client is None):
                kwargs.client = 'unset'
            if (kwargs.port == 'localport'):
                (
                    port,
                    p4droot
                ) = \
                    (
                        kwargs.port,
                        kwargs.p4droot
                    )
                if (p4droot is None):
                    StopError = 'A RSH port requires p4droot to be set'
                else:
                    p4droot = os.path.abspath(p4droot)
                    if (port == 'localport'):
                        if (p4droot is not None):
                            if (os.path.exists(p4droot) is True):
                                ''' Check that p4droot is valid 
                                '''
                                kwargs.port = set_localport(p4droot)
                            else:
                                StopError = f"No such p4droot path ({p4droot})."
                        else:
                            StopError = f"A RSH port requires p4droot to be set"
            (connected, info_or_failure) = self.is_connected(kwargs.port)
            if (connected is True):
                if (kwargs.oSchema is None):
                    if (kwargs.version is not None):
                        oSchema = SchemaXML(version=kwargs.version)
                    else:
                        try:
                            objp4 = Py4(**kwargs)
                            oSchema = objp4.oSchema
                        except Exception as err:
                            StopError = f"Error increate py4 object: {err}"
            if (oSchema is not None):
                kwargs.update(
                    **{
                        'oSchema': oSchema,
                        'version': oSchema.version
                    }
                )
            else:
                StopError = f"Error in creating a connectio: {info_or_failure}."
        except Exception as err:
            StopError = f"Error: {err}"
        if (StopError is not None):
            print(f'Error: {StopError}')
        else:
            self.varsdef(name, kwargs)
            self.setstored()
            return self.load(name, objp4=objp4)

    def is_connected(self, p4port):
        failure = f"""Perforce client error:\n\tConnect to server failed; check $P4PORT."""
        p4opener = Popen(['p4', '--port', p4port, 'info'], stdout=PIPE, stderr=PIPE)
        try:
            (outFile, errFile) = p4opener.communicate()
            out = decode_bytes(outFile \
                    if (len(outFile) > 0)
                    else errFile)
            if (out.startswith(failure)):
                return (False, failure)
            return (True, out)
        except Exception as err:
            bail(err)
        finally:
            if (hasattr(p4opener, 'close') is True):
                if (p4opener.closed is False):
                    p4opener.close()

    def load(self, name, objp4=None):
        if (self.varsdef(name) is None):
            print(f'LoadErrror: No such name: "{name}"')
        else:
            try:
                kwargs = self.varsdef(name)
                kwargs.update(**{'loglevel': self.loglevel})
                p4port = kwargs.port
                (connected, connect_msg) = self.is_connected(p4port)
                if (connected is True):
                    if (objp4 is None):
                        if (kwargs.version is not None):
                            kwargs.delete('version')
                        objp4 = Py4(**kwargs)
                    self.shellObj.kernel.shell.push({name: objp4})
                    self.setstored()
                    print(f'Reference ({name}) loaded & connected to {p4port}')
                    return objp4
                else:
                    bail(f'\nUnable to connect to {p4port}\n{connect_msg}')
            except (Exception, KeyError, AttributeError) as err:
                bail(err)

    def unload(self, name):
        if (self.varsdef(name) is None):
            print(f'UNLoadErrror: No such name: "{name}"')
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
            print(f'PurgeError:\nNo such key "{name}"')
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
            except:pass
            self.setstored()
            print(f'Reference ({name}) destroyed')