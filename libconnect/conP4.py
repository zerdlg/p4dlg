import os
import re
from subprocess import PIPE, Popen
import unittest

from libdlg.dlgStore import Storage, Lst
from libdlg.dlgFileIO import is_writable, make_writable
from libdlg.dlgUtilities import bail, decode_bytes
from libpy4.py4IO import Py4
from libdlg.dlgUtilities import set_localport

__all__ = ['ObjP4']

'''  [$File: //dev/p4dlg/libconnect/conP4.py $] [$Change: 473 $] [$Revision: #14 $]
     [$DateTime: 2024/09/08 08:15:23 $]
     [$Author: mart $]
'''

class ObjP4(object):
    """
        {'fred': {'client': 'gc.pycharm',
        'p4droot': '/Users/gc/p4dinst/2015.2/',
        'port': 'rsh:/Users/gc/p4dinst/2015.2/p4d -r /Users/gc/p4dinst/2015.2 -L /Users/gc/p4dinst/2015.2/serverlog -i -vserver=3',
        'user': 'mart'},

        'oP4': {'client': 'gc.pycharm',
        'password': '3Sour.Sn4kes...!',
        'port': 'anastasia.local:1777',
        'user': 'mart'},

        'p4': {'client': 'gc.pycharm',
        'p4droot': '/Users/gc/p4dinst/2015.2',
        'port': 'rsh:/Users/gc/p4dinst/2015.2/p4d -r /Users/gc/p4dinst/2015.2 -L /Users/gc/p4dinst/2015.2/serverlog -i -vserver=3',
        'user': 'mart'}}

        oSchema = schema('r15.2')
        {'oP4': {'client': 'gc.pycharm',
          'p4droot': '/Users/gc/p4dinst/2015.2',
          'port': 'rsh:/Users/gc/p4dinst/2015.2/p4d -r /Users/gc/p4dinst/2015.2 -L /Users/gc/p4dinst/2015.2/serverlog -i -vserver=3',
          'user': 'mart',
          'oSchema': oSchema}}

        oSchema = schema('r16.2')
        {'oP4': {'client': 'computer_p4q',
                 'port': 'anastasia.local:1777',
                 'user': 'mart',
                 'oSchema': oSchema}}

    TODO: add something to login / logout/ create .p4config, .p4ignore, .p4tickets, etc...
          add something for `p4 passwd`
    """

    def help(self):
        hlpstr = """\
        Create and manage a connector to a Perforce instance.
        
        Note that the reference already exists and is exposed as `p4con` or `p4connect` when running in the shell.
        
        >>> oSchema = SchemaXML()(version='r16.2')  # a reference to class SchemaXML
        >>> p4con.create(
                    'my_connection',                # the name of the connector
                    oSchema,                        
                    user='mart',                    # a p4 user to access P4D
                    port='anastasia.local:1777',    # the port to a p4d instance
                    client='my_client'              # the clientspec that defines your workspace
            )
        
        Once create, it is stored locally. You can simply load it directly from the shell.
        
        >>> p4con.load('my_connection')
        Reference (oP4) loaded & connected to anastasia.local:1777
        <Py4 anastasia.local:1777 >
        
        methods:
            
            create  -   requires:
                                * a name for the connector
                                
                                * either a predefined reference to class Py4 or a 
                                release version to an existing XML schema document.
                                
                                * keywords:
                                            user    -   a perforce user
                                            port    -   the port for the p4d instance
                                            client  -   the name of a client that defines the workspace
                    
            load    -   load an existing connector
                        requires the name of a connector
            
            unload  -   unload a running connector
                        requires the name of a connector
            
            update  -   update the connector attributes
                        requires the name of a connector as key/value pairs containing the updated value 
            
            purge   -   remove the stored data related to the named connector
                        requires the name of a connector 
                         
        """
        print(hlpstr)

    def __init__(self, shellObj, loglevel='INFO'):
        self.shellObj = shellObj
        self.loglevel = loglevel.upper()
        self.setstored()

    def fixkeys(self, **kwargs):
        def fixkey(key):
            return re.sub(r'^p4', '', key) if (key != 'p4droot') else key
        return {fixkey(kkey): value for (kkey, value) in kwargs.items()}

    def delete(self, name, key):
        if (self.shellObj.cmd_p4vars(name) is None):
            print(f'Key Error - No such key "{key}"')
        else:
            try:
                kwargs = self.shellObj.cmd_p4vars(name)
                kwargs.delete(key)
                objp4 = Py4(**kwargs)
                self.shellObj.cmd_p4vars(name, kwargs)
                self.load(name)
                self.shellObj.kernel.shell.push({name: objp4})
                self.setstored()
                print(f"key ({key}) deleted")
            except KeyError as err:
                print(f'{err}')

    def delete_new(self, name, key):
        try:
            [self.shellObj.kernel.shell.all_ns_refs[idx][name] for idx in range(0, 2)]
        except KeyError as err:
            print(err)
        Storage(self.shellObj.__dict__).delete(name)
        self.setstored()

    def setstored(self):
        self.stored = Lst(key for key in self.shellObj.cmd_p4vars().keys() if (key != '__session_loaded_on__'))

    ''' temporarily set or change any valid p4 globals
    '''
    def __call__(self, loglevel=None):
        self.loglevel = loglevel.upper() or self.loglevel
        return self

    def update(self, name, **kwargs):
        kwargs = Storage(self.fixkeys(**kwargs))
        if (kwargs.port is not None):
            kwargs.port = set_localport(kwargs.p4droot)
        try:
            p4kwargs = self.shellObj.cmd_p4vars(name).merge(kwargs)
            self.shellObj.cmd_p4vars(name, p4kwargs)
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
        if (self.shellObj.cmd_p4vars(name) is not None):
            print(f'CreateError:\n Name already exists "{name}" - use op4.update({name}) instead')
        try:
            (args, kwargs) = (Lst(args), Storage(self.fixkeys(**kwargs)))
            oSchema = kwargs.oSchema
            if (
                    (oSchema is None)
                    & (kwargs.version is not None)
            ):
                oSchema = self.shellObj.memoize_schema(kwargs.version)
                kwargs.oSchema = oSchema
            if (kwargs.version is not None):
                kwargs.delete('version')
            StopError = None
            if (False in (
                    (kwargs.user is not None),
                    (kwargs.port is not None)
                    )
            ):
                StopError = 'p4 globals (user, port) are required!'
            if (kwargs.client is None):
                kwargs.client = 'unset'
            if (kwargs.port == 'localport'):
                (port, p4droot) = (kwargs.port, kwargs.p4droot)
                if (p4droot is None):
                    StopError = 'p4droot must be set to use an RSH port!'
                else:
                    p4droot = os.path.abspath(p4droot)
                    if (port == 'localport'):
                        if (p4droot is not None):
                            if (os.path.exists(p4droot) is True):
                                ''' check that path to /p4d is valid 
                                '''
                                kwargs.port = set_localport(p4droot)
                            else:
                                StopError = f"p4droot path is invalid {p4droot}"
                        else:
                            StopError = f"A RSH port requires p4droot to be set"
        except Exception as err:
            StopError = f"Error: {err}"

        if (StopError is not None):
            print(f'Error:\n{StopError}')
        else:
            self.shellObj.cmd_p4vars(name, kwargs)
            self.setstored()
            '''  time to load the thing
            '''
            objp4 = self.load(name)
            return objp4

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
            return (True, None)
        except Exception as err:
            bail(err)
        finally:
            if (hasattr(p4opener, 'close') is True):
                if (p4opener.closed is False):
                    p4opener.close()

    def load(self, name):
        if (self.shellObj.cmd_p4vars(name) is None):
            print(f'KeyError:\n No such key "{name}"')
        else:
            try:
                kwargs = self.shellObj.cmd_p4vars(name)
                kwargs.update(**{'loglevel': self.loglevel})
                p4port = kwargs.port
                (connected, connect_msg) = self.is_connected(p4port)
                if (connected is True):
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
        if (self.shellObj.cmd_p4vars(name) is None):
            bail(
                f'Attribute Error:\n No such attribute "{name}"'
            )
        else:
            try:
                [self.shellObj.kernel.shell.all_ns_refs[idx][name] for idx in range(0, 2)]
                Storage(self.shellObj.__dict__).delete(name)
                self.shellObj.kernel.shell.push(self.shellObj.__dict__)
                self.setstored()
                print(f'Reference ({name}) unloaded')
                return
            except KeyError as err:
                print(f'KeyError:\n{err}')

    def destroy(self, name):
        if (self.shellObj.cmd_p4vars(name) is None):
            print(f'KeyError:\nNo such key "{name}"')
        else:
            try:
                filename = self.shellObj.varsdata.p4vars.path
                self.unload(name)
                if (is_writable(filename) is False):
                    make_writable(filename)
                self.shellObj.cmd_p4vars(name, None)
                if hasattr(globals(), name):
                    globals().__delattr__(name)
                self.setstored()
                print(f'Reference ({name}) destroyed')
            except Exception as err:
                bail(
                    f'Exception:\n{err}'
                )