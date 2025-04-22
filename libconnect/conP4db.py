import os
import sys
import re, socket
from subprocess import PIPE, Popen
from pprint import pprint

from libdlg.dlgStore import Storage, Lst
from libfs.fsFileIO import is_writable, make_writable
from libdlg.dlgUtilities import bail, decode_bytes
from libpy4.py4IO import Py4
from libdlg.dlgUtilities import set_localport
from libconnect.conP4 import ObjP4

__all__ = ['ObjP4db']

'''  [$File: //dev/p4dlg/libconnect/conP4db.py $] [$Change: 683 $] [$Revision: #8 $]
     [$DateTime: 2025/04/07 18:39:56 $]
     [$Author: mart $]
'''


class ObjP4db(object):
    """
        {'fred': {'client': 'gc.pycharm',
        'p4droot': '/Users/gc/p4dinst/2015.2/',
        'port': 'rsh:/Users/gc/p4dinst/2015.2/p4d -r /Users/gc/p4dinst/2015.2 -L /Users/gc/p4dinst/2015.2/serverlog -i -vserver=3',
        'user': 'zerdlg'},

        'oP4': {'client': 'gc.pycharm',
        'password': '3Sour.Sn4kes...!',
        'port': 'anastasia.local:1777',
        'user': 'zerdlg'},

        'p4': {'client': 'gc.pycharm',
        'p4droot': '/Users/gc/p4dinst/2015.2',
        'port': 'rsh:/Users/gc/p4dinst/2015.2/p4d -r /Users/gc/p4dinst/2015.2 -L /Users/gc/p4dinst/2015.2/serverlog -i -vserver=3',
        'user': 'zerdlg'}}

        oSchema = schema('r15.2')
        {'oP4': {'client': 'gc.pycharm',
          'p4droot': '/Users/gc/p4dinst/2015.2',
          'port': 'rsh:/Users/gc/p4dinst/2015.2/p4d -r /Users/gc/p4dinst/2015.2 -L /Users/gc/p4dinst/2015.2/serverlog -i -vserver=3',
          'user': 'zerdlg',
          'oSchema': oSchema}}

        oSchema = schema('r16.2')
        {'oP4': {'client': 'computer_p4q',
                 'port': 'anastasia.local:1777',
                 'user': 'zerdlg',
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
                    user='zerdlg',                    # a p4 user to access P4D
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
        self.hostname = re.sub('\.', '', socket.gethostname())
        self.platform = sys.platform
        (
            self.user,
            self.port,
            self.client,
            self.dbroot,
            self.client_root
        ) \
            = \
            (
                None,
                None,
                None,
                None,
                None
            )
        self.stored = None
        self.varsdef = self.shellObj.cmd_p4dbvars
        self.setstored()

    def show(self, name):
        return self.varsdef(name)

    def fixkeys(self, **kwargs):
        def fixkey(key):
            return re.sub(r'^p4', '', key) if (key != 'p4droot') else key
        return {fixkey(kkey): value for (kkey, value) in kwargs.items()}

    def delete(self, name, key):
        if (self.varsdef(name) is None):
            print(f'Key Error - No such key "{key}"')
        else:
            try:
                kwargs = self.varsdef(name)
                kwargs.delete(key)
                objp4db = Py4(**kwargs)
                self.varsdef(name, kwargs)
                self.load(name)
                self.shellObj.kernel.shell.push({name: objp4db})
                self.setstored()
                print(f"key ({key}) deleted")
            except KeyError as err:
                print(f'{err}')

    def setstored(self):
        self.stored = Lst(key for key in self.varsdef().keys() if (key != '__session_loaded_on__'))

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
            p4kwargs = self.varsdef(name).merge(kwargs)
            self.varsdef(name, p4kwargs)
            print(f'Reference ({name}) updated')
            objp4db = self.load(name)
            self.shellObj.kernel.shell.push({name: p4kwargs})
            self.setstored()
            return objp4db
        except TypeError as err:
            print('TypeError:\n{}'.format(err))
        except Exception as err:
            print(err)

    def setup_resc(self, dbdir, dbdata):
        if (os.path.isdir(dbdata)):
            try:
                os.mkdir(dbdir)
            except:
                pass
        if (not os.path.isfile(dbdata)):
            inittxt_header = self.shellObj.ktxt()
            oFile = open(dbdata, 'w')
            try:
                oFile.write(inittxt_header)
            finally:
                oFile.close()

    def create(self, name, *args, **kwargs):
        if (self.varsdef(name) is not None):
            print(f'CreateError:\n Name already exists "{name}" - use self.update({name}) instead')
        else:
            (args, kwargs) = (Lst(args), Storage(self.fixkeys(**kwargs)))
            objp4 = kwargs.objp4
            if (isinstance(objp4, str) is True):
                objp4 = ObjP4(self.shellObj, loglevel='INFO').load(objp4)
            dbroot = f'//p4db/{name}'
            dbdir = f"{self.shellObj.configvars('dbdir')}/{name}"
            dbdata = f"{dbdir}/.dbdata"
            user = objp4._user
            clientname = f'{name}_{user}_{self.hostname}_{self.platform}'
            self.client = clientname
            objp4.client(clientname, '-i', **{'Root': dbdir})#, 'View': [f'{dbroot}/... {clientname}/...',]})

            self.setup_resc(dbdir, dbdata)
            dbname = kwargs.dbname
            paths = objp4.where(dbroot)
            pprint(paths)

            #self.client_root = kwargs.client_root or f"{dbdir}/{dbname}"
            #clientname = f'{name}_{self.user}_{self.hostname}_{self.platform}'
            #self.client = kwargs.clientname = clientname

    def create_old(self, name, *args, **kwargs):
        '''
p4dbcon.create('testdb', **{'port': 'anastasia:1777',
                            'user': 'zerdlg',
                            'depot': '//p4db',
                            'client_root': '/Users/zerdlg/p4db',
                            ''})
        '''
        if (self.varsdef(name) is not None):
            print(f'CreateError:\n Name already exists "{name}" - use self.update({name}) instead')
        StopError = None

        (args, kwargs) = (Lst(args), Storage(self.fixkeys(**kwargs)))
        try:

            p4name = f'p4connection_{name}'
            self.user = kwargs.user
            self.port = kwargs.port
            self.depot = kwargs.depot or '//p4db'
            dbdir = os.path.abspath('/Users/gc/p4db/')
            if (not os.path.exists(dbdir)):
                try:
                    os.mkdir(dbdir)
                except Exception as err:
                    print(err)
            local_dbdata = f"{dbdir}/.dbdata"
            if (os.path.isfile(local_dbdata) is False):
                inittxt = '''[$File: //dev/p4dlg/libconnect/conP4db.py $] [$Change: 683 $] [$Revision: #8 $]\n[$DateTime: 2025/04/07 18:39:56 $]\n[$Author: mart $]\n'''
                oFile = open(local_dbdata, 'w')
                try:
                    oFile.write(inittxt)
                finally: oFile.close()
            dbname = kwargs.dbname
            self.client_root = kwargs.client_root or f"{dbdir}/{dbname}"
            clientname = f'{name}_{self.user}_{self.hostname}_{self.platform}'
            self.client = kwargs.clientname = clientname

            p4con = ObjP4(self.shellObj, loglevel='INFO').create(p4name, **kwargs)

            p4con.client(clientname, **{'Root': self.client_root})
            server_root = f"{self.depot}/{dbname}"
            if (p4con.files(f'{server_root}/.dbdata')(0) is None):
                p4con.add(local_dbdata)
                p4con.submit(**{'description': 'inital checkin'})

            kwargs.p4name = p4name
            kwargs.oSchema = p4con.oSchema
            kwargs.version = p4con.version
        except Exception as err:
            StopError = f"Error: {err}"
        if (StopError is not None):
            print(f'Error:\n{StopError}')
        else:
            self.varsdef(name, kwargs)
            self.setstored()
            '''  time to load the thing
            '''
            objp4db = self.load(name)
            return objp4db

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
        if (self.varsdef(name) is None):
            print(f'KeyError:\n No such key "{name}"')
        else:
            try:
                kwargs = self.varsdef(name)
                kwargs.update(**{'loglevel': self.loglevel})
                p4port = kwargs.port
                (connected, connect_msg) = self.is_connected(p4port)
                if (connected is True):
                    objp4db = Py4(**kwargs)
                    self.shellObj.kernel.shell.push({name: objp4db})
                    self.setstored()
                    print(f'Reference ({name}) loaded & connected to {p4port}')
                    return objp4db
                else:
                    bail(f'\nUnable to connect to {p4port}\n{connect_msg}')
            except Exception as err:
                bail(f'KeyError: \n{err}')

    def unload(self, name):
        if (self.varsdef(name) is None):
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
        if (self.varsdef(name) is None):
            print(f'KeyError:\nNo such key "{name}"')
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