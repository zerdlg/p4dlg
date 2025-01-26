from os.path import dirname

from libconnect.conP4 import ObjP4
from libsh.shell import ServeLib
from libpy4.py4IO import Py4

from libdlg.dlgSchema import SchemaXML, to_releasename

''' a few ways to connect

    1. import p4qshell and start scripting as though you were typing in a console window (yeah, weird but efficient) 
    2. import Py4, create an object, ...
'''

def create_p4connect_p4qshell(name, version):
    oShell = ServeLib()()                               # reference to class DLGShell
    oSchema = oShell.schema(version)                    # deserialize a p4 schema of this release
    p4user = {
            'user': 'gc',                               # the user that will connect to p4d
            'port': 'anastasia.local:1777',
            'client': 'computer_p4q',
            'oSchema': oSchema
    }
    ''' create a p4 connection by importing p4qshell (ServerLib)
    '''
    return ObjP4(oShell, loglevel='INFO').create(name, **p4user)

def create_p4connect_Py4(version):
    schemaversion = to_releasename(version)             # format the given p4 release version
    oSchema = SchemaXML(schemaversion)                    # a reference to class SchemaXML
    p4args = {                                          # the p4 user
        'user': 'gc',
        'port': 'anastasia.local:1777',
        'client': 'computer_p4q',
        'oSchema': oSchema
    }
    ''' Create a p4 connection by creating a reference to class Py4
    '''
    return Py4(**p4args)


def main():
    (name, version) = ('oP4', 'r15.2')

    shell_p4 = create_p4connect_p4qshell(name, version)
    print(shell_p4)
    ''' output:
    
    CreateError:                                                    # doesn't create if already exist, but will load 
    Name already exists "oP4" - use op4.update(oP4) instead        
    <Py4 anastasia.local:1777 >                                     # the p4 connection has still been loaded
    '''
    py4_p4 = create_p4connect_Py4(version)
    print(py4_p4)
    ''' output:
    
    Reference (oP4) loaded & connected to anastasia.local:1777
    <Py4 anastasia.local:1777 >                                     # the p4 connection has still been loaded
    '''

if (__name__ == '__main__'):
    main()