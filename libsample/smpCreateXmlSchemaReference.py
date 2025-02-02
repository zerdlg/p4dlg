from libdlg.dlgSchema import *
from libdlg.dlgTables import *

def create_reference(version):
    ''' oSchema exposes 2 distinct class references;     
            `p4schema`  -> provides access the published p4db schema
                           as dictionary with object-like attributes. 
            `p4model`   -> provides access to a shuffled & modified
                           schema suited for building a DB model of
                           the p4 db.
    '''
    return SchemaXML(version)

if (__name__ == '__main__'):
    oSchema = create_reference('r16.2')