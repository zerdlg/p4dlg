from libdlg.dlgSchema import *
from libdlg.dlgTables import *

def create_reference(version):
    ''' oSchema exposes 2 distinct class references;     
            `p4schema`  -> provides access the published p4db schema
                           as dictionary with object-like attributes. 
            `p4model`   -> provides access to a shuffled & modified
                           schema suited for building a DB model of
                           the p4 db.

            type >>> pprint(p4schema) & >>> pprint(p4model) to view their contents.
                           
        It also exposes functions needed to get & manage xml formatted schemas 
        locally on this system. This is achieved by invoking the `update_xmlschemas`
        method. We can control tasks and behaviours by modifying values to 3 attributes:
    '''

    oSchema = SchemaXML()(version) # pass the release version you want in the __call__ method
                                   # if you need to access the Schema directly
    p4schema = oSchema.p4schema
    p4model = oSchema.p4model

if (__name__ == '__main__'):
    create_reference('r16.2')