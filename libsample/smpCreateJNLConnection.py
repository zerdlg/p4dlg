import os
from libdlg.dlgSchema import getObjSchema
from libjnl.jnlIO import P4Jnl

def jnlconnect(jnlfile, oSchema=None, version=None):

    (oSchema, version) = getObjSchema(jnlfile)
    oJnl = P4Jnl(jnlfile, oSchema, version)
    tables = oJnl.tables
    print(tables)

if (__name__ == '__main__'):
    jnlfile = os.path.abspath("../resc/journals/checkpoint.14")
    ''' Don't have the SchemaXML object or the version? 
        No problem! The path to the journal or checkpoint file might just be enough.
        Though, I suggest you pass it in rather than not.

        *Note: Though a checkpoint will provide the db.counters records required to guess
        the version, a journal may not... Just saying.
    '''
    jnlconnect(jnlfile)