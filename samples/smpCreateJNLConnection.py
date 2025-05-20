import os
from libsql.sqlSchema import get_schemaObject
from libjnl.jnlIO import P4Jnl

def jnlconnect(jnlfile, oSchema=None, version=None):
    ''' making use of get_schemaObject...

            The goal is to create a reference to class P4Jnl so as to parse/query a perforce journal file,
            and/or to update/delete records. That said, to do so we need a jnl file (the path) and a schema
            to help us navigate the file.

            the get_schemaObject helper takes in a journal as first param, and, optionally, the schema object as
            the 2nd param (if we have it) and the version as the 3rd param (if we have it).

        Guessing the schema's release version...

            If the target jnl file is a checkpoint, then we can get away with only knowing the path to the
            target file because the version can be calculated by seeking out a db.counters record that holds
            the schema's upgrade value which can be used to figure out the version it relates to. And, with
            release version in hand, we can instanciate the correct reference to class SchemaXML.

            However, we will likely not be as lucky if the target jnl file is a journal file. In this case, we
            will need to provide jnlconnect with either the SchemaXML object or perforce release that created
            the journal.

            get_schemaObject() will always return a tuple with a length of 2 (oSchema, version). It is also
            possible that getObjChema() can figure it out, in which case (None, None) is returned.


    '''
    (oSchema, version) = get_schemaObject(jnlfile, oSchema=oSchema, version=version)
    ''' oJnl is the SchemaXML cal reference - we're ready to go!
    '''
    oJnl = P4Jnl(jnlfile, oSchema, version)
    return oJnl

if (__name__ == '__main__'):
    jnlfile = os.path.abspath("../resc/journals/checkpoint.14")
    oJnl = jnlconnect(jnlfile)

    ''' making sure it is in working order
    '''
    tables = oJnl.tables
    print(tables)