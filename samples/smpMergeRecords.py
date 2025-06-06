from os.path import dirname

from libsql.sqlQuery import AND
from libjnl.jnlIO import P4Jnl
from libsql.sqlRecord import Record
from libsql.sqlSchema import SchemaXML, to_releasename
from libdlg.dlgStore import Storage, Lst
import resc.journals as journals

'''  [$File: //dev/p4dlg/samples/smpMergeRecords.py $] [$Change: 707 $] [$Revision: #11 $]
     [$DateTime: 2025/05/14 13:55:49 $]
     [$Author: zerdlg $]
'''

__all__ = ['Merge']

class  Merge(object):
    ''' USAGE:

        >>> qry1 = AND(
                        (oP4.files.action.belongs(('add', 'edit'))),
                        (oP4.files.type != 'ktext')
                        )
        >>> targetfiles = oP4(qry1)._select(oP4.files.depotFile)
        >>> filelist = oP4(oP4.files.depotFile.belongs(targetfiles)).select('depotFile')
        >>> result = oP4().edit(('--type', 'ktext', *filelist))

    '''

    def __init__(self, *args, version="r16.2", **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        schemaversion = to_releasename(version)
        oSchema = SchemaXML(schemaversion)
        journal = args(0)
        ''' Create a reference to class P4Jnl
        '''
        self.oJnl = P4Jnl(journal, oSchema)

    def __call__(self, *args, **kwargs):
        oJnl = self.oJnl
        files = oJnl(oJnl.revdx.idx > 0).select()
        gfiles = files.groupby('change', orderby='depotRev', flat=True)
        for cl in gfiles:
            if (len(gfiles[cl]) <= 30):
                change = oJnl(oJnl.change.change == cl).fetch()
                records = gfiles[cl].limitby((0,10))
                for rec in records:
                    for (key, value) in change.items():
                        rec.merge({'change': {key: value}})
                print(rec.change.change)
                Record(rec).datatable()

                #for rec in records:
                #    record = Record(rec.merge(change))
                #    record.datatable()

if (__name__ == '__main__'):
    journaldir = dirname(journals.__file__)
    journal = f'{journaldir}/journal.8'
    result = Merge(journal,  version='r16.2')()
    print(result)