from os.path import dirname
from pprint import pprint
from libdlg.dlgQuery_and_operators import AND
from libjnl.jnlIO import P4Jnl
from libdlg import SchemaXML, to_releasename, DLGRecord, DLGRecords
from libdlg.dlgStore import Storage, Lst
import schemaxml
import resc.journals as journals

'''  [$File: //dev/p4dlg/libsample/smpRetype.py $] [$Change: 466 $] [$Revision: #11 $]
     [$DateTime: 2024/08/23 04:23:28 $]
     [$Author: mart $]
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
        schemadir = dirname(schemaxml.__file__)
        schemaversion = to_releasename(version)
        objSchema = SchemaXML(schemadir)
        oSchema = objSchema(schemaversion)
        journal = args(0)
        ''' Create a reference to class P4Jnl
        '''
        self.oJnl = P4Jnl(journal, oSchema)

    def __call__(self, *args, **kwargs):
        oJnl = self.oJnl
        files = oJnl(oJnl.revdx.idx > 0).select()
        gfiles = files.groupby('change', orderby='depotRev', groupdict=True)
        for cl in gfiles:
            if (len(gfiles[cl]) <= 30):
                change = oJnl(oJnl.change.change == cl).fetch()
                records = gfiles[cl].limitby((0,10))
                for rec in records:
                    for (key, value) in change.items():
                        rec.merge({f'change.{key}': value})
                print(rec.change.change)
                DLGRecord(rec).datatable()

                #for rec in records:
                #    record = DLGRecord(rec.merge(change))
                #    record.datatable()

if (__name__ == '__main__'):
    journaldir = dirname(journals.__file__)
    journal = f'{journaldir}/journal.8'
    result = Merge(journal,  version='r16.2')()
    print(result)