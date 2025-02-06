from libdlg.dlgQuery_and_operators import AND
from libpy4.py4IO import Py4
from libdlg import SchemaXML, to_releasename
from libdlg.dlgStore import Storage, Lst

'''  [$File: //dev/p4dlg/libsample/smpRetype.py $] [$Change: 466 $] [$Revision: #11 $]
     [$DateTime: 2024/08/23 04:23:28 $]
     [$Author: zerdlg $]
'''

__all__ = ['Retype']

class Retype(object):
    ''' USAGE:

        >>> qry1 = AND(
                        (oP4.files.action.belongs(('add', 'edit'))),
                        (oP4.files.type != 'ktext')
                        )
        >>> targetfiles = oP4(qry1)._select(oP4.files.depotFile)
        >>> filelist = oP4(oP4.files.depotFile.belongs(targetfiles)).select('depotFile')
        >>> result = oP4().edit(('--type', 'ktext', *filelist))

    '''

    def __init__(self, **kwargs):
        ''' Create a reference to class Py4
        '''
        self.oP4 = Py4(**kwargs)

    def __call__(self, preview=False):
        ''' Retype all added | edited python files that have not been previously typed as ktext.
        '''
        oP4 = self.oP4
        ''' 1. A query to find all python files where the filetype is not 'ktext` 
               and where field `action` has a value of either an `add` or an `edit`. 
        '''
        qry1 = AND(
                    AND(
                        (oP4.files.action.belongs(('add', 'edit'))),
                        (oP4.files.type != 'ktext')
            ),
            (oP4.files.depotFile.endswith('.py'))
        )
        targetfiles = oP4(qry1)._select(oP4.files.depotFile)
        cmdargs = [
                    '-t',
                    'ktext',
                    *targetfiles
                ]
        if (preview is True):
            cmdargs.insert(0, '--preview', )
        try:

            oP4.change(**{'description':'just a test changelist!'})
            oP4.change()
            oP4.edit(*cmdargs)

        except Exception as err:
            print(err)

def _retype(**kwargs):
    kwargs = Storage(kwargs)
    preview = False
    if (kwargs.preview is not None):
        preview = kwargs.pop('preview')
    oP4 = Py4(**kwargs)
    ''' 1. A query to find all python files where the filetype is not 'ktext` 
                   and where field `action` has a value of either an `add` or an `edit`. 
            '''
    qry = AND(
        AND(
            (oP4.files.action.belongs(('add', 'edit'))),
            (oP4.files.type != 'ktext')
        ),
        (oP4.files.depotFile.endswith('.py'))
    )
    targetfiles = oP4(qry)._select(oP4.files.depotFile)

    cmdargs = [
        '-t',
        'ktext',
        *targetfiles
    ]

    try:
        change = oP4.change(**{'description': 'just a test changelist!'})
        cmdargs.extend(['--change', change])
        if (preview is True):
            cmdargs.insert(0, '--preview')
        else:
            oP4.edit(*cmdargs)
            submit = oP4.submit(change)

    except Exception as err:
        print(err)



if (__name__ == '__main__'):
    p4args = {
        'user': 'zerdlg',
        'port': 'anastasia.local:1777',
        'client': 'computer_p4dlg',
        'preview': True
    }
    #results = Retype(**p4args)(preview=True)
    results = _retype(**p4args)
    for res in results:
        print(res)