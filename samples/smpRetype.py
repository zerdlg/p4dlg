import re
from pprint import pprint
from libsql.sqlQuery import AND
from libpy4.py4IO import Py4
from libdlg.dlgStore import Storage, Lst

'''  [$File: //dev/p4dlg/samples/smpRetype.py $] [$Change: 717 $] [$Revision: #17 $]
     [$DateTime: 2025/05/15 11:21:30 $]
     [$Author: zerdlg $]
'''

__all__ = ['retype']

def retype(**kwargs):
    kwargs = Storage(kwargs)
    preview = kwargs.pop('preview')
    oP4 = Py4(**kwargs)

    ''' 1. A query to find all python files where the filetype is not 'ktext` 
           and where field `action` is either `add` or `edit`. 
    '''
    qry = (
            (oP4.files.action.belongs(('add', 'edit'))) &
            (oP4.files.type != 'ktext') &
            (oP4.files.depotFile.endswith('.py'))
    )

    ''' The files that meet the above criteria 
    '''
    targetfiles = oP4(qry)._select(oP4.files.depotFile)

    cmdargs_retype = [
        '-t',
        'ktext',
        *targetfiles
    ]
    try:
        change_record = oP4.change(**{'description': 'Retyping modules to ktext.'})
        change = Lst(re.split('\s',change_record.data))(1)
        [cmdargs_retype.insert(0, option) for option in [change, '--change']]
        if (preview is True):
            cmdargs_retype.insert(0, '--preview')
        edit = oP4.edit(*cmdargs_retype)
        submit_args = ['--change', change]
        submit = f"\nwould have submited {len(targetfiles)} files." \
            if (preview is True) \
            else oP4.submit(*submit_args)
        return (change_record, edit, submit, targetfiles)
    except Exception as err:
        print(err)

if (__name__ == '__main__'):
    p4args = Storage(
        {
        'user': 'zerdlg',
        'port': 'anastasia.local:1777',
        'client': 'computer_p4dlg',
        'preview': False
        }
    )
    (
        change_record,
        edit_records,
        submit_record,
        targetfiles
    ) = (
        retype(**p4args)
    )
    if (p4args.preview is False):
        print('\nchange record:')
        pprint(change_record)
        print('\nedit records:')
        pprint(edit_records.records)
        print('\nsubmit records:')
        pprint(submit_record.records)
    else:
        print('\npreview:')
        print(change_record, '\n', edit_records, submit_record)
    print('\naffected files:')
    pprint(targetfiles)
