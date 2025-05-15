import os, sys
from pprint import pformat
from libdlg.dlgStore import Storage
from libdlg.dlgOptions import ArgsParser
from libsh import shell

'''  [$File: //dev/p4dlg/dlg.py $] [$Change: 707 $] [$Revision: #7 $]
     [$DateTime: 2025/05/14 13:55:49 $]
     [$Author: zerdlg $]
'''

''' USAGE & REQUIREMENTS

    Requirements: qtconsole (I generally install Anaconda for 3rd party dependencies)
              
    %> python p4dlg.py shell
    
'''

def initprog(**opts):
    opts = Storage(opts)
    print(f'Processing subparser `{opts.which}` with the following cmd ine options:\n{pformat(opts)}')
    if (len(opts) > 0):
        subparser = opts.pop('which')
        if (subparser == 'shell'):
            shell.Serve()(**opts)
        #elif (subparser == 'bck'):
        #    Retype()(**opts)
        #elif (subparser == 'retype'):
        #    P4DBackup()(**opts)

if (__name__ == '__main__'):
    '''  cmd line options
    '''
    root_path = os.path.abspath('.')
    opts = Storage({'root_path': root_path})
    args = []
    oParser = ArgsParser()
    domainargs = oParser().parse_args(args if (len(sys.argv) == 1) else None)
    opts.update(**{k: v for (k, v) in dict(vars(domainargs)).items()}) #if (v not in ([None], [], None, False, 'unset'))})
    subparser_name = opts.which
    initprog(**opts)