from subprocess import (
    Popen,
    STDOUT,
    PIPE
)

from libdlg.dlgStore import Storage
from libdlg.dlgUtilities import decode_bytes

'''  [$File: //dev/p4dlg/libsample/smpP4DBackup.py $] [$Change: 461 $] [$Revision: #5 $]
     [$DateTime: 2024/08/09 18:15:55 $]
     [$Author: zerdlg $]
'''

# not really a sample, but it is helpful.

__all__ = ['p4d_backup']

def exec_backup(*cmdargs):
    oFile = Popen(*cmdargs, stdout=PIPE, stderr=STDOUT)
    try:
        if (hasattr(oFile, 'stdout')):
            (outFile, errFile) = oFile.communicate()
            out = outFile if (len(outFile) > 0) else errFile
            return decode_bytes(out)
    except Exception as err:
        return err
    finally:
        if (hasattr(oFile, 'close')):
            oFile.close()

def p4d_backup(**opts):
    ''' build a cmd line that looks like this:

                %> scp -r pi@anastasia.local:~/p4dinst ~/anastasia_p4dinst_backup

            using these options:
            +-------------------+-------------------+
            |       src         |       dst         |
            +-------------------+-------------------+
            | local_src_user    | local_dst_user    |
            | local_src_server  | xxxxxxxxxxxxxxxxx |
            | local_src_dir     | local_dst_dir     |
            +-------------------+-------------------+
            | remote_src_user   | remote_dst_user   |
            | remote_src_server | remote_dst_server |
            | remote_src_dir    | remote_dst_dir    |
            +-------------------+-------------------+

        my own default values:
    '''
    opts = Storage(opts)
    ''' expected options
    '''
    recurse = opts.recurse or True
    ''' remote src options 
    '''
    remote_src_user = opts.remote_src_user
    remote_src_server = opts.remote_src_server
    remote_src_dir = opts.remote_src_dir
    ''' remote dst options
    '''
    remote_dst_user = opts.remote_dst_user
    remote_dst_server = opts.remote_dst_server
    remote_dst_dir = opts.remote_dst_dir
    ''' local src options  
    '''
    local_src_user = opts.local_src_user        # shouldn't be needed
    local_src_dir = opts.local_src_dir
    ''' local dst options
    '''
    local_dst_dir = opts.local_dst_dir
    ''' start building the src str
    '''
    cmdargs = ['scp']
    ''' iterate options and build the backup args accordingly
    '''
    if (recurse is True):
        cmdargs.append('-r')

    if (not None in (
            remote_src_user,
            remote_src_server,
            remote_src_dir,
            local_dst_dir
        )
    ):
        ''' scp from remote server to a local directory

            eg.    %> scp -r pi@anastasia.local:~/p4dinst ~/anastasia_p4dinst_backup'
        '''
        srcargs = [
            remote_src_user,
            '@',
            remote_src_server,
            ':',
            remote_src_dir
        ]
        dstargs = [local_dst_dir]

    elif (not None in (
            remote_src_user,
            remote_src_server,
            remote_src_dir,
            remote_dst_user,
            remote_dst_server,
            remote_dst_dir
        )
    ):
        ''' scp from a remote server to a directory on a remote server 

            eg.    %> scp -r pi@anastasia.local:~/p4dinst gc@computer.local:~/anastasia_p4dinst_backup
        '''
        srcargs = [
            remote_src_user,
            '@',
            remote_src_server,
            ':',
            remote_src_dir
        ]
        dstargs = [
            remote_dst_user,
            '@',
            remote_dst_server,
            ':',
            remote_dst_dir
        ]

    elif (not None in (local_src_dir, local_dst_dir)):
        ''' scp from local directory to a local directory 

            eg.    %> scp -r ~/p4dinst ~/anastasia_p4dinst_backup
        '''
        (scrargs, dstargs) = ([local_src_dir], [local_dst_dir])

    elif (not None in (local_src_dir, remote_dst_dir)):
        ''' scp from local directory to a local directory 

            eg.    %> scp -r ~/p4dinst gc@computer.local:~/anastasia_p4dinst_backup
        '''
        (scrargs, dstargs) = (
            [local_src_dir], [
                remote_dst_user,
                '@',
                remote_dst_server,
                ':',
                remote_dst_dir
            ]
                              )

    else:
        print('NO CAN DO SCP! - SOL :(')
    ''' put it together & execute.
    '''
    cmdargs += (srcargs + dstargs)
    print(f"Secure SCP transfer: {''.join(srcargs)} {''.join(dstargs)}")
    return exec_backup(*cmdargs)

def backup(**kwargs):
    kwargs = Storage(kwargs)

    ''' transfer from remote to local
    '''
    opts = {'recurse': kwargs.recurse or True,
            'local_src_dir': kwargs.remote_src_dir or '~/p4dinst/2015.2',
            'remote_src_user': kwargs.remote_src_user or 'pi',
            'remote_src_server': kwargs.remote_src_server or 'anastasia.local',
            'remote_src_dir': kwargs.remote_src_dir or '~/anastasia_p4dinst_backup'}
    p4d_backup(**opts)

if (__name__ == '__main__'):
    backup()