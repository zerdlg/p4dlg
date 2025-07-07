import os
from os import path

from subprocess import (
    Popen,
    PIPE
)

from libdlg.dlgStore import Storage
from libdlg.dlgUtilities import decode_bytes

'''  [$File: //dev/p4dlg/sample/test_smpP4DBackup.py $] [$Change: 461 $] [$Revision: #5 $]
     [$DateTime: 2024/08/09 18:15:55 $]
     [$Author: zerdlg $]
'''

# not really a sample, but it is helpful.

__all__ = ['p4d_backup']

def exec_backup(*cmdargs):
    oFile = Popen(cmdargs, stdout=PIPE, stderr=PIPE)
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
    recurse = opts.recurse
    ''' if recurse is False, then we should have filanme values (src & dst).
    '''
    remote_src_filename = opts.remote_src_filename
    remote_dst_filename = opts.remote_dst_filename
    local_src_filename = opts.local_src_filename
    local_dst_filename = opts.local_dst_filename
    ''' remote src options 
    '''
    remote_src_user = opts.remote_src_user
    remote_src_server = opts.remote_src_server
    remote_src = opts.remote_src_dir
    if (remote_src_filename is not None):
        remote_src = path.join(remote_src, remote_src_filename)
    ''' remote dst options
    '''
    remote_dst_user = opts.remote_dst_user
    remote_dst_server = opts.remote_dst_server
    remote_dst = opts.remote_dst_dir
    if (remote_dst_filename is not None):
        remote_dst = path.join(remote_dst, remote_dst_filename)
    ''' local src options  
    '''
    local_src_user = opts.local_src_user
    local_src = opts.local_src_dir
    if (local_src_filename is not None):
        local_src = path.join(local_src, local_src_filename)
    ''' local dst options
    '''
    local_dst = opts.local_dst_dir
    if (local_dst_filename is not None):
        local_dst = path.join(local_dst, local_dst_filename)
    ''' start building the src str
    '''
    pwdfile = path.abspath('../scp_passwd')
    cmdargs = ['scp']
    ''' iterate options and build the backup args accordingly
    '''
    if (recurse is True):
        cmdargs.append('-r')

    if (not None in (
            remote_src_user,
            remote_src_server,
            remote_src,
            local_dst
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
            remote_src
        ]
        dstargs = [local_dst]

    elif (not None in (
            remote_src_user,
            remote_src_server,
            remote_src,
            remote_dst_user,
            remote_dst_server,
            remote_dst
        )
    ):
        ''' scp from a remote server to a directory on a remote server 

            eg.    %> scp -r pi@anastasia.local:~/p4dinst gc@computer.local:~/anastasia_p4dinst_backup
        '''
        srcargs = \
            [
                remote_src_user,
                '@',
                remote_src_server,
                ':',
                remote_src
            ]
        dstargs = \
            [
                remote_dst_user,
                '@',
                remote_dst_server,
                ':',
                remote_dst
            ]

    elif (not None in (local_src, local_dst)):
        ''' scp from local directory to a local directory 

            eg.    %> scp -r ~/p4dinst ~/anastasia_p4dinst_backup
        '''
        (scrargs, dstargs) = ([local_src], [local_dst])

    elif (not None in (local_src, remote_dst)):
        ''' scp from local directory to a local directory 

            eg.    %> scp -r ~/p4dinst gc@computer.local:~/anastasia_p4dinst_backup
        '''
        (scrargs, dstargs) = \
            (
                [local_src],
            [
                remote_dst_user,
                '@',
                remote_dst_server,
                ':',
                remote_dst
            ]
            )

    else:
        print('SCP, NO CAN DO! - SOL :(')
    ''' put it together & execute.
    '''
    #cmdargs += (srcargs + dstargs)
    cmdargs += (''.join(srcargs), ''.join(dstargs))
    full_cmdline = f"Secure CP transfer: {' '.join(cmdargs)}"#{''.join(srcargs)} {''.join(dstargs)}"
    print(full_cmdline)
    return exec_backup(*cmdargs)

def backup(**kwargs):
    kwargs = Storage(kwargs)

    ''' transfer a journal from remote to local
    '''
    filename = 'checkpoint.16'
    opts = {'recurse': False,
            'remote_src_filename': kwargs.remote_src_filename or filename,
            'remote_dst_filename': None,
            'local_src_filename': None,
            'local_dst_filename': kwargs.local_dst_filename or filename,
            'local_src_dir': None,
            'local_dst_dir': kwargs.local_dst_dir or '/Users/gc/anastasia/dev/p4dlg/resc/journals',
            'remote_src_user': kwargs.remote_src_user or 'pi',
            'remote_src_server': kwargs.remote_src_server or 'anastasia.local',
            'remote_src_dir': kwargs.remote_src_dir or '/home/pi/p4dinst'}
    p4d_backup(**opts)

if (__name__ == '__main__'):
    backup()