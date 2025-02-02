import re

from libdlg.dlgStore import Lst, Storage, objectify
from libdlg.dlgUtilities import decode_bytes
from libdlg.dlgQuery_and_operators import AND, OR
from libpy4.py4SpecIO import SpecIO

'''  [$File: //dev/p4dlg/libpy4/py4Run.py $] [$Change: 474 $] [$Revision: #17 $]
     [$DateTime: 2024/09/09 06:39:06 $]
     [$Author: mart $]
'''

''' Usage example:

    objp4 = Py4(**kwargs)
    cmdargs = ['--user', 'bigbird', '--port', 'anastasia.local:1777']
    inforecord = Py4Run(objp4, *cmdargs, tablename='info')()(0)
'''


__all__ = ['Py4Run']

class Py4Run(object):
    def __repr__(self):
        return f'<Py4Run {self.tabledata.tablename}>'

    def __iter__(self):
        iter(self)

    def __init__(self, objp4, *args, **tabledata):
        ''' objp4 is an instance of class Py4
        '''
        self.objp4 = objp4
        cmdargs = Lst(args) or Lst(self.objp4.p4args)
        (
            self.options,
            self.tabledata
        ) = \
            (
                cmdargs,
                Storage(tabledata)
            )

        ''' Py4 does provide its own logger, if objp4 IS iinstance of Py4, then use it.
            other keep appending any msg to loglist (until time is made available to do 
            something not quite as dumb!
        '''
        self.loglist = []
        logger = self.objp4.logger \
            if (hasattr(self.objp4, 'logger')) \
            else self.tabledata.logger
        self.loginfo = logger.loginfo if (logger is not None) else self.loglist.append
        self.logwarning = logger.logwarning if (logger is not None) else self.loglist.append
        self.logerror = logger.logerror if (logger is not None) else self.loglist.append
        self.logcritical = logger.logcritical if (logger is not None) else self.loglist.append

    def __call__(self, *cmdargs, **cmdkwargs):
        (cmdargs, cmdkwargs) = (Lst(cmdargs), Storage(cmdkwargs))
        join_datachunks = cmdkwargs.joindata_chunks or False
        ''' tablename should exist in tablememo
        '''
        tablename = self.tabledata.tablename
        is_spec = self.tabledata.is_spec or False
        if AND(
                (is_spec is True),
                (tablename in self.objp4.spec_takes_no_lastarg)
        ):
            return
        ''' don't bother if this command doesn't take/need a last position arg 
        '''
        noargs_cmds = (
                self.objp4.nocommands
                + self.objp4.fetchfirst
                + self.objp4.spec_takes_no_lastarg
        )
        lastarg = None
        if (not tablename in noargs_cmds):
            (lastarg, cmdargs) = self.objp4.define_lastarg(tablename, *cmdargs)
        self.objp4.p4globals += self.objp4.supglobals
        ''' tablename must be cmdargs' 1st argument. so either insert 
            if missing or, if suspected to be in the wrong position, 
            pop it out, then insert it to the very left.
        '''
        if (tablename is not None):
            if (not tablename in cmdargs):
                cmdargs.insert(0, tablename)
            elif (cmdargs(0) != tablename):
                cmdargs.pop(cmdargs.index(tablename))
                cmdargs.insert(0, tablename)
            ''' Down this way if we have a spec...
            '''
            if AND(
                    (is_spec is True),
                    (not '--explain' in cmdargs)
            ):
                cmdargs += self.options
                ''' **cmdkwargs should contain only spec key/value pairs to create/update a spec.
                
                    >>> oP4.client(
                        '-S',
                        'other_client's_view', 
                        --force=True,
                        **{
                            'Client':'my_client',
                            'Root': '/home/gc/projects'
                            }
                        )
                '''
                (
                    specname,
                    specinput,
                    altarg
                ) = (
                    self.objp4.parseInputKeys(
                        self.tabledata,
                        lastarg,
                        **cmdkwargs
                    )
                )
                if (len(specinput) > 0):
                    cmdkwdict = {ckey.lower(): ckey for ckey in cmdkwargs.keys()}
                    ''' for whatever its worth... the same key can't exist in 
                        both cmdkwargs and in specinput. Delete from cmdkwargs
                        as needed
                    '''
                    for skey in specinput.keys():
                        if (skey.lower() in cmdkwdict.keys()):
                            rkey = cmdkwdict[skey.lower()]
                            cmdkwargs.delete(rkey)

                if AND(
                        (is_spec is True),
                        AND(
                            (specname is None),
                            (lastarg is not None)
                        )
                ):
                    specname = lastarg
                ''' Even if we want to create/update a spec, we will, in all cases, 
                    need -o to begin with, then can we look at -i if (cmdkwargs > 0)
                '''
                specargs = objectify(
                            {
                                    'o': {
                                          'short': '-o',
                                          'keyword': '--output'
                                },
                                    'i': {
                                          'short': '-i',
                                          'keyword': '--input'
                                    }
                            }
                )
                ''' at this time, let's remove -o/-i from cmdargs
                '''
                for sparg in specargs.keys():
                    poparg = cmdargs.index(specargs[sparg].short) \
                        if (specargs[sparg].short in cmdargs) \
                        else cmdargs.index(specargs[sparg].keyword) if (specargs[sparg].keyword in cmdargs) else None
                    if (poparg is not None):
                        cmdargs.pop(poparg)
                ''' specname *should* be cmdargs(-1) by now, 
                    otherwise we'll let the env deal with it.
                '''
                if (not cmdargs(-1).startswith('-')):
                    if OR(
                            (cmdargs(-1) != tablename),
                            (cmdargs(-1) == tablename == 'spec')
                    ):
                        ''' just guessing... as in %> p4 client -o myClientName
                        '''
                        if (specname is None):
                            specname = specinput[tablename] \
                                if (tablename in specinput.keys()) \
                                else cmdargs(-1)
                            if (not tablename in cmdargs):
                                cmdargs.insert(0, tablename)
                            if AND(
                                    (len(cmdargs) == 1),
                                    (specname == tablename == 'spec')
                            ):
                                cmdargs.append('spec')
                            elif (cmdargs(0) != tablename):
                                cmdargs.pop(cmdargs.index(tablename))
                                cmdargs.insert(0, tablename)
                ''' no matter what, be it input or output, 
                    we need a spec's output
                '''
                try:
                    return SpecIO(self.objp4).out(
                        tablename,
                        specname,
                        *cmdargs,
                        **cmdkwargs
                    )
                finally:
                    self.objp4.close()
        ''' does this cmd have and/or require a final cmd line 
            arg (like a filename or a spec name, etc.)?
        '''
        if (isinstance(lastarg, str) is True):
            ''' we have a required filename... pop it out if
                already in cmdargs & force any options into 
                cmdargs (but in order!). Then insert the 
                filename in cmdargs last position. 
            '''
            if (lastarg in cmdargs):
                cmdargs.pop(cmdargs.index(lastarg))
            if (lastarg in self.options):
                self.options.pop(self.options.index(lastarg))
            options = self.options.storageindex(reversed=True)
            [cmdargs.append(value) for (key, value) in options.items() \
                if (not value in cmdargs)]
            cmdargs.append(lastarg)
        ''' get ready to run the thing!
        '''
        try:
            oP4Globals = self.objp4.p4globals
            strGlobals = ''.join(oP4Globals)
            is_match = re.match(f'^{strGlobals}', ''.join(cmdargs))
            if (is_match is None):
                cmdargs = Lst(oP4Globals + cmdargs)
            if (not 'input' in cmdkwargs.keys()):
                if AND(
                      OR(
                            (tablename in self.objp4.nocommands),
                            ('--explain' in cmdargs)
                        ),
                      (tablename not in ('help'))
                ):
                    output = self.objp4.p4OutPut_noCommands(tablename, *cmdargs)
                else:
                    output = self.objp4.p4OutPut(
                                                    tablename,
                                                    *cmdargs,
                                                    **cmdkwargs
                    )

                    if AND(
                            (tablename == 'print'),
                            (len(output) >= 2)
                    ):
                        if (output(0).type not in (
                                'binary',
                                'symlink',
                                'apple',
                                'resource'
                        )
                        ):
                            if (not None in (
                                            output(1).code,
                                            output(1).data
                                        )
                            ):
                                ''' decision: join or don't join data chunks 
                                    *** think about the size of the combined chunks of text
                                    
                                    Of course, said datachunks need only apply to text files (omit bins)
                                '''
                                if (join_datachunks is True):
                                    metadata = output(0)
                                    data = ''
                                    for out_chunk in output[1:]:
                                        data += out_chunk.data
                                    output = Storage(
                                        {
                                            'code': metadata,
                                            'data': data
                                        }
                                    )
                    if AND(
                            (tablename in self.objp4.nocommands),
                            (type(output).__name__ == 'DLGRecords')
                    ):
                        if (len(output) == 1):
                            output = output(0)
                            if (hasattr(output, 'data') is True):
                                output = output.data
            else:
                cmdargs.pop(cmdargs.index('-G'))
                output = self.objp4.p4Input(tablename, *cmdargs, **cmdkwargs)
            output = decode_bytes(output)
            return output
        finally:
            self.objp4.close()
