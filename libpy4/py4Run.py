import re

from libdlg.dlgStore import Lst, ZDict, objectify
from libdlg.dlgUtilities import decode_bytes, ALLLOWER
from libpy4.py4SpecIO import SpecIO

'''  [$File: //dev/p4dlg/libpy4/py4Run.py $] [$Change: 680 $] [$Revision: #27 $]
     [$DateTime: 2025/04/07 07:06:36 $]
     [$Author: zerdlg $]
'''

''' Usage example:

    objp4 = Py4(**kwargs)
    cmdargs = ['--user', 'zerdlg', '--port', 'anastasia.local:1777']
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
                ZDict(tabledata)
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

    def spec(self, tablename, *cmdargs, **kwargs):
        pass

    def __call__(self, *cmdargs, **cmdkwargs):
        (cmdargs, cmdkwargs) = (Lst(cmdargs), ZDict(cmdkwargs))
        join_datachunks = cmdkwargs.joindata_chunks or False
        ''' tablename should exist in tablememo
        '''
        tablename = self.tabledata.tablename
        is_spec = self.tabledata.is_spec or False
        if (
                (is_spec is True) &
                (tablename in self.objp4.spec_takes_no_lastarg)
        ):
            ''' don't bother if this command doesn't take/need a last position arg 
            '''
            return

        noargs_cmds = Lst(
            set(
                self.objp4.nocommands +
                self.objp4.fetchfirst +
                self.objp4.spec_takes_no_lastarg + ['submit']
            )
        )
        lastarg = None

        if (not tablename in noargs_cmds):
            (lastarg, cmdargs, noqry) = self.objp4.define_lastarg(tablename, *cmdargs)

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
            if (
                    (is_spec is True) &
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
                # BUG: if (lastarg is None AND cmdargs is empty):
                #          specname should not equal tablename!
                #   i.e.: p4.client()
                #
                #   implemented in py4IO at line 906
                #
                #   should also unmangle the specname spectype vs
                #   tablename fiasco. even though the results are
                #   good (it's just too messy!)
                #
                (specname, specinput, altarg) = (
                    self.objp4.parseInputKeys(
                        self.tabledata,
                        lastarg,
                        *cmdargs,
                        **cmdkwargs
                    )
                )
                if (len(specinput) > 0):
                    ''' for whatever its worth... the same key can't exist in 
                        both cmdkwargs and in specinput. Delete from cmdkwargs
                        as needed
                    '''

                    [cmdkwargs.delete(ckey) for ckey in cmdkwargs.copy().keys() if
                     (ckey.lower() in [key.lower() for key in specinput.keys()])]

                ''' temp fix - need o fix this issue in parseInputKeys (or before)
                '''
                if (specname == tablename):
                    specname = None

                ''' input (-i) can't happen without output (-o),
                    remove those io flags for now.
                '''
                io_option = cmdargs.intersect(['-o', '--output', '-i', '--input'])(0)
                if (io_option is not None):
                    idx = cmdargs.index(io_option)
                    cmdargs.pop(idx)
                ''' 'p4 spec <specname>' - source of ambiguity?
                    no doubt.
                '''
                if (not cmdargs(-1).startswith('-')):
                    if (
                            (cmdargs(-1) != tablename) |
                            (cmdargs(-1) == tablename == 'spec')
                    ):
                        if (not tablename in cmdargs):
                            cmdargs.insert(0, tablename)
                        if (
                                (len(cmdargs) == 1) &
                                (specname == tablename == 'spec')
                        ):
                            cmdargs.append('spec')
                        elif (cmdargs(0) != tablename):
                            cmdargs.pop(cmdargs.index(tablename))
                            cmdargs.insert(0, tablename)
                        elif (len(cmdargs) > 0):
                           if (tablename == 'spec'):
                               if (cmdargs(-1) != 'spec'):
                                   if (cmdargs(-1) in self.objp4.p4spec):
                                       if (tablename not in cmdargs):
                                           cmdargs.insert(-2, tablename)
                if (lastarg is not None):
                    cmdargs.append(lastarg)
                ''' no matter what, be it input or output, 
                    we need a spec's output
                '''
                try:
                    specres = SpecIO(self.objp4).out(
                        tablename,
                        lastarg,
                        *cmdargs,
                        **specinput#**cmdkwargs
                    )
                    return specres
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
                if (
                        (
                            (tablename in self.objp4.nocommands) |
                            ('--explain' in cmdargs)
                        ) &
                        (tablename not in ('help'))
                ):
                    output = self.objp4.p4OutPut_noCommands(tablename, *cmdargs)
                else:
                    output = self.objp4.p4OutPut(
                                                    tablename,
                                                    *cmdargs,
                                                    **cmdkwargs
                    )

                    if (
                            (tablename == 'print') &
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
                                    output = ZDict(
                                        {
                                            'code': metadata,
                                            'data': data
                                        }
                                    )
                    if (
                            (tablename in self.objp4.nocommands) &
                            (type(output).__name__ == 'Records')
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
