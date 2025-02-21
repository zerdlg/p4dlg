import re

from libdlg.dlgStore import *
from libsql.sqlRecord import Record
from libdlg.dlgUtilities import Flatten, bail
from libpy4.py4Mapping import P4Mapping

class SpecIO(object):
    def __init__(self, objp4):
        self.objp4 = objp4

    def __call__(self, *args, **kwargs):
        return self

    def out(
            self,
            tablename,
            lastarg,
            *cmdargs,
            **specinput
    ):
        (cmdargs, specinput) = (Lst(cmdargs), ZDict(specinput))
        tabledata = self.objp4.memoizetable(tablename)
        fieldsmap = tabledata.fieldsmap
        outputargs = Lst(self.objp4.p4globals + cmdargs.copy())
        inputargs = Lst(self.objp4.p4globals + cmdargs.copy())

        ''' what do we have? --output, --input or --delete?
        '''
        (
            is_output,
            is_delete,
            requires_force
        ) \
            = (
            False,
            False,
            False
        )

        is_input = True \
            if (len(specinput) > 0) \
            else False
        if (is_input is False):
            dsum = sum(
                [
                    ('-d' in outputargs),
                    ('--delete' in outputargs)
                ]
            )
            is_output = False \
                if (dsum > 0) \
                else True
            is_delete = False \
                if (
                (
                        (is_input is True) |
                        (is_output is True)
                ) &
                (dsum == 0)
            ) \
                else True

            ''' TODO: need more rules for requires_force
            '''
            requires_force = True if (tablename != 'spec') else False
        ''' for now, force self._<spec> on specname... though it would best to 
            make sure the previous command resets it's specname. Anyways ... 
        '''
        if (not True in (
                is_input,
                is_output,
                is_delete
        )
        ):
            is_output = True

        p4globals = ''.join(self.objp4.p4globals)
        for argsitem in (outputargs, inputargs):
            argsitem = argsitem.clean()
            is_match = (re.match(f'^{p4globals}', ''.join(argsitem)) is not None)
            if (is_match is False):
                self.objp4.p4globals += argsitem
        has_outputkey = (
            ('-o' in outputargs) |
            ('--output' in outputargs)
        )
        has_forcekey = (
            ('-f' in outputargs) |
            ('--force' in outputargs)
        )
        if (is_delete is True):
            if (not True in has_forcekey):
                idx = (outputargs.index('-d') + 1) \
                    if ('-d' in outputargs) \
                    else (outputargs.index('--delete') + 1) \
                    if ('--delete' in outputargs) \
                    else -1
                if (lastarg == outputargs(-1)):
                    outputargs.insert(idx, '--force')
                else:
                    outputargs.append('--force')
        elif (
                (
                        (is_output is True) |
                        (is_input is True)
                ) &
                (has_outputkey is False)
        ):
            ''' make sure the output args endswith [tablename, '--output', specname]
            
                but, rule out that tablename is 'spec', otherwise handle it. 
            '''
            if (tablename == 'spec'):
                if (outputargs(-1) != tablename):
                    if (outputargs(-1) in self.objp4.p4spec):
                        outputargs.insert(-1, '--output')
            elif (lastarg == tablename):
                if (outputargs.count(tablename) > 1):
                    outputargs.insert(-1, '--output')
                else:
                    outputargs.append('--output')
                    if (lastarg is not None):
                        outputargs.append(lastarg)
            elif (tablename == outputargs(-1)):
                outputargs.append('--output')
                if (
                        (lastarg not in outputargs) &
                        (lastarg is not None)
                ):
                    outputargs.append(lastarg)
            elif (
                    (lastarg is not None) &
                    (tablename == outputargs(-2)) &
                    (lastarg == outputargs(-1))
            ):
                outputargs.insert(-1, '--output')
        try:
            out = self.objp4.p4OutPut(tablename, *outputargs)
            outrecord = Lst(out)(0)
            if (type(outrecord) is Lst):
                outrecord = outrecord(0) or ZDict()
            if (outrecord not in (Lst(), None)):
                err_record_keys = Lst('data', 'severity').intersect(outrecord.keys())
                if (len(err_record_keys) > 0):
                    return (outrecord)
            if (type(outrecord).__name__ == 'Record'):
                outrecord = outrecord.as_dict()
            '''     we will likely need to flatten a record's fields when using -G. I.e.:

                    convert
                               {...,
                                'View1': '//depot/bla/...',
                                'View2': '//depot/blabla/...', 
                                'View3': '//depot/blablabla/...'
                               ...}
                    to
                               {...,
                                'View': ['//depot/bla/...',
                                         '//depot/blabla/...', 
                                         '//depot/blablabla/...'],
                               ...}

                    Usage example just below
            '''
            oFlat = Flatten(**outrecord)
            numfields = oFlat.getfields()
            outrecord = oFlat.reduce()
            ''' no user input, get out now
            '''
            if (is_input is False):
                return Record(outrecord)
        finally:
            self.objp4.close()
        ''' To save a new or updated spec, the output becomes the input's base record (except for change). 
        '''
        if (lastarg is not None):
            if (cmdargs(-1) == lastarg):
                cmdargs.pop(-1)
        specinputcopy = ZDict(specinput.copy())
        ''' - cleanup p4d-generated & managed field values
            - remove read-only fields from input spec                    
            - add the --input flag to inputargs
            - fix fieldnames (as per fieldsmap)
            - convert list'ed values as unique & numbered keys in inputspec
        '''
        [specinputcopy.delete(key) for key in specinput.keys() \
         if key.lower() in ('access', 'update', 'code')]

        ''' THIS BLOC NEEDS WORK!!!
        '''
        if (
                (tablename in ('user',)) |
                (requires_force is True)
        ):
            if (not '-f' in inputargs):
                inputargs.append('-f')
        ''' TODO: needs an options playbook ...
        '''
        inputargs.append('--input')
        for (leftkey, leftvalue) in specinputcopy.items():
            rightkey = fieldsmap[leftkey.lower()]
            rightvalue = outrecord[rightkey]
            if (
                    (isinstance(rightvalue, list)) &
                    (isinstance(leftvalue, list))
            ):
                outrecord[rightkey] = P4Mapping(leftvalue)(rightvalue)
            elif (not rightkey in numfields):
                if (rightkey != leftkey):
                    specinput.rename(leftkey, rightkey)
            outrecord.merge(specinput)
        ''' revert back to a flattened record/spec.
        '''
        outrecord = Flatten(**outrecord).expand()
        ''' cleanup outrecord & remove un-needed fields in spec input
        '''
        [outrecord.delete(key) for key in outrecord.copy().keys() \
         if key.lower() in ('access', 'update', 'code')]
        ''' run the spec command and return it's output
        '''
        try:
            return self.objp4.p4Input(tablename, *inputargs, **outrecord)
        except Exception as err:
            bail(err)
        finally:
            self.objp4.close()
