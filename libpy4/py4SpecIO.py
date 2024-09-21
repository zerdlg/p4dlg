import re

from libdlg.dlgStore import *
from libdlg.dlgQuery_and_operators import AND, OR
from libdlg.dlgRecord import P4QRecord
from libdlg.dlgUtilities import Flatten, bail


class SpecIO(object):
    def __init__(self, objp4):
        self.objp4 = objp4

    def __call__(self, *args, **kwargs):
        return self

    def out(
            self,
            tablename,
            specname,
            *args,
            **specinput
    ):
        (args, specinput) = (Lst(args), Storage(specinput))
        tabledata = self.objp4.memoizetable(tablename)
        fieldsmap = tabledata.fieldsmap
        (
            outputargs,
            inputargs
        ) \
            = \
            (
                Lst(self.objp4.p4globals + args.copy()),
                Lst(self.objp4.p4globals + args.copy())
            )
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
                if AND(
                OR(
                    (is_input is True),
                    (is_output is True)
                ),
                (dsum == 0)
            ) \
                else True

            ''' TODO: need more rules for requires_force
            '''
            requires_force = True if (tablename != 'spec') else False
        altarg = tabledata.altarg
        if (specname is None):
            if (len(specinput) > 0):
                (specname, specinput, altarg) = self.objp4.parseInputKeys(
                    tabledata,
                    specname,
                    **specinput
                )
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

        specoptions = Storage(
            {
                'is_input': is_input,
                'is_output': is_output,
                'is_delete': is_delete,
                'requires_force': requires_force
            }
        )
        #self.loginfo(f"spec command options: {specoptions}")

        p4globals = ''.join(self.objp4.p4globals)
        for argsitem in (outputargs, inputargs):
            if (None in argsitem):
                argsitem.remove(None)
            is_match = re.match(f'^{p4globals}', ''.join(argsitem))
            if (is_match is None):
                self.objp4.p4globals += argsitem
        has_outputkey = (
            ('-o' in outputargs),
            ('--output' in outputargs)
        )
        has_forcekey = (
            ('-f' in outputargs),
            ('--force' in outputargs)
        )
        if (is_delete is True):
            if (not True in has_forcekey):
                idx = (outputargs.index('-d') + 1) \
                    if ('-d' in outputargs) \
                    else (outputargs.index('--delete') + 1) \
                    if ('--delete' in outputargs) \
                    else -1
                if (specname == outputargs(-1)):
                    outputargs.insert(idx, '--force')
                else:
                    outputargs.append('--force')
        elif AND(
                (True in (is_input, is_output)),
                (not True in has_outputkey)
        ):
            if (specname == outputargs(-1)):
                outputargs.insert(-1, '--output')
            else:
                outputargs.append('--output')

        if AND(
                (specname != outputargs(-1)),
                (specname is not None)
        ):
            outputargs.append(specname)

        try:
            out = self.objp4.p4OutPut(tablename, *outputargs)
            outrecord = Lst(out)(0)

            if (type(outrecord) is Lst):
                outrecord = outrecord(0) or Storage()
            if (outrecord not in (Lst(), None)):
                if AND(
                        (outrecord.generic is not None),
                        (outrecord.data is not None)
                ):
                    return (outrecord)
            if (isinstance(outrecord, P4QRecord)):
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
                return outrecord
        finally:
            self.objp4.close()
        ''' To save a new or updated spec, the output becomes the input's base record (except for change). 
        '''
        if (inputargs(-1) == specname):
            inputargs.pop(-1)
        specinputcopy = Storage(specinput.copy())
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
        if OR(
                (tablename in ('user',)),
                (requires_force is True)
        ):
            if (not '-f' in inputargs):
                inputargs.append('-f')
        ''' TODO: needs an options playbook ...
        '''
        inputargs.append('--input')
        for oldkey in specinputcopy.keys():
            newkey = fieldsmap[oldkey.lower()]
            if AND(
                    (isinstance(outrecord[newkey], list)),
                    (isinstance(specinput[newkey], list))
            ):
                outrecord[newkey] += specinput[newkey]
            elif (not newkey in numfields):
                if (newkey != oldkey):
                    specinput.rename(oldkey, newkey)
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
