import re

from libsql.sqlValidate import *
from libsql.sqlRecords import Records
from libdlg.dlgStore import (
    Lst,
    objectify,
    Storage
)
from libdlg.dlgUtilities import (
    bail,
    noneempty,
    Flatten,
    reg_explain,
    reg_changelist,
    reg_usage,
    reg_p4help_for_usage,
    reg_filename,
    reg_option,
    spec_lastarg_pairs
)

'''  [$File: //dev/p4dlg/libpy4/py4Options.py $] [$Change: 693 $] [$Revision: #25 $]
     [$DateTime: 2025/04/22 07:22:55 $]
     [$Author: mart $]
'''

__all__ = ['Py4Options']

class Py4Options(object):
    ''' a class that determines and collects enough relevant
        data about a p4 command (yes, meaning the `tablename`).

        args:
                args[0] - the parent instance (Py4).
                args[1] - the tablename

        kwargs:
                optionsadata    - a dictionary that collects and passes in relevant data
                                  about the table/cmd the options will reference.

        return value:
               optionsdata  - a dictionary containing the table's options.

        What's in the box.
            * usage info, such as available cmd line options & keywords
            * syntax info (including both optional and required options,
              as well as positional options requirements).
           * the table's expected fieldnames that make up records
           * a `fieldsmap` (a lower_case to actual_case relationship) so
             we don't need to remember the fields' naming convention
             (lowercase? camelCase?, CamelCase? Capitalized?, etc.  argh!!!)
           * We make a distinction between:
                    * command options, spec options
                    * specs (spec collections)
                    * spec references (p4 jobspec)
                    * no-commands
                    * `usage` commands (look and feel like subparsers)
                    * ...
    '''
    def __init__(self, *args, **optionsdata):
        (args, optionsdata) = (Lst(args), objectify(optionsdata))
        (
            self.objp4,
            self.tablename
        ) = \
            (
                args(0),
                args(1)
            )

        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.objp4.logger,
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
        )
        ]
        self.optiondefaults = Storage()
        self.optionsdata = objectify(optionsdata)

    def __call__(self, *args, **kwargs):
        ''' optionsref = {
                            'fieldnames': [fieldnames],
                            'fieldsmap': {fieldsmap},
                            'error': error,
                            'tableoptions': {
                                              'keywords': [keywords],
                                              'missing_keywords': [missing_keywords],
                                              'options': [options],
                                              'optionsmap': {optionsmap},
                                              'usage': 'usage line'
                                    }
                        }
        '''
        (
            keywords,
            missing_keywords,
            options,
            optionsmap,
            usage,
            fieldnames,
            fieldsmap,
            error,
            records
        ) = \
            (
                Lst(),
                Lst(),
                Lst(),
                Storage(),
                None,
                Lst(),
                Storage(),
                None,
                None
            )
        if (not self.tablename in ([None] + self.objp4.usage_items)):
            ''' field `generic` gets an arbitrary value, say 1... who cares, really?
            '''
            (data, generic, code, severity) = (None, 1, 'error', 3)
            cmdargs = Lst(
                            self.objp4.p4globals \
                            + [
                                self.tablename,
                                '--explain'
                            ]
            )
            ''' For a given table (cmd), figure out the p4 cmdline:
                    Usage help      (eg. `print [-a -A -k -o localFile -q -m max] [-U] files...`
                    options         (eg. -f)
                    keywords        (eg. --force)
                    optionsmaps     (eg. {'f': 'force',
                                            ... 
                                          }
            '''
            datarecord = self.objp4.p4OutPut(self.tablename, *cmdargs)
            if (len(datarecord) > 0):
                if (
                        (isinstance(datarecord, list) is True) |
                        (is_recordsType(datarecord) is True)
                ):
                    datarecord = datarecord.first()
                if (isinstance(datarecord, dict) is True):
                    (
                        generic,
                        code,
                        severity,
                        data
                    ) = \
                        (
                            datarecord.generic,
                            datarecord.code,
                            datarecord.severity,
                            datarecord.data
                        )
                if (isinstance(data, str) is True):
                    data = Lst(data.splitlines()).clean()
                if (
                        (len(data) > 0) &
                        (code == 'error') &
                        (severity == 3)
                ):
                    ''' generic 1, code 'error' & severity 3...                    
                        
                        Such it is with perforce...  An error message, though not an error. 
                    '''
                    for line in data:
                        ''' Examine each of the data lines:
                        '''
                        if (reg_explain.match(line) is not None):
                            result = Lst(
                                re.split('\s', line.strip('-').split(':')[0])).clean()
                            (kwd, opt) = result \
                                if (len(result) == 2) \
                                else (result.first(), None) \
                                if (len(result) == 1) \
                                else (None, None)
                            if (opt is not None):
                                ''' The perforce cmdline help is not dynamically generated.
                                    That said, they are inconsistent, therefore have unpredictable
                                    formatting and syntax. FOr that reason, instead of taking
                                    advantage of such thin gs as brackets and other markers,
                                    we will strip them all out and guess (deduce, come to a 
                                    questionable conclusion, etc.) what the type options we have,
                                    syntax, what args are required (if any), etc.
                                    
                                    Also, some p4 commands have more options than keywords... 
                                    Potentially, we can make them up on the fly... but that's something
                                    for later, when time permits. For now, we'll just hve to give our
                                    best guess. 
                                '''
                                if (kwd is None):
                                    missing_keywords.append(kwd)
                                else:
                                    kwd = re.sub('-', '', kwd)
                                opt = re.sub('[()-]', '', opt)
                            (
                                keywords.append(kwd),
                                options.append(opt),
                                optionsmap.merge({opt: kwd})
                            )
                        elif (reg_usage.match(line) is not None):
                            usage = re.sub(r'^Usage: ', '', line)
                        elif (reg_p4help_for_usage.match(line) is not None):
                            usage = str(line).lstrip().rstrip()
            ''' remove None values and empty strings.
            '''
            options = options.clean()
            optionsmap.delete(None)
            ''' Analyse the Usage string.
            '''
            if (usage is not None):
                ''' This is where we try build the table's cmdargs we need to run a p4 cmd. Its 
                    output will help define the required cmd options (if any).                     
                '''
                usage = re.sub('[\[\]]', '', usage)     # TODO: do something with | statements in usage line
                cmdargs = self.objp4.p4globals + [self.tablename]
                ''' lastarg... as in >>> p4 edit //depot/files/...
                                                 |---------------|
                                                         |
                                          the cmd's very last positional arg.
                    
                    figure out if the cmd:
                        1. takes a last argument ?
                        2. let's us guess what kind of arg we need ? (filename, specname, changelist, etc...)
                        3. is optional / required ?
                        4. has a default value ?
                        5. let's us make one up ?
                '''
                lastarg = None
                if (reg_filename.search(usage) is not None):
                    cmdargs += self.get_more_table_options(keywords)
                    rightside_mapping = f'//{self.objp4._client}/...'
                    cmdargs += [rightside_mapping]
                elif (not self.tablename in (self.objp4.nocommands + self.objp4.initcommands)):
                    if (
                            (
                                    (self.optionsdata.is_command is True) &
                                    (self.optionsdata.is_spec is False)
                            ) |
                            (self.optionsdata.is_specs is True)
                    ):
                        if (reg_changelist.search(usage) is not None):
                            changeargs = self.objp4.p4globals + ['changes', '-m1']
                            cl = self.objp4.p4OutPut('changes', *changeargs)(0).change
                            cmdargs.append(cl)
                    else:
                        cmdargs += self.get_more_table_options(keywords)

                ''' We don't don't need to run a spec cmd to lean about it's 
                    fieldnames, so skip this next block if `is_spec` is True.
                '''
                if (self.optionsdata.is_spec is True):
                    if (self.tablename in spec_lastarg_pairs.getkeys()):
                        lastarg = spec_lastarg_pairs[self.tablename].default
                        cmdargs += ['--output', lastarg]
                    else:
                        cmdargs += ['--output']
                if (self.tablename not in ('submit',)):
                    ''' Let's not try this with `p4 submit`, lest we screw ourselves up.
                    '''
                    records = self.objp4.p4OutPut(self.tablename, *cmdargs, lastarg=lastarg)
                    if (
                            (
                                    (is_recordsType(records) is True) |
                                    (isinstance(records, list))
                            ) &
                            (
                                    (is_recordType(records(0)) is True) |
                                    (isinstance(records(0), Storage))
                            ) &
                            (len(records) > 0)
                    ):
                        ''' using the -G global option may cause the resulting record to 
                            contain numbered fields, like
                            
                                View0, View1, View2, View3, ...
                            
                            So we need to flatten/reduce & transform the record, eg.
                                          
                            from
                                 <Record {'Client': 'computer_dlg',
                                          'Description': 'Created by zerdlg.\n',
                                          'Host': 'computer.local',
                                          'LineEnd': 'local',
                                          'Options': 'noallwrite noclobber nocompress unlocked nomodtime normdir',
                                          'Owner': 'zerdlg',
                                          'Root': '/Users/gc/anastasia/dev/p4dlg',
                                          'SubmitOptions': 'submitunchanged',
                                          'View0': '//qa/... //computer_dlg/qa/...',
                                          'View1': '//spec/... //computer_dlg/spec/...',
                                          'View2': '//release/... //computer_dlg/release/...',
                                          'View3': '//git/... //computer_dlg/git/...',
                                          'View4': '//dev/... //computer_dlg/dev/...',
                                          'View5': '//depot/... //computer_dlg/depot/...',
                                          'code': 'stat'}>

                            to
                                 <Record {'Client': 'computer_dlg',
                                          'Description': 'Created by zerdlg.\n',
                                          'Host': 'computer.local',
                                          'LineEnd': 'local',
                                          'Options': 'noallwrite noclobber nocompress unlocked nomodtime normdir',
                                          'Owner': 'zerdlg',
                                          'Root': '/Users/gc/anastasia/dev/p4dlg',
                                          'SubmitOptions': 'submitunchanged',
                                          'View': ['//swerve/... //computer_dlg/swerve/...',
                                                   '//spec/... //computer_dlg/spec/...',
                                                   '//release/... //computer_dlg/release/...',
                                                   '//git/... //computer_dlg/git/...',
                                                   '//dev/... //computer_dlg/dev/...',
                                                   '//depot/... //computer_dlg/depot/...'],
                                          'code': 'stat'}>
        
                        At any rate, run the cmd with the collected options and just 
                        grab the resulting records' first record (rec0) and use its keys
                        as fieldnames.
                        '''
                        rec0 = Flatten(**Storage(records(0))).reduce()
                        fieldnames = rec0.getkeys()
                        if (
                                (len(fieldnames) == 1) &
                                (fieldnames(0) == 'code')
                        ):
                            rec0.delete('code')
                            fieldnames.pop(0)
                        else:
                            rec_results = Lst(
                                'code',
                                'generic',
                                #'severity',
                                'data'
                            ).intersect(fieldnames)
                            if (
                                    (len(rec_results) in (3, 4)) &
                                    (rec0.code == 'error')
                                    #& (rec0.severity >= 3)
                            ):
                                error = rec0.data
                                fieldnames = Lst()

                            if (len(fieldnames) == 0):
                                ''' once flattened, retake the inventory of this record's fields
                                '''
                                fieldnames = rec0.getkeys()
            else:
                usage = f'usage string for cmd `{self.tablename}` could not be set.'
        if (
                (len(fieldnames) == 0) &
                (len(fieldsmap) > 0)
        ):
            fieldnames = fieldsmap.getvalues()
        if (
                (len(fieldsmap) == 0) &
                (len(fieldnames) > 0)
        ):
            fieldsmap = Storage(
                zip(
                    [
                        fname.lower() for fname in fieldnames
                    ],
                    fieldnames
                )
            )
        optionsref = objectify(
            {
                'fieldnames': fieldnames,
                'fieldsmap': fieldsmap,
                'error': error,
                'tableoptions': {
                                    'keywords': keywords,
                                    'missing_keywords': missing_keywords,
                                    'options': options,
                                    'optionsmap': optionsmap,
                                    'usage': usage
                                }
                        }
                )
        self.optionsdata.merge(optionsref)
        self.buildoptionvalues()
        if (self.optionsdata.is_spec is True):
            ''' Define spec references (and, at the same time, get 
                those fieldnames we so very much need). But first,
                remove whatever fieldnames collected so far.   
            '''
            [
                self.optionsdata.update(**{fielditem: fieldvalue}) for (fielditem, fieldvalue) in {
                'fieldnames': [],
                'fieldsmap': {}
            }.items()
            ]
            specref = self.buildspecref(self.tablename)
            self.optionsdata.merge(specref)
        self.all_options = self.optionsdata.tableoptions.options \
                           + self.optionsdata.tableoptions.keywords
        return self

    def buildoptionvalues(self):
        ''' Update `self.optionsdata.fieldnames` & `self.optionsdata.fieldsmap` inplace then  move on.
        '''
        current_options = self.optionsdata.tableoptions.options
        usage = self.optionsdata.tableoptions.usage
        usage_items = Lst(re.split('\s', usage)[1:]).clean()
        usage_stoidx = usage_items.storageindex(reversed=True)
        for arg in current_options:
            for (idx, argitem) in usage_stoidx.items():
                argname = re.sub('-', '', argitem)
                if (argname == arg):
                    default_value = usage_stoidx[idx]
                    if (reg_filename.search(default_value) is None):
                        default_value = False \
                            if (reg_option.match(default_value) is None) \
                            else None
                    self.optiondefaults.merge(
                        {
                            argname: default_value,
                            self.optionsdata.tableoptions.optionsmap[argname]: default_value
                        }
                    )

    def buildspecref(self, spectype):
        '''
            spec def
            +-----+-----------------+--------+--------+-----------+
            |  ID | NAME            | TYPE   | LENGTH | REQUIRED  |
            +-----+-----------------+--------+--------+-----------+
            | 309 | Options         | line   | 64     | optional  |
            | 313 | SubmitOptions   | select | 25     | optional  |
            | 303 | Access          | date   | 20     | always    |
            | 304 | Owner           | word   | 32     | optional  |
            | 301 | Client          | word   | 32     | key       |
            | 302 | Update          | date   | 20     | always    |
            | 307 | Root            | line   | 64     | required  |
            | 308 | AltRoots        | llist  | 64     | optional  |
            | 305 | Host            | word   | 32     | optional  |
            | 306 | Description     | text   | 128    | optional  |
            | 318 | Type            | select | 10     | optional  |
            | 311 | View            | wlist  | 64     | optional  |
            | 317 | ChangeView      | llist  | 64     | optional  |
            | 310 | LineEnd         | select | 12     | optional  |
            | 314 | Stream          | line   | 64     | optional  |
            | 316 | StreamAtChange  | line   | 64     | optional  |
            | 315 | ServerID        | line   | 64     | always    |
            +-----+-----------------+--------+--------+-----------+

            usage note:  always = read-only
                         optional = optional
                         required = not optional

            specref:  collected data about a spec, specifically:
                * fieldnames
                * fieldsmap
                * spectype
                * altarg
                * keying
                * specfield         -> Really just the same as `spectype`, but capitalized.
                * fieldsdata        -> Everything included in the table above (overkill? maybe).
                                       At any rate, well worth keeping around for now ...
        '''
        def getSpecData():
            fdata = Lst(filter(lambda key: key.attribute == 'key', fieldsdata))
            return fdata or Lst()

        cmdargs = Lst(
            self.objp4.p4globals \
            + ['spec',
                '--output',
                spectype
            ]
        )
        spec = self.objp4.p4OutPut(self.tablename, *cmdargs, lastarg=spectype)(0)
        if (spec.code is not None):
            if (spec.code == 'error'):
                bail(spec.dada)
        specrec = Flatten(spec).reduce()
        specfields = specrec.Fields
        specitems = [re.split(r'\s', item) for item in specfields]
        fieldsdata = [
            Storage(
                zip(
                    self.objp4.specfield_headers, specitem
                )
            ) for specitem in specitems
        ]
        specdata = getSpecData()
        altarg = spec.altArg or specdata(0).name.lower() \
            if (noneempty(specdata) is False) \
            else spectype.capitalize()
        keying = Lst(item.name for item in specdata) \
            if (noneempty(specdata) is False) \
            else Lst()
        fieldnames = Lst(fieldattr.name for fieldattr in fieldsdata).nodups()
        fieldsmap = Storage([(field.lower(), field) for field in fieldnames])
        specref = {
                   'fieldnames': fieldnames,
                   'fieldsmap': fieldsmap,
                   'spectype': spectype,
                   'altarg': altarg,
                   'keying': keying,
                   'specfield': spectype.capitalize(),
                   'fieldsdata': fieldsdata
        }
        return specref

    def sanitize(self, opt):
        return re.sub('-', '', opt)

    def get_more_table_options(self, keywords):
        args = []
        for kwitem in (
                'preview',
                'force',
                'max'
        ):
            if (kwitem in keywords):
                if (kwitem == 'max'):
                    args +=  ['--max', '1']
                else:
                    args.append(f'--{kwitem}')
        return args

    def get_arg_by_kwarg(self, kwditem):
        ''' get the arg name of the specified keyword
        '''
        for (arg, kwarg) in self.optionsdata.tableoptions.optionsmap.items():
            if (kwarg == kwditem):
                return arg

    def get_kwarg_by_arg(self, argitem):
        ''' get the keyword name of the specified arg
        '''
        for (arg, kwarg) in self.optionsdata.tableoptions.optionsmap.items():
            if (arg == argitem):
                return kwarg

    def arg_is_valid(self, arg):
        return True \
            if (self.sanitize(arg) in self.all_options) \
            else False

    def requires_value(self, arg):
        return True \
            if (self.optiondefaults[self.sanitize(arg)] is not None) \
            else False
