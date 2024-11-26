import re

from libdlg.dlgRecords import DLGRecords
from libdlg.dlgRecord import DLGRecord
from libdlg.dlgStore import (
    Lst,
    objectify,
    Storage
)
from libdlg.dlgQuery_and_operators import AND, OR
from libdlg.dlgUtilities import (
    noneempty,
    bail,
    Flatten,
    reg_explain,
    reg_changelist,
    reg_usage,
    reg_p4help_for_usage,
    reg_filename,
    reg_option,
)

'''  [$File: //dev/p4dlg/libpy4/py4Options.py $] [$Change: 473 $] [$Revision: #29 $]
     [$DateTime: 2024/09/08 08:15:23 $]
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
        self.spec_lastarg_pairs = objectify(
                {'change': {
                    'lastarg': 'changelist#',
                    'default': '1'
                },
                'depot': {
                    'lastarg': 'depotname',
                    'default': 'nodepot'
                },
                'server': {
                    'lastarg': 'serverID',
                    'default': '0'
                },
                'group': {
                    'lastarg': 'groupname',
                    'default': 'nogroup'
                },
                'job': {
                    'lastarg': 'jobname',
                    'default': 'job0001'
                },
                'label': {
                    'lastarg': 'labelname',
                    'default': 'nolabel'
                },
                'client': {
                    'lastarg': 'clientname',
                    'default': 'noclient'
                },
                'ldap': {
                    'lastarg': 'ldapname',
                    'default': 'noldap'
                },
                'user': {
                    'lastarg': 'username',
                    'default': 'nouser'
                },
                'stream': {
                    'lastarg': 'streamname',
                    'default': 'nostream'
                },
                'remote': {
                    'lastarg': 'remoteID',
                    'default': 'noremote'
                },
                'spec': {
                    'lastarg': 'type',
                    'default': 'nospec'
                },
                'branch': {
                    'lastarg': 'branchname',
                    'default': 'nobranch'
                }
            }
        )

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
        if (not self.tablename in [None] + self.objp4.usage_items):
            (data, generic, code, severity) = (None, 1, 'error', 3)
            cmdargs = Lst(
                            self.objp4.p4globals \
                            + [
                                self.tablename,
                                '--explain'
                            ]
            )


            datarecord = self.objp4.p4OutPut(self.tablename, *cmdargs)
            if (len(datarecord) > 0):
                if (isinstance(datarecord, (list, DLGRecords))):
                    datarecord = datarecord.first()
                if (isinstance(datarecord, Storage) is True):
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
                if AND(
                        AND(
                            AND(
                                (len(data) > 0),
                                (generic == 1)
                            ),
                            (code == 'error')
                        ),
                        (severity == 3)
                ):
                    ''' generic 1, code 'error' & severity 3...                    
                        Such it is with perforce... 
                        An error message, though not an error. 
                    '''
                    for line in data:
                        ''' Looking for usage lines to build the p4 cmd line.
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
                                if (kwd is None):
                                    ''' Some commands do have more options than 
                                        keywords... Potentially, we can make them
                                        up on the fly... We'll see later.  
                                    '''
                                    missing_keywords.append(kwd)
                                if (re.search(r'^\(.*\)$', opt) is not None):
                                    opt = opt.strip("(-)")
                            (
                                keywords.append(kwd),
                                options.append(opt),
                                optionsmap.merge({opt: kwd})
                            )
                        elif (reg_usage.match(line) is not None):
                            usage = re.sub(r'^Usage: ', '', line)
                        elif (reg_p4help_for_usage.match(line) is not None):
                            usage = str(line).lstrip().rstrip()

            if (usage is not None):
                ''' This is where we try build the table's cmdargs we need to run a p4 cmd. Its 
                    output will help define the required cmd options (if any).                     
                '''
                usage = re.sub('[\[\]]', '', usage)  # TODO: do something with | statements in usage line
                cmdargs = self.objp4.p4globals + [self.tablename]
                (lastarg, more_cmdargs) = (None, [])
                if (reg_filename.match(usage) is not None):
                    try:
                        more_cmdargs = self.get_more_table_options(keywords)
                        rightside_mapping = f'//{self.objp4._client}/...'
                        more_cmdargs += [rightside_mapping]
                    except Exception as err:
                        self.logerror(err)
                elif (not self.tablename in (self.objp4.nocommands + self.objp4.initcommands)):
                    if OR(
                            AND(
                                (self.optionsdata.is_command is True),
                                (self.optionsdata.is_spec is False)
                            ),
                            (self.optionsdata.is_specs is True)
                    ):
                        more_cmdargs = self.get_more_table_options(keywords)
                    elif (self.optionsdata.is_spec is True):
                        if (self.tablename in self.spec_lastarg_pairs is True):
                            lastarg = self.spec_lastarg_pairs[self.tablename].default
                            ''' TODO: REQUIRES WORK!
                            
                                specs' last position options:
    
                                    change      ->      [ changelist# ]
                                    depot       ->      depotname
                                    server      ->      serverID
                                    group       ->      groupname
                                    job         ->      [ jobname ]
                                    label       ->      labelname
                                    client      ->      [ clientname ]
                                    ldap        ->      ldapname
                                    user        ->      [ username ]
                                    stream      ->      [ streamname ]
                                    remote      ->      [ remoteID ]
                                    spec        ->      type
                                    branch      ->      [ branchname ]
    
                            when time permits ... 
                            remove the `>>> p4 <spectype>` -o` crap for their field definitions!
                            we need to do `>>> p4 spec <spectype>` instead 
    
                            last_option = None
                            if (reg_changelist.match(usage) is not None):
                                last_option = '1'
                            elif (reg_filename.match(usage) is not None):
                                last_option = f'//{self.objp4._client}/...'
                            # if ('max' in keywords):
                            #    more_cmdargs.append('-m1')
                            # if (last_option is not None):
                            #    more_cmdargs.append(last_option)
    
    
                                no args required:
                                    license
                                    protect
                                    triggers
                                    typemap
                            '''
                            more_cmdargs += ['--output', lastarg]
                        else:
                            more_cmdargs += ['--output']
                    else:
                        more_cmdargs = self.get_more_table_options(keywords)
                cmdargs = cmdargs + more_cmdargs
                records = self.objp4.p4OutPut(self.tablename, *cmdargs, lastarg=lastarg)
                if (isinstance(records, Lst) is True):
                    if (isinstance(records(0), Storage)):
                        rec0 = records(0)
                        fieldnames = rec0.getkeys()
                        if AND(
                                (len(fieldnames) == 1),
                                (fieldnames(0) == 'code')
                        ):
                            rec0.delete('code')
                            fieldnames.pop(0)
                        else:
                            rec_results = Lst(
                                'code',
                                'generic',
                                'severity',
                                'data'
                            ).intersect(fieldnames)
                            if AND(
                                    (len(rec_results) == 4),
                                    (rec0.code == 'error')
                            ):
                                error = rec0.data
                                fieldnames = Lst()
                            else:
                                rec0 = Flatten(**Storage(rec0)).reduce()
                                fieldnames = rec0.getkeys()
            else:
                usage = f'usage string for cmd `{self.tablename}` could not be set.'
        if AND(
                (len(fieldnames) == 0),
                (len(fieldsmap) > 0)
        ):
            fieldnames = fieldsmap.getvalues()
        if AND(
                (len(fieldsmap) == 0),
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
            ''' define spec references
            '''
            specref = self.buildspecref(self.tablename)
            self.optionsdata.merge(specref)
        self.all_options = self.optionsdata.tableoptions.options \
                           + self.optionsdata.tableoptions.keywords
        return self

    def buildoptionvalues(self):
        usage = self.optionsdata.tableoptions.usage
        usage_items = Lst(re.split('\s', usage)[1:]).clean()
        usage_stoidx = usage_items.storageindex(reversed=True)
        for arg in self.optionsdata.tableoptions.options:
            for (idx, argitem) in usage_stoidx.items():
                argname = re.sub('-', '', argitem)
                if (argname == arg):
                    default_value = usage_stoidx[idx]
                    if (reg_filename.match(default_value) is None):
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
        if (reg_changelist.search('changelist#') is not None):
            args.append('1')
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
