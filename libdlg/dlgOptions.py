import sys
import argparse
from libdlg.dlgStore import ZDict, objectify, Lst

''' a parser to handle cmd line options

        combining options:  -a -b -c  -> -abc
            * requires:   {'action': 'store_const',
                           'const': True,
                           'default': False}

        repeatable (any number of times): myprog.py --dbrows -id 1234 -id 2234 -id 3234
            * requires:   {'nargs': '+',
                           'action': 'store', 
                           'const': False/None,
                           'default': False/None}


                'row': {'id': {'short': 'id',
                               'description': 'some clever description of an id',
                               'epilog': 'some unclear epilog for asset_id!',
                               'required': False,
                               'const': None,
                               'default': None,
                               'action':'store',
                               'nargs': '+',
                               'help': \
                                   "If you mess up, try try again!"},                           
'''

'''  [$File: //dev/p4dlg/libdlg/dlgOptions.py $] [$Change: 467 $] [$Revision: #6 $]
     [$DateTime: 2024/08/24 08:15:42 $]
     [$Author: zerdlg $]
'''

__all__ = ['ArgsParser', ]

'''
        # ---- P4GLOBALS OPTIONS GROUP, inherited by any subparsers that target p4 operations
                # ---- These options are required
                'p4globals': {'user': {'short': 'u',
                                       'required': False,
                                       'const': None,
                                       'default': None,
                                       'nargs': '?',
                                       'help': \
                                           "A Perforce user"},
                              'port': {'short': 'p',
                                       'required': False,
                                       'const': None,
                                       'default': None,
                                       'nargs': '?',
                                       'help': \
                                           "A Perforce server:port"},
                              'client': {'short': 'c',
                                         'required': False,
                                         'const': None,
                                         'default': None,
                                         'nargs': '?',
                                         'help': \
                                             "A Perforce client"}}
'''

class ArgsParser(object):
    def __init__(self, *args, **kwargs):
        '''  referenced & inheritable options and subparsers definitions:

    USAGE:

    option references:

    referenceOption: {REFERENCE_NAME : {OPTION_LONG: {'short':OPTION_SHORT,
                                                      'required':False,
                                                      'default':False,
                                                      'const':False,
                                                      'nargs':'?',
                                                      'help':"default is True"}},},

    subparsers:

    'subparser':{SUBPARSER_NAME: {'reference':[NAME,],
                                  'help': 'help info for the subparser.',
                                  'private': 'OPTION_LONG': {'OPTION_LONG':{
                                                                'short':OPTION_SHORT,
                                                                'required':False,
                                                                'default':False,
                                                                'const':True,
                                                                'nargs':'?',
                                                                'help':\
                                                                   "something useful, meaningful & helpful "
                                                                   }
                                  }
                },
        '''
        self.optionData = objectify(
            {
                'referenceOption': {
                    # ---- COMMON OPTIONS GROUP, inherited by all subparsers
                    # ---- These options are not required
                    'common': {
                        'quiet': {'short': 'q',
                                  'action': 'store',
                                  'required': False,
                                  'default': False,
                                  'const': True,
                                  'nargs': '?',
                                  'help': "default is True"},
                        'debug': {'short': 'dbg',
                                           'action': 'store',
                                           'required': False,
                                           'default': False,
                                           'const': True,
                                           'nargs': '?',
                                           'help': "run with debug info"},
                        'output_format': {'short': 'o',
                                          'action': 'store',
                                          'required': False,
                                          'default': 'csv',
                                          'const': 'None',
                                          'nargs': '?',
                                          'help': "stored output format"},
                    },
                    # ---- P4D backups
                    'backup': {
                        'source': {'short': 'src',
                                   'action': 'store',
                                   'required': False,
                                   'default': False,
                                   'const': True,
                                   'nargs': '?',
                                   'help': "source directory to be back'd up"},
                        'destination': {'short': 'dst',
                                        'action': 'store',
                                        'required': False,
                                        'default': None,
                                        'const': 'None',
                                        'nargs': '?',
                                        'help': "dest directory for the backup"}
                    },
                    # ---- Note: no abreviated options for p4 actions/ you must use the keyword
                    'initshell': {
                        'short': 's',
                        'required': False,
                        'default': True,
                        'const': True,
                        'nargs': '?',
                        'help': 'start QT shell'
                    }
                },
                # ----              SUBPARSERS
                'subparser':
                # ---- CREATE OPTIONS THAT READ CONFIG JSON FILES AND GENERATE RELATED OPTIONS (for p4braider)
                    {'accel': {
                        'reference': ['p4'],                       #p4braider
                        'help': '- accel help -',
                        'private': {}
                    },#"self.defineconfigs('accel')"},
                     'archive': {
                         'reference': ['p4'],                      #p4braider
                         'help': '- archive help -',
                         'private': {}
                     },#"self.defineconfigs('archive')"},
                     'artist_workspace': {
                         'reference': ['p4'],                      #p4braider
                         'help': '- artist_workspace help -',
                         'private': {}
                     },#"self.defineconfigs('artist_workspace')"},
                     'shell': {
                         'reference': ['initshell'],
                         'help': '- run a qtconsole -',
                         'private': {}
                     },
                     'bck': {
                         'reference': ['backup'],
                         'help': '- run a backup of P4DROOT -',
                         'private': {}
                     }
                }
            }
        )

        self.aParser = argparse.ArgumentParser(
            **{
                'prog': 'p4dlg',
                'description': "A program that facilitates SQL-like syntax.",
                'usage': '%(prog)s [options]',
                'add_help': True
            }
        )

        self.subParsers = self.aParser.add_subparsers(help='command help')
        self.bParser = argparse.ArgumentParser(add_help=False)

    def __call__(self, *args, **kwargs):
        ''' time for subparsers - set them up!
        '''
        subparsers = self.optionData.subparser.keys()
        ''' define each subparser
        '''
        for subparser_name in subparsers:
            whichname = subparser_name.lower()
            optparser = self.optionData.subparser[subparser_name]
            ''' private options simply define the subparser's own options 
                *must be unique & are not sharable.
            '''
            if (optparser.private is not None):
                private_options = optparser.pop('private')
                if (len(private_options) > 0):
                    optparser.update(**private_options)
            ''' other referenced options linked to this subparser
            '''
            refoptions = optparser.pop('reference') or []
            if (isinstance(refoptions, list) is False):
                refoptions = Lst([refoptions])
            ''' time to link references
                * all subparsers get common options
            '''
            for opt in refoptions:
                if (self.optionData.referenceOption[opt] is not None):
                    optdata = self.optionData.referenceOption[opt]
                    optparser.update(**optdata)
            ''' add subparser
            '''
            sparser = self.subParsers.add_parser(
                whichname,
                help=optparser.help,
                parents=[self.bParser])
            ''' define subparser's private & linked options then add them to the thing
            '''
            for (optname, optvalue) in optparser.items():
                options = Lst([f'--{optname}', ])
                if (isinstance(optvalue, dict)):
                    kwoptions = ZDict(optvalue.copy())
                    ''' add optional `short` options 
                    '''
                    if (kwoptions.short is not None):
                        shortopt = kwoptions.pop('short')
                        options.insert(0, f'-{shortopt}')
                    ''' define default values for subparser's description and epilog
                    '''
                    description = kwoptions.pop('description') \
                        if (kwoptions.description is not None) \
                        else 'no description'
                    epilog = kwoptions.pop('epilog') \
                        if (kwoptions.epilog is not None) \
                        else 'no epilog'
                    ''' TODO: think about supporting options referenced by other options... useful? 
                    '''
                    try:
                        ''' put it all together!
                            add *options, **keywords, epilog & description, then move on to the next!
                        '''
                        sparser.add_argument(*options, **kwoptions)
                        sparser.description = description
                        sparser.epilog = epilog
                        sparser.add_help
                    except AttributeError as err:
                        sys.exit(f'\n{err}')
            sparser.set_defaults(which=whichname)
        return self.aParser