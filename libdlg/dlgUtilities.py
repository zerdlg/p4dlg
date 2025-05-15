import sys, os
import datetime
from io import StringIO
import socket
try:
    import cPickle as pickle
except:
    import pickle
import string
try:
    from functools import wraps
except ImportError:
    wraps = lambda f: f
from fractions import Fraction
import itertools
import hashlib
from six import b

from marshal import loads, load, dump, dumps
import re
import shutil
from pprint import pformat
try:
    from functools import wraps
except ImportError:
    wraps = lambda f: f
try:
    import cPickle as pickle
except ImportError:
    import pickle

from libdlg.dlgStore import Storage, objectify, Lst, StorageIndex

''' maps py2 to 3 '''
from importlib import reload
import builtins as builtin
import _thread as thread
import queue as Queue
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders as Encoders
from email.header import Header
from email.charset import Charset, add_charset, QP as charset_QP
from urllib.parse import unquote
from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib import parse as urlparse
from urllib import request as urllib2
from urllib.request import FancyURLopener, urlopen
from urllib.parse import quote as urllib_quote, unquote, urlencode, quote_plus as urllib_quote_plus
from xmlrpc.client import ProtocolError

''' dict backward compatibility (not for Storage) 
    
    >>> d = {'a':1, 'b': 2}
    
    >>> iterkeys(d)
    ['a', 'b']
    
    >>> itervalues(d)
    [1, 2]
    
    >>> iteritems(d)
    [('a', 1), ('b', 2)]
    
    for Storage:
    
    >>> sd = Storage(d)
    >>> sd.iterkeys()
    ['a', 'b']
    
    >>> sd.itervalues()
    [1, 2]
    
    >>> sd.iteritems()
    [('a', 1), ('b', 2)]
  
'''
iterkeys = lambda d: iter(Storage(d).getkeys())
itervalues = lambda d: iter(Storage(d).getvalues())
iteritems = lambda d: iter(Storage(d).getitems())
integer_types = (int,)
string_types = (str,)
text_type = str
basestring = str
long = int
unichr = chr
unicode = str
maketrans = str.maketrans
ClassType = type
openurl = urlopen

'''  [$File: //dev/p4dlg/libdlg/dlgUtilities.py $] [$Change: 707 $] [$Revision: #43 $]
     [$DateTime: 2025/05/14 13:55:49 $]
     [$Author: zerdlg $]
'''

__all__ = [
    # pretty much all p4 stuff related stuff
           'set_localport', 'fix_name', 'friendly_tablename', 'real_tablename',
           'p4charsymbols', 'p4ops', 'journal_actions', 'ignore_actions', 'fixep4names',
           'relative_operators', 'relative_change_operators', 'relative_revision_operators',
            'revision_actions',
    #
           'isdepotfile', 'isfsfile', 'isanyfile', 'is_expression', 'is_mode', 'is_marshal',
           'versionname_to_releasename',
    #
           'reg_option', 'reg_depotfile_specifier', 'reg_rev_change_specifier',
           'reg_p4help_for_usage', 'reg_escape', 'reg_p4global', 'reg_valid_table_field',
           'reg_p4dtime', 'reg_dbtablename', 'reg_explain', 'reg_usage',
           'reg_releaseversion', 'reg_marshal', 'reg_input', 'reg_output',
           'reg_classvar', 'reg_filename', 'reg_rcs_quotes', 'reg_datetime_fieldtype',
           'reg_datetime_fieldname', 'reg_epochtime', 'reg_server_or_remote',
    #
           'reg_alphanumeric',
           'reg_ipython_builtin', 'reg_envvariable', 'reg_changelist', 'reg_spec_usage',
           'reg_p4map', 'reg_default', 'reg_objdict', 'reg_job', 'reg_depotpath',
    #
           'now', 'casttype', 'noneempty', 'IsMatch',
           'storageIndexToList', 'Casttype', 'is_int_hex_or_str', 'itemgrouper_filler',
           'itemgrouper', 'containschars', 'sqOperators', 'getTableOpKeyValue', 'getOpKeyValue',
           'remove', 'annoying_ipython_attributes', 'queryStringToStorage', 'bail', 'raiseException',
           'ALLLOWER', 'ALLUPPER', 'PY2', 'fix_tablename',
    #
           'Flatten', 'fractions2Float', 'percents2Float', 'isnum', 'is_iterable', 'is_array',
           'decode_bytes', 'Plural', 'table_alias',
    #
           'reload', 'unquote', 'HTMLParser', 'urlparse', 'urllib2', 'builtin', 'thread', 'Queue',
           'MIMEBase', 'MIMEMultipart', 'MIMEText', 'Encoders', 'Header', 'Charset', 'add_charset',
           'charset_QP', 'FancyURLopener', 'urlopen', 'openurl', 'unquote', 'urllib_quote_plus',
           'urllib_quote', 'unquote', 'urlencode', 'HTTPError', 'URLError',
    #
           'hashlib_md5', 'iterkeys', 'itervalues', 'iteritems',
    #
           'integer_types', 'string_types', 'text_type', 'basestring', 'xrange', 'long', 'unichr',
           'unicode', 'maketrans', 'ClassType', 'ProtocolError',
    #
           'to_bytes', 'to_native', 'to_unicode', 'serializable', 'dttypes',
           'datefields',
    #
           'sanitizename', 'spec_lastarg_pairs', 'p4_error_severity',
]

(mloads, mload, mdump, mdumps) = (loads, load, dump, dumps)
hashlib_md5 = lambda s: hashlib.md5(bytes(s, 'utf-8'))
reg_objdict = re.compile('^(\w+)\.([^.]+)$')
PY2 = sys.version_info[0] == 2

''' p4_error_severity table

    0 -> nothing
    1 -> info
    2 -> warning
    3 -> user error
    4 -> system error (should be fatal)

    * perforce sends back an info, warning, or error message, typically 
      with the following keys:
  
        code        -   error, text, ...
        data        -   the error message
        generic     -   whatever
        severity    -   one of [0, 1, 2, 3, 4]
    
    Note: Though the message is, of course, quite helpful, the `severity` 
          value is a clear, and better, indication of what happens next.
'''
p4_error_severity = Storage(
    {
        0: 'empty',     # nothing happened
        1: 'info',      # informative msg - good
        2: 'warning',   # doh! not great  - but typically not the worst
        3: 'user',      # user error      - bad
        4: 'system'     # system error    - fatal
    }
)

''' regex for getting usage string args,
    essentially cmds take require positional 
    args.
'''
def reg_spec_usage(specname, usageline):
    specialcases = {'workspace': 'client', 'changelist': 'change'}
    rname = specialcases[specname] if (specname in specialcases) else specname
    reg = re.compile(f'^.*(\[)?({rname})?(name|list|ID|type)?(#)?(\])?$')
    try:
        res = reg.search(usageline)
        if (res is not None):
            return res
    except:pass

reg_filename = re.compile(r'^.*\sfile(s|name)?(\s)?(\s?\.\.\.)?$')
reg_changelist = re.compile(r'^.*\schange(list)?(#)?(\s?\.\.\.)?$')
''' reg = re.compile(f'^.*(file(s|name)?)?(\[)?({rname})?(name|list|ID|type)?(#)?(\.)?(\])?$')

    reg_filename & reg_change are used for parsing p4 usage messages

    *** assuming all `[]` will get stripped out before parsing

    reg_filename should provide for any of these cmdline filenames
        I.e.:
            p4 copy [options] fromFile[rev] toFile
            p4 copy [options] -b branch [-r] [toFile[rev] ...]
            p4 copy [options] -b branch -s fromFile[rev] [toFile ...]
            p4 copy [options] -S stream [-P parent] [-F] [-r] [toFile[rev] ...]
            
            p4 sync [ -f -k -n -N -p -q -r -s ] [-m max] [files...]
                                                          file[revRange] ...
                                                          filename ...
                                                          filenames ...
                                                          etc.
            
    reg_change
        I.e.:
            p4 describe [-d<flags> -m -s -S -f -O -I] changelist# ...
                                                      etc. 
                          
    spec arguments (last arg):    
        'branch'        [branchname]    -> domain spec
        'client'        [clientname]    -> domain spec
        'depot'         depotname       -> domain spec
        'label'         labelname       -> domain spec
        'stream'        [streamname]    -> domain spec
        'change'        [changelist#]
        'changelist'    [changelist#]
        'group'         groupname
        'job'           [jobname]
        'workspace'     [clientname]
        'ldap',         ldapname
        'user'          [username]
        'spec'          type
    
        returns None:
            'server'        serverID
            'remote'        [remoteID]
            'typemap'       None            -> domain spec
            'license'       None
            'triggers'      None
            'protect'       None
'''
p4path_re = '^//[\w\.\*]*/.*'
reg_job = re.compile(r'^.*job[0-9]+$')
reg_server_or_remote = re.compile(r'^\d+$')
depotpath_pattern = '//[\w\.\*]*/.*'
reg_depotpath = re.compile(f'^{depotpath_pattern}$')
reg_p4map = re.compile(f'^{depotpath_pattern}\s{depotpath_pattern}$')
reg_envvariable = re.compile(r'^P4[A-Z]')
reg_explain = re.compile(r'^--[a-zA-Z0-9_\-]+(\s\(-[a-zA-Z]\))?:.*$')
reg_usage = re.compile(r'^Usage:.*$')
reg_releaseversion = re.compile('\d{4}\.\d+')
reg_marshal = re.compile(r'^{s\s.*')
reg_input = re.compile(r'^(-i)|(--input)$')
reg_output = re.compile(r'^(-o)|(--output)$')
reg_classvar = re.compile(r'^__.*__$')
reg_option = re.compile(r'^-.*$')
reg_depotfile_specifier = r'.*([#,@]).*$'
reg_rev_change_specifier = re.compile(r'.*([#,@]).*$')
reg_p4help_for_usage = re.compile(r'^[\s]*p4\s-h\sfor\susage\.?\s*$')
reg_escape = re.compile(r"\'")
reg_p4global = re.compile(r"^\t--.*:.*$")
reg_p4dtime = p4dtime = re.compile('^\d*(\.(\d){2}){5}$')
reg_dbtablename = re.compile(r'^db\..*$')
reg_rcs_quotes = re.compile("@[^@]*@")
reg_ipython_builtin = re.compile(r"^_ipython_|_repr_|getdoc")
reg_alphanumeric = re.compile('^[0-9a-zA-Z]\w*$')
reg_valid_table_field = re.compile(r'^[^\d_][_0-9a-zA-Z-]*\Z')
reg_default = {
    'id':       '[1-9]\d*',
    'decimal':  '\d{1,10}\.\d{2}',
    'integer':  '[+-]?\d*',
    'float':    '[+-]?\d*(\.\d*)?',
    'double':   '[+-]?\d*(\.\d*)?',
    'date':     '\d{4}\-\d{2}\-\d{2}',
    'time':     '\d{2}\:\d{2}(\:\d{2}(\.\d*)?)?',
    #'datetime': '\d{4}\-\d{2}\-\d{2} \d{2}\:\d{2}(\:\d{2}(\.\d*)?)?',
    'datetime': '\d{4}\\\d{2}\\d{2} \d{2}\:\d{2}(\:\d{2}(\.\d*)?)?',
    }
reg_epochtime = re.compile(r'^\d*(\.\d+)?$')
reg_datetime_fieldname = re.compile(r'[dD]ate|[aA]ccess|[modtT]ime]|[uP]date]|[tT]ime|[dD]ate[tT]ime]')
reg_datetime_fieldtype = re.compile(r'^[dD]ate')

spec_lastarg_pairs = objectify(
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
                    'default': '1'
                },
                'spec': {
                    'lastarg': 'type',
                    'default': 'client'
                },
                'branch': {
                    'lastarg': 'branchname',
                    'default': 'nobranch'
                }
            }
        )

p4charsymbols = {
                '*': '%2A',
                '#': '%23',
                '%': '%25',
                '@': '%40'
}
p4ops = {
         '=': {
             'rev': '#=', 'change': '@=', 'att': '__eq__'
         },
         '<': {
             'rev': '#<', 'change': '@<', 'att': '__lt__'
         },
         '<=': {
             'rev': '#<=', 'change': '@<=', 'att': '__le__'
         },
         '>': {
             'rev': '#>', 'change': '@>', 'att': '__gt__'
         },
         '>=': {
             'rev': '#>=', 'change': '@>=', 'att': '__ge__'
         },
         '!=': {
             'rev': '#', 'change': '@', 'att': '__ne__'
         }
}
journal_actions = [
    "put",
    "del",
    "pv",
    "dv",
    "rv",
    "vv",
    "ex",
    "mx",
    "nx",
    "dl",
    "ver",
    #Null
]

ignore_actions = [
    "ex",
    "mx",
    "nx"
]
fixep4names = Storage(
    {
        'group': 'p4group',
        'db.group': 'p4group',
        'type': 'p4type',
        'user': 'p4user',
        'db.user': 'p4user',
        'db.desc': 'describe',
        'desc': 'describe'
    }
)
relative_operators = {
    'LT': '<',
    'LE': '<=',
    'GT': '>',
    'GE': '>=',
    'EQ': '='
}
relative_change_operators = {
    'LT': '@<',
    'LE': '@<=',
    'GT': '@>',
    'GE': '@>=',
    'EQ': '@='
}
relative_revision_operators = {
    'LT': '#<',
    'LE': '#<=',
    'GT': '#>',
    'GE': '#>=',
    'EQ': '#='
}
revision_actions = {
    'add': '#add',
    'edit': '#edit',
    'delete': '#delete',
    'branch': '#branch',
    'integrate': '#integrate',
    'import': '#import'
}
datefields=[
    'Access',
    'accessDate',
    'Update',
    'updateDate'
]
serializable = (
    int,
    float,
    long,
    bool,
    list,
    dict,
    tuple,
    basestring,
    type(None)
)
''' TODO: implement _rname on aliased tables!
'''
table_alias = Storage(
    {
        'changelist': 'change',
        'workspace': 'client'
    }
)
(
    date,
    datetime,
    time
) = \
    (
        datetime.date,
        datetime.datetime,
        datetime.time
    )
dttypes = (
    date,
    datetime,
    time
)

def to_bytes(obj, charset='utf-8', errors='strict'):
    try:
        res = bytes(obj) \
            if (isinstance(obj, (bytes, bytearray, memoryview))) \
            else obj.encode(charset, errors) \
            if (isinstance(obj, str)) \
            else None
        if (res is None):
            bail(TypeError('bytes expected, got None'))
        return res
    except Exception as err:
        bail(err)

def to_native(obj, charset='utf8', errors='strict'):
    try:
        res = obj.decode(charset, errors) \
            if (isinstance(obj, bytes)) \
            else obj if (isinstance(obj, str)) \
            else None
        return res
    except Exception as err:
        bail(err)

def to_unicode(obj, charset='utf-8', errors='strict'):
    try:
        res = obj.decode(charset, errors) \
            if (isinstance(obj, bytes)) \
            else text_type(obj) \
            if (obj is not None) \
            else None
        return res
    except Exception as err:
        bail(err)

def decode_bytes(out):
    ''' this one checks if output is marshal then loads it needed.

        * have p4 execs call on it for its subprocess output
    '''
    try:
        if (type(out) is bytes):
            if (is_marshal(out) is True):
                loader = mloads \
                    if (not hasattr(out, 'read')) \
                    else mload
                out = loader(out)
            else:
                out = out.decode('utf8')
                if (out.startswith('{')):
                    out = ''.join(out.split("\n", 1)[1:])
        if (isinstance(out, dict)):
            if (set(map(type, out)) == {bytes}):
                out = Storage({k.decode('utf-8'): v.decode('utf-8')
                if (type(v) is bytes)
                else v for (k, v) in out.items()})
        return out
    except Exception as err:
        bail(err, False)

now = datetime.now
sqOperators = [
    '=',
    '!=',
    '<',
    '>',
    '>=',
    '<=',
    '#^',       # startswith
    '#$',       # endswith
    '##',       # re.match
    '#?',       # re.search
    '!#',       # not contains / regex -> same effect as ~
    '#',        # contains / regex
    '~'
]


''' like py2 xrange 
'''
def xrange(x):
    return iter(range(x))

def annoying_ipython_attributes(name):
    name_dismiss_attr = [
        item for item in (
            (reg_ipython_builtin.match(name)),
            (re.match(r'^_.*$', str(name))),
            (re.match(r'^shape$', name))
        ) if (item not in (None, False))
    ]
    return name_dismiss_attr

def noneempty(item):
    return (
            item in [
        '',
        [],
        (),
        {},
        set(),
        False,
        None
    ]
    )

def sanitizename(name):
    if (reg_alphanumeric.match(name) is None):
        bail(f'invalid table or field name: {name}')
    return name

def is_iterable(obj):
        try:
            if (isinstance(obj, str) is False):
                (i for i in obj)
                return True
            return False
        except TypeError:
            return False

def is_array(obj):
    ''' iterable but not dict '''
    try:
        obj.get
        return False
    except:
        try:
            if (type(obj).__name__ in (
                    'Py4Field',
                    'JNLField',
                    'DLGQuery',
                    'DLGExpression'
            )
            ):
                return False
            if (isinstance(obj, str) is False):
                (i for i in obj)
                return True
            return False
        except TypeError:
            return False


def casttype(_type, value):
    try:
        return objectify(value) \
            if (isinstance(_type, dict)) \
            else Lst(value) \
            if (isinstance(_type, list)) \
            else eval('{}({})'.format(_type, value)) \
            if (isinstance(_type, str)) \
            else _type(value) \
            if ((not isinstance(_type, (None, bool))) \
                & (callable(_type) is True)) \
            else value
    except:
        return value

class Flatten(Storage):
    '''  Inherits Storage but used primarely to reduce/flatten numbered
         un-unique keys after merging Storage objects (dicts)

        I.e.: to unmangle marshalled output when setting the '-G' global option in p4 cmd lines in a terminal

        I.e.:

           >>> output={'bla0': 'gc',
                       'bla1': 'zerdlg',
                       'bla2': 'charlotte',
                       'bla3': 'cara',
                       'fieldtest': 'abc',
                       'view0': '//depot/... //test_main/...',
                       'view1': '//depot/gc/... //test_main/gc/...',
                       'view2': '//depot/charlotte/... //test_main/charlotte/...'}

           >>> oAggr = Flatten(**output)

        reduce (combine value of duplicate keys in one list):

            >>> oAggr.reduce()

           {'bla': ['gc', 'mia', 'charlotte', 'cara'],
            'fieldtest': 'abc',
            'view': ['//depot/... //test_main/...',
                     '//depot/gc/... //test_main/gc/...',
                     '//depot/chloe/... //test_main/chloe/...']}

        flatten (return to numbered keys):

            >>> oAggr.expand()

           {'bla0': 'gc',
            'bla1': 'mia',
            'bla2': 'charlotte',
            'bla3': 'cara',
            'fieldtest': 'abc',
            'view0': '//depot/... //test_main/...',
            'view1': '//depot/gc/... //test_main/gc/...',
            'view2': '//depot/charlotte/... //test_main/charlotte/...'}

        * can toggle between reduce/flatten at your pleasure.

        USAGE:

            d = {'a':'gc', 'b':'charlotte'}

            atest = Flatten(**d)
            btest = atest.mergekeys(**{'a':'minou','c':'gareth'})
        or
            btest = Flatten(**d).mergekeys(**{'a':'minou','c':'gareth'})

            btest
            Out[3]: <Flatten({'a0': 'gc', 'a1': 'minou', 'b': 'charlotte', 'c': 'gareth'})>

            best.keys()
            Out[4 ['a0', 'a1', 'b', 'c0', 'c1']

            btest.reduce()
            Out[5]: <Flatten({'a': ['minou', 'gc'], 'b': 'charlotte', 'c': 'gareth'})>

            btest.keys()
            Out[6] ['a', 'b', 'c']

            btest.flatten()
            Out[7]: <Flatten({'a0': 'minou', 'a1': 'gc', 'b': 'charlotte', 'c': 'gareth'})>

            btest.keys()
            Out[8] ['a0', 'a1', 'b', 'c0', 'c1']

            btest.get_numbered_keys()
            Out[9] ['a', 'c']

            btest.get_max_key('a')
            Out[10]: 'a1'

            btest.get_max_key('b')
            Out[11]: None

            btest.get_max_key('a', ['a0', 'a1', 'a2', 'a3','a5', 'c', 'd0', 'd1', 'd3'])
            Out[12]: 'a5'

            btest.get_max_key('d', ['a0', 'a1', 'a2', 'a3','a5', 'c', 'd0', 'd1', 'd3'])
            Out[13]: 'd3'
    '''
    numex = re.compile('\d+$')

    __str__ = __repr__ = lambda self: f'<{type(self).__name__}({format(dict(self))})>'

    def __call__(self, *args, **kwargs):
        return self

    def __init__(self, *args, **kwargs):
        super(Storage, self).__init__()
        [self.merge(arg) for arg in args if isinstance(arg, dict)]
        self.merge(kwargs)
        self.expand()

    def mergekeys(self, *args, **kwargs):
        ''' merge a new dict
               * duplicate keys will be merged in a `flattened` state
               * initial non-numbered key will be renamed and numbered as key 0
                    e.g.: {'keyname': 'value'} --> {'keyname0': 'value'}
        '''
        if (len(kwargs) > 0):
            kwargs = Storage(kwargs)
            duplicatekeys = self.getkeys().intersect(kwargs.keys())
            previously_numbered_keys = self.get_numbered_keys()
            if (len(duplicatekeys) > 0):
                for dkey in duplicatekeys:
                    nextkey = 0
                    if (dkey in previously_numbered_keys):
                        nextkey = (self.get_max_key(dkey) + 1)
                    self.rename(dkey, f'{dkey}{nextkey}')
                    kwargs.rename(dkey, f'{dkey}{(nextkey + 1)}')
            self.merge(kwargs)
        return self

    def getfields(self):
        return self.getkeys()

    def get_max_key(
            self,
            key=None,
            iterable=None
    ):
        '''  return the max (highest) numbered key, as specified in `key`

                    e.g.: Aggr.get_max_key('key')

                * If no `iterable` is specified, then keys belonging to self will be referenced
        '''
        if (key is not None):
            if (iterable is None):
                iterable = self.getkeys()
            result = [field for field in iterable if (field.startswith(key))]
            if (len(result) > 1):
                return max(result)

    def get_numbered_keys(self):
        return Lst(
            set(
                [
                    re.sub(Lst(self.numex.findall(i))(0), '', i) for i in \
                    filter(lambda i: self.numex.search(i), self)
                ]
            )
        )

    def reduce(self):
        for nfield in self.get_numbered_keys():
            self.merge(
                {
                    nfield: Lst([self.pop(field) for field in \
                        self.getkeys() if (field.startswith(nfield))])
                }
            )
        return self

    def expand(self, addnewlines=False):
        dcopy = self.copy()
        for (key, value) in dcopy.items():
            if (isinstance(value, list)):
                values = self.pop(key).storageindex(reversed=True)
                for idx in values:
                    fvalue = '{}\n' if (addnewlines == True) else '{}'
                    iValue = fvalue.format(values[idx])
                    self.merge({f'{key}{idx}': iValue})
        return self

class NOFile(object):
    def __init__(self, oP4, filename=None, mode='r'):
        self.closed = False
        self.oP4 = oP4
        if (filename is None):
            self.oFile = StringIO()
        elif (hasattr(filename, 'read')):
            self.oFile = filename
        else:
            self.oFile = StringIO(filename)
        # super(noFile,self).__init__()

    def getvalue(self):
        return self.filename.getvalue()

    def _truncate(self, size=0):
        pos = self.oFile.tell()
        self.oFile.truncate(size)
        current_length = len(self.oFile.getvalue())
        if (size > current_length):
            self.oFile.seek(current_length)
            try:
                self.oFile.write(
                    b("\x00") * (size - current_length)
                )
            finally:
                self.oFile.seek(pos)

    def __call__(self):
        return self

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.closed = True

    def flush(self):
        return self.oFile.flush()

    def next(self):
        raise StopIteration

    def readlines(self, *args, **kwargs):
        return self.oFile.readline()

    def close(self):
        self.closed = True

    def read(self, size=None):
        return self.oFile.read()

    def seek(self, *args, **kwargs):
        return self.oFile.seek()

    def tell(self):
        return len(self.getvalue()) \
            if (hasattr(self, 'getvalue')) \
            else self.size \
            if (hasattr(self, 'size')) \
            else 0

    def truncate(self, *args, **kwargs):
        return self.oFile.tell()

    def write(self, data):
        self.oFile.write(data)
        self.oFile.seek(0)

    def writelines(self, *args, **kwargs):
        self.oFile.writelines(args)

def versionname_to_releasename(ver):
    return re.sub('^(\d){0,2}', 'r', ver) \
        if (reg_releaseversion.match(ver) is not None) \
        else ver

def is_marshal(obj):
    char = b'\x00' \
        if (type(obj) is bytes) \
        else '\x00'
    return True \
        if (char in obj) \
        else False

def isnum(i):
    try:
        if (not isinstance(i, bool)):
            return (isinstance(float(i), float))
    except:
        return False

def set_localport(p4droot):
    '''  RSH access.
        * requires p4droot to be set.
        * server logs: creates a 'serverlog' dir under p4droot
    '''
    p4droot = os.path.abspath(p4droot)
    p4dbin = os.path.join(p4droot, 'p4d')
    rsh_strConnect = f'rsh:{p4dbin}'
    serverlog = os.path.join(p4droot, 'serverlog')
    (
        serverroot_arg,
        serverlog_arg
    ) = \
        (
            f'-r {p4droot}',
            f'-L {serverlog}'
        )
    (
        inetd,
        subsys
    ) = \
        (
            '-i',
            '-vserver=3'
        )
    strPort = ' '.join(
                        [
                            rsh_strConnect,
                            serverroot_arg,
                            serverlog_arg,
                            inetd,
                            subsys
                        ]
    )
    ''' remove extra ' chars within the actual localport str.
    '''
    localport = reg_escape.sub('', strPort)
    try:
        os.makedirs(serverlog)
    except:
        pass
    return localport

'''  things gone wrong - exit!
'''
def raiseException(exception, msg):
    return exception(msg)

#def bail(err, exit=True):
#    ''' things gone wrong - exit!
#    '''
#    msg = err.message \
#        if (hasattr(err, 'message')) \
#        else err
#    if (isinstance(msg, dict)):
#        msg = pformat(msg)
#    msg = '\n'.join(["\nBailing...", msg])
#    if (exit is True):
#        sys.exit(msg)
#    print(msg)

def bail(err, exit=True, exception=None, logger=None):
    msg = err.message \
        if (hasattr(err, 'message')) \
        else err.args[0] \
        if (hasattr(err, 'args')) \
        else pformat(err) \
        if (isinstance(err, dict) is True) \
        else str(err)
    msg = f'Bailing...{msg}'

    if (logger is not None):
        logger(msg)

    if ((exit is True) and (exception is None)):
        sys.exit(msg)
    elif (exception is not None):
        raiseException(exception, msg)
    else:
        print(msg)

def remove(root, name=None):
    def removefile(filename):
        if (
                (not os.path.islink(filename))
                & (os.path.isdir(filename))
        ):
            try:
                shutil.rmtree(filename)
            except:
                [removefile(f) for f in os.listdir(filename)]
        else:
            deletefile = os.unlink \
                if (os.path.islink(filename)) \
                else os.remove
            deletefile(filename)

    filename = os.path.join(root, name) \
        if (name is not None) \
        else root

    if (os.path.exists(filename)):
        removefile(filename)


def itemgrouper(n, sequence):
    args = ([iter(sequence)] * n)
    return (
        [item for item in izl if item != None] for izl in itertools.zip_longest(*args)
    )

def itemgrouper_filler(n, sequence, fillvalue=None):
    args = ([iter(sequence)] * n)
    return itertools.zip_longest(*args, fillvalue=fillvalue)

'''  returns a tuple (key, value, op)
    
        I.e: 'depotFile#test'   --> ('depotFile', 'test', '#')    
'''
def getOpKeyValue(qry):
    (left, right) = (None, None)
    for op in sqOperators:
        if (op in qry):
            (
                left,
                right
            ) = (
                objectify(
                    {op:
                         {'keyvalues': qry.split(op, 1)
                          }
                     }
                ).getvalues()[0].keyvalues
            )
            return (
                left.strip(),
                right.strip(),
                op.strip()
            )
    return (
        left,
        right,
        None
    )

def queryToString(qry):
    ''' something to get <table>.<field><op>value

     arg can be one of manyb tyoes...
     1) DLGQuery
     2) P4Expression
     3) Py4Field/JNLField
    '''
    return f"{qry.tablename}.{qry.fieldname}{qry.op}{qry.right}" \


'''  returns a tuple (table, key, value, op)

     I.e: 'files.depotFile#test'   --> ('files', 'depotFile', 'test', '#')
'''
def getTableOpKeyValue(qry):
    (table, fieldname, value, op, inversion) = (None, None, None, None, None)
    for operator in sqOperators:
        if (operator in qry):
            (
                left,
                value
            ) = qry.split(operator, 1)
            (
                table,
                fieldname
            ) = Lst(left.split('.')) \
                if ('.' in left) \
                else (table, left)
            return (
                table,
                fieldname,
                value,
                operator
            )
    return (
        table,
        fieldname,
        value,
        op
    )

'''  convert a string query to a Storage

        I.e. >>> stringQueryToStorage("clients.client=gc.pycharm")
                {'left': {'tablename': 'clients',
                       'fieldname': 'client'},
                 'right': 'gc.pycharm',
                 'op': '='}
'''
def queryStringToStorage(qry):
    '''  qry may or may not contain the tablename
            eg. files.depotFile#\.py$  -> depotFile#\.py$

            assume it does, otherwise exception and handle accordingly
    '''
    inversion = False
    try:
        (
            tablename,
            fieldname,
            value,
            op
        ) = getTableOpKeyValue(qry)
        if (tablename.startswith('~')):
            tablename = re.sub('~', '', tablename)
            inversion = True
        return objectify(
            {
                'left':
                    {
                        'tablename': tablename,
                        'fieldname': fieldname
                  },
             'inversion': inversion,
             'right': value,
             'op': op
            }
        )
    except:
        (fieldname, op, value) = getOpKeyValue(qry)
        if (fieldname.startswith('~')):
            fieldname = re.sub('~', '', fieldname)
            inversion = True
        return objectify(
            {
                'left': {
                    'fieldname': fieldname,
                    'tablename': None
                },
                'inversion': inversion,
                'right': value,
                'op': op
            }
        )

class is_equal(str):
    def __eq__(self, otherfile):
        return self.upper() == otherfile.upper() \
            if (isinstance(otherfile, str)) \
            else False

class __containschars__(object):
    def __call__(self):
        return self

    def __init__(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        self.anycase = kwargs.anycase \
            if (isinstance(kwargs.anycase, bool)) \
            else False
        self.strNormal = self.normalize(
            args
            if (len(args) > 0)
            else self
        )
        self.strNil = string.maketrans('', '')

    def as_string(self, tStr):
        return str(tStr)

    def normalize(self, i):
        if (isinstance(i, str)):
            return i.lower() \
                if (self.anycase is True) \
                else i
        # elif (type(i) is UnicodeType):
        #    '''
        #            str.translate doesn't handle unicode :(
        #            for our own humble purposes, strip it out!
        #
        #            'NFC', 'NFKC', 'NFD', and 'NFKD'
        #                                                        '''
        #    i = unicodedata.normalize('NFKD', i).encode('ascii', 'ignore')
        #    return i.lower() if (self.anycase is True) else i
        elif (isinstance(i, dict)):
            if (isinstance(i, Storage) is False):
                i = objectify(i)
            (storekeys, storevalues) = (i.getkeys(), i.getvalues())
            storevalues = [item.lower() for item in storevalues if (isinstance(item, str))]
            nStr = ''.join(storevalues) if (self.anycase is True) else ''.join(storevalues)
            return nStr
        elif (isinstance(i, serializable)):
            if (type(i) is not Lst):
                i = Lst(i)
            i = Lst(item.lower() for item in i if (isinstance(item, str)))
            nStr = ''.join(i) if (self.anycase is True) else ''.join(i)
            return nStr

    def _any(self, other=None):
        if (other is None):
            return False
        return (len(other) != len(other.translate(self.strNil, self.strNormal)))

    def _strict(self, other=None):
        if (other is None):
            return False
        return (len(other.translate(self.strNil, self.strNormal)) > 0)

    def any(self, other):
        if (self.strNormal is None):
            return False
        return True \
            if (self._any(self.normalize(other)) is True) \
            else False

    def strict(self, other):
        if (self.strNormal is None):
            return False
        return True \
            if (self._strict(self.normalize(other)) is True) \
            else False

''' >>> containschars('blabla','al')    
    True
'''
def containschars(*args, **kwargs):
    (args, kwargs) = (Lst(args), Storage(kwargs))
    (searchin, term) = args if (len(args) == 2) else ('', '')
    if (len(args) != 2):
        print('bailing, 2 args required, got {} instead'.format(len(args)))
    strict = kwargs.strict if (isinstance(kwargs.strict, bool)) else False
    return getattr(__containschars__(searchin, **kwargs),
                   'any' \
                       if (strict is False) \
                       else 'strict')(term)

''' return (True, arg) if args contains a file mode, otherwise (False,None)
'''
def is_mode(*args):
    for arg in args:
        if (containschars('rwadeicob+~', arg, strict=True) is True):
            return True, arg
    return False, None


def is_expression(exp):
    '''
           return (True, op) if op contains
           something (anything), otherwise (False, None)
    '''
    eTable = equivalence_table()
    for key in eTable.getkeys():
        op = eTable[key].op
        if (
                (containschars(exp, op, strict=True, ))
                and (not containschars(op, '@'))
        ):
            return True, op
    return False, None

def ALLLOWER(litems, includekeys=False):
    if (isinstance(litems, str)):
        return litems.lower()
    elif (isinstance(litems, dict) is True):
        litems = Storage(litems)
        for (key, value) in litems.items():
            if (isinstance(value, str)):
                if (includekeys is True):
                    litems.merge({key: value.lower()}).rename(key, key.lower())
                else:
                    litems.merge({key: value.lower()})
        return litems
    #elif (isinstance(litems, (list, tuple, set)) is True):
    elif (is_array(litems) is True):
        origintype = type(litems)
        litems = Lst(litems)
        for item in litems:
            if (isinstance(item, str)):
                if (item.lower() != item):
                    idx = litems.index(item)
                    litems.insert(idx, litems.pop(idx).lower())
        return origintype(litems)
    else:
        return litems

def ALLUPPER(litems, includekeys=False):
    if (isinstance(litems, str)):
        return litems.upper()
    elif (isinstance(litems, dict) is True):
        litems = Storage(litems)
        for (key, value) in litems.items():
            if (isinstance(value, str)):
                if (includekeys is True):
                    litems.merge({key: value.upper()}).rename(key, key.upper())
                else:
                    litems.merge({key: value.upper()})
        return litems
    #elif (isinstance(litems, (list, tuple)) is True):
    elif (is_array(litems) is True):
        return Lst(item.upper() for item \
                   in litems if (isinstance(item, str)))
    else:
        return litems

class cachedprop(object):
    def __init__(self, fget, doc=None):
        self.fget = fget
        self.__doc__ = doc or fget.__doc__
        self.__name__ = fget.__name__

    def __get__(self, obj, cls):
        if obj is None:
            return self
        obj.__dict__[self.__name__] = result = self.fget(obj)
        return result


class IsMatch(object):
    '''
            something for quick matches (return 'string', (True/False))

            >>> IsMatch('^.*[0-9]$')('blabla123')
            blabla123, True

            >>> IsMatch('^.*[0-9]$')('fred')
            fred, False
    '''
    def __init__(
            self,
            expression,
            strict=False,
            search=False,
            extract=False,
            groupidx=None
    ):
        if (not hasattr(expression, 'search')):
            if (
                    ((strict is True) | (search is True))
                    & (not expression.startswith('^'))
            ):
                expression = '^{}'.format(expression)
            if (
                    ((strict is True) | (search is True))
                    & (not expression.endswith('$'))
            ):
                expression = '{}$'.format(expression)
        self.regex = re.compile(expression) \
            if (isinstance(expression, str)) \
            else expression \
            if (hasattr(expression, 'search')) \
            else None
        (self.groupidx, self.extract) = (groupidx, extract)

    def __call__(self, value):
        if (
                (self.regex is not None)
                & (hasattr(self.regex, 'search'))
        ):
            match = self.regex.search(value)
            if (match is not None):
                match_group = match.group() \
                    if (self.groupidx is None) \
                    else match.group(self.groupidx)
                return (self.extract and match_group or value, True)
        return (value, False)


def storageIndexToList(_storeidx, default='idx'):
    '''  reduces the StorageIndex to a list of record-like items - creates
            an id field with the keys....

            >>> storeidx=StorageIndex({0:{'a': 'gc'},
                                       1:{'b': 'charlotte'},
                                       2:{'c': 'normand'}})
            >>> storeidx.reduceToList()
            [[{'id': 0}, {'a': 'gc'}],
            [{'id': 1}, {'b': 'charlotte'}],
            [{'id': 2}, {'c': 'normand'}]]
    '''

    return Lst(Storage({'idx': default}).merge(_storeidx[idx])
               if ()
               else '' for (idx, value) in _storeidx.items())

def Casttype(_type, value):
    try:
        if (isinstance(_type, (dict, Storage))):
            return objectify(value)
        elif (isinstance(_type, list)):
            return Lst(value)
        elif (isinstance(_type, str)):
            return eval('{}({})'.format(_type, value))
        elif (
                (isinstance(_type, (None, bool)) is False)
                & (callable(_type) is True)
        ):
            return _type(value)
        else:
            return value
    except Exception as err:
        print('CasttypeError: {}'.format(err))
    return value


(decimals_default, average_default) = (2, False)
def fractions2Float(*args, **kwargs):
    '''  assumes that fractions are of type string, otherwise (call on func floats2percent)

            ** kwargs.average is True -> returns an average float
    '''
    (args, kwargs) = (Lst(args), Storage(kwargs))
    decimals = kwargs.decimals or decimals_default
    average = kwargs.average or average_default
    sums = [float((Fraction(arg)) \
                      if ((isinstance(arg, str)) and ('/' in arg)) \
                      else float(Fraction(str(arg))) \
        if (isinstance(arg, float)) \
        else arg) for arg in args]
    result = (sum(sums) / len(sums)) \
        if (average is True) \
        else sum(sums)
    return round(float(result), decimals)

def percents2Float(*args, **kwargs):
    '''  assumes that npercents are string and that they end with a %, otherwise skip

            ** kwargs.average is True -> returns an average float
    '''
    (args, kwargs) = (Lst(args), Storage(kwargs))
    decimals = kwargs.decimals or decimals_default
    average = kwargs.average or average_default
    sums = [float(arg.strip('%')) for arg in args if ((isinstance(arg, str)) and ('%' in arg))]
    result = (sum(sums) / len(sums)) if (average is True) else sum(sums)
    return round(float(result), decimals)

def is_int_hex_or_str(item):
    '''  easy solution to check if item is an int,
         a hex, a str or just plain old None

         results:
            returns -1, user passed in NoneType
            returns 0, it's a str
            returns 1, it's a hex
            retruns 2, it's an int

         eg.

         >>> is_int_hex_or_str('99')
         2                              #  it's an int
         >>> is_int_hex_or_str('0x14')
         1                              #  it's a hex
         >>> is_int_hex_or_str('Super')
         0                              #  it's a str
         >>> is_int_hex_or_str(None)
         -1

         ** except for a None value, item MUST be of type str.
            Otherwise Python will interprete 0x14 as being `20`,
            thereby returning an erroneous value (2 instead of 1)
    '''
    (item, res) = (str(item), 0)
    if (item == 'None'):
        res = -1
    else:
        for baseitem in (10, 16):
            try:
                int(item, baseitem)
                res += 1
            except ValueError:
                pass
    return res


    try:
        int(item, 10)
        res += 1
    except ValueError: pass
    try:
        int(item, 16)
        res += 1
    except ValueError: pass
    return res

def equivalence_table():
    return objectify(
        {
            'startswith': {'op': '#'},
            'notstartswith': {'op': '!#'},
            'endswith': {'op': '#'},
            'notendswith': {'op': '!#'},
            'contains': {'op': '#'},
            'notcontains': {'op': '!#'},
            'implies': {'op': '>>'},
            'belongs': {'op': '@'},
            'notbelongs': {'op': '!@'},
            'equal': {'op': '='},
            '__eq__': {'op': '='},
            'notequal': {'op': '!='},
            'not': {'op': '!='},
            '__ne__': {'op': '!='},
            'greaterthan': {'op': '>'},
            '__gt__': {'op': '>'},
            'lesserthan': {'op': '<'},
            '__lt__': {'op': '<'},
            'greaterequal': {'op': '>='},
            '__ge__': {'op': '>='},
            'lesserequal': {'op': '<='},
            '__le__': {'op': '<='}
        }
    )


def isfsfile(filename):
    return (re.match(r'^\/[^/]|^[^/].*\/.*$', filename) is not None)

def isdepotfile(filename):
    return (re.match(r'//.*$', filename) is not None)

def isanyfile(filename):
    return ((isdepotfile(filename)) | (isfsfile(filename)))

def real_tablename(tablename):
    if (re.match(r'^db\.', tablename) is None):
        return f"db.{tablename}"
    return tablename


def friendly_tablename(tablename):
    if (re.match(r'^db\..*$', tablename) is not None):
        return re.sub(r'db\.', '', tablename)
    return tablename

class fix_name(object):
    def __init__(self, tableformat='remove'):
        self.tableformat = tableformat
        '''     alternative names for tables and fields where
                p4 table &\| field names map to SQL reserved
                words

                change keys in the reserved_keywords below,
                but do continue to consider the values,
                otherwize you may (I mean, will most certainly)
                hit problems while defining the tables
        '''
        self.reserved_keywords = Storage(
                                    {
                                        'group': 'p4group',
                                        'db.group': 'p4group',
                                        'type': 'p4type',
                                        'user': 'p4user',
                                        'db.user': 'p4user',
                                        'id': 'idx',
                                        'db.desc': 'describe',
                                        'desc': 'describe'
                                    }
        )

    def remove(self, tblitems):
        [tblitems.pop(i) for i in (0, -1) if (tblitems(i) == 'db')]
        return ''.join(tblitems)

    def normalize(self, tblitems):
        return ''.join(tblitems)

    def replace(self, tblitems):
        return '_'.join(tblitems)

    def is_reserved(self, name):
        return False \
            if (not name in self.reserved_keywords.keys()) \
            else True

    def normalizeTableName(self, name):
        tblitems = Lst(name.split('.'))
        return self.normalize(tblitems) \
            if (self.tableformat == 'normalize') \
            else self.remove(tblitems) \
            if (self.tableformat == 'remove') \
            else self.replace(tblitems)

    def __call__(self, name, table_or_field='table'):
        return self.normalizeTableName(name)
        # if (table_or_field=='table'):
        #    name=self.normalizeTableName(name)
        # return name if (self.is_reserved(name) is False) else self.reserved_keywords[name]

''' just a quicker, no-hassle shortcut func to class fix_name)
'''
def fix_tablename(name):
    return fix_name().normalizeTableName(name)

class Plural(object):
    '''     a bit of an overkill, but still -

            KISS -> just keep the rules we need in this p4 context (spec/specs).

            something to get a spec's singular from its plural and/or
            a spec's plural from its singular

            rules are simple....
                    search, sub & replace
                    return the plural form of a spec, or None

                    we validate:
                        1) the word against rules
                        2) the words against p4 commands

    *** english grammar rules (as I can possibly understand them!)
    https://englishgrammarsoft.com/singular-and-plural-nouns-rules-examples/

    #+--------+----------------------------------+----------------------------+-------------------------+------------+
    # cols:   |               A                  |             B              |          C              |      D     |
    #+--------+----------------------------------+----------------------------+-------------------------+------------+
    #  A-C    |                                  |                            |                         |            |
   [          {'A':          'lf'                ,                            ,      'C':  'lves'       ,            },
              {'A':          ''                  ,                            ,      'C':  's'          ,            },
              {'A':          'us'                ,                            ,      'C':  'i'          ,            },
              {'A':          'on'                ,                            ,      'C':  'a'          ,            },
    #         |                                  |                            |                         |            |
    #+--------+----------------------------------+----------------------------+-------------------------+------------+
    # A-B-C   |                                  |                            |                         |            |
              {'A':     '[^aeioudgkprt]h'        ,     'B':  ''               ,      'C':  'es'                      },
              {'A':     '(qu|[^aeiou])y'         ,     'B':  'y'              ,      'C':  'ies'                     },
    #         |                                  |                            |                         |            |
    #+--------+----------------------------------+----------------------------+-------------------------+------------+
    # A-B-C-D |                                  |                            |                         |            |
              {'A':     '[(ss)(sh)(ch)sxz]'      ,   'B':  ''                 ,      'C':  'es'         ,  'D':'e'}  ]
    #         |                                  |                            |                         |            |
    #+--------+----------------------------------+----------------------------+-------------------------+------------+

        usage:
                    >>> oPlural = Plural()
                    >>> oPlural.pluralize('client')
                    'clients
                    >>> oPlural.pluralize('clients')
                    None
                    >>> oPlural.singularize('clients')
                    'client'
                    >>> oPlural.singularize('client')
                    None
                    >>> oPlural.is_plural('client')
                    False
                    >>> oPlural.is_plural('clients')
                    True
                    >>> oPlural.is_singular('client')
                    True
                    >>> oPlural.is_singular('clients')
                    False
    '''

    def __init__(self):
        self.rules = objectify(
            [
                {'A': '[(ss)(sh)(ch)sxz]', 'B': '', 'C': 'es', 'D': 'e'},
                {'A': '[^aeioudgkprt]h', 'B': '', 'C': 'es'},
                {'A': '(qu|[^aeiou])y', 'B': 'y', 'C': 'ies'},
                {'A': '', 'C': 's'}
            ]
        )

    def __call__(self):
        return self

    def rulecompiler(self, exp=''):
        return re.compile(f'{exp}$')

    def get_A_B_C(self, rule, case, term):
        if ((case != 'plural') and ('C' in rule.keys())):
            return (self.rulecompiler(rule.A),
                    self.rulecompiler(rule.B) \
                        if (rule.B is not None) \
                        else self.rulecompiler(rule.A),
                    rule.C)
        else:
            return (
                    self.rulecompiler(rule.C), self.rulecompiler(rule.C), rule.A \
                        if (not term[-1] in ['s', 'x', 'z']) \
                        else rule.D
                    if (rule.D is not None)
                    else ''
            )

    def pluralize_sequence(self, seq=Lst(), as_dict=True):
        sequence = Lst(seq) \
            if (type(seq) is not Lst) \
            else seq
        if (noneempty(seq) is False):
            plural_sequence = Lst(self.pluralize(spec) for spec in sequence)
            return Storage(
                zip(
                    sequence,
                    plural_sequence
                )
            ) \
                if (as_dict is True) \
                else plural_sequence

    def singularize_sequence(self, seq=Lst(), as_dict=True):
        sequence = Lst(seq) if (type(seq) is not Lst) else seq
        if (noneempty(seq) is False):
            singular_sequence = Lst(self.singularize(spec) for spec in sequence)
            return Storage(
                zip(
                    sequence,
                    singular_sequence
                )
            ) \
                if (as_dict is True) \
                else singular_sequence

    def pluralize(self, singular):
        for rule in self.rules:
            (A, B, C) = self.get_A_B_C(rule, 'singular', singular)
            if (noneempty(A.search(singular)) is False):
                return B.sub(C, singular)

    def singularize(self, plural):
        for rule in self.rules:
            (A, B, C) = self.get_A_B_C(rule, 'plural', plural)
            if (noneempty(A.search(plural)) is False):
                return B.sub(C, plural)

    def is_plural(self, wd):
        return True \
            if (noneempty(self.singularize(wd)) is False) \
            else False

    def is_singular(self, wd):
        return False \
            if (noneempty(self.singularize(wd)) is False) \
            else True