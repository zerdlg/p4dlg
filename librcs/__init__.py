import re
from io import StringIO

from libdlg import Lst, bail
from libdlg.dlgError import *

''' The librcs package owes much of its functionality to James Strickland. 
    Originaly released in March 1999 and updated in November 2011.
'''
''' Introduction texts taken from selected modules from said converter written by James.

    ... cvs2p4.py
    
    CVS to Perforce converter.
    All authors, dates, revisions, and branching relationships are preserved.
    Labels are output to files which can be used to create labels with cvstop4makelabels.py
    (see the latter for more details and the reasons why).

    The converter works in two stages. In the first stage, the list of revisions
    and labels/branches is determined by reading each RCS file. The list of
    revisions is then sorted by date:time, and drives the second stage, which
    is the generation of the corresponding Perforce metadata.
    
    See cvstop4.config for configuration options. Prior to running the
    script you will need to at least specify where the CVS files are located
    and where you would like the Perforce data stored.
    
            Written by James Strickland June 2011
            Relies on my rcs.py library.
            
    ... rcs.py
    
    Module to read RCS files, as defined by the grammar given in rcsfile(5).
    This is a totally self-contained module.  If run from the command line
    it will print out the metadata for any RCS files you specify.

    TODO:
    - add support for a whole bunch of useful utilities such as
      explode, collect, merge, insert

    James Strickland
    Perforce Software
    March 1999

    May 2011: Updated and expanded to include the ability to create, modify and
    write RCS files.
'''


'''  [$File: //dev/p4dlg/librcs/__init__.py $] [$Change: 472 $] [$Revision: #6 $]
     [$DateTime: 2024/09/03 03:46:02 $]
     [$Author: mart $]
'''

__all__ = [
    'atquote',
    'revstringtolist',
    'READBLOCKSIZE',
    'whitespace',
    'separator',
    'num',
    'idchar',
    'id',
    'sym',
    'symcolonnum',
    'idcolonnum',
    'anystring',
    'ZEROORONE',
    'ZEROORMORE',
    'ONE',
    'readlines',
    'ParseRCSError'
]

READBLOCKSIZE = 128
''' RCS definition of "white space" is ' \b\t\n\v\f\r' 
    ... seems like a bit much, let's just do '\s' instead
'''
whitespace = ' \b\t\n\v\f\r' #'\s'
separator = whitespace + ';'
num = re.compile(r'[\d\.]+')
idchar = re.compile(r'[^$,.:;@]')
id = re.compile(f'({num.pattern})?{idchar.pattern}({idchar.pattern}|{num.pattern})*')
sym = re.compile(f'\d*{idchar.pattern}({idchar.pattern}|\d)*')
symcolonnum = re.compile(f'{sym.pattern}:{num.pattern}')
idcolonnum = re.compile(f'{id.pattern}:{num.pattern}')
anystring = re.compile(r'.*')
ZEROORONE = 0
ONE = 1
ZEROORMORE = 2

def atquote(s):
    ''' return string quoted using @, as used in RCS files.
    '''
    qstr = re.sub('@', '@@', s)
    return f'@{qstr}@'


def revstringtolist(revstring):
    ''' Return an RCS revision string as an array of numbers, or [] if the string is invalid.
    '''
    revs = Lst()
    for rs in revstring.split('.'):
        try:
            rsnum = int(rs)
        except ValueError:
            return revs
        if (rsnum < 0):
            return revs
        revs.append(rsnum)
    return revs

def readlines(s):
    ''' With 2.7, cStringIO.StringIO(s).readlines() was the only way to split a string into
        lines without either losing the \n's and gaining an extra, empty string
        (string.split()) or splitting on more than \n (str.splitlines(True)), which
        causes a failure on "binary" RCS files or files with \r

        Now, io.StringIO.readlines should do the same job?
    '''
    try:
        out = StringIO(s).readlines()
        if (type(out) is bytes):
            out.decode('utf-8')
        return out
    except Exception as err:
        bail(err)

class ParseRCSError(RCSError):
    def __init__(
            self,
            RCSobject,
            expecting,
            found
    ):
        RCSError.__init__(self)
        (
            self.RCSobject,
            self.expecting,
            self.found
        ) = \
            (
                RCSobject,
                expecting,
                found
            )
        self.offset = self.RCSobject.tell()

    def __str__(self):
        msg = f'''Parse error at file offset {self.offset} while processing
{self.RCSobject.sourcefilename}
Expected: {self.expecting}
Found: {self.found}
'''
        try:
            offset = max(self.offset - 256, 0)
            self.RCSobject.infile.seek(offset)
            msg += f'Context:\n====\n{self.RCSobject.infile.read(512)}\n====\n'
        except:
            pass
        return msg