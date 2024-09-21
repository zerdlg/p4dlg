import sys, os, re
import io, platform
import ast
import json
from pprint import pformat
from stat import (S_IREAD, S_IRGRP, S_IROTH, S_IWUSR,
                  S_IXGRP, S_IXUSR, S_ISDIR, S_ISREG)
import mimetypes
import hashlib
import gzip, tarfile
if (sys.version_info.major == 2):
    try:
        from CStringIO import StrinIO as StringIO
    except ImportError:
        from StringIO import StringIO
elif (sys.version_info.major == 3):
    from io import StringIO as StringIO
try:
    import cPickle as pickle
except ImportError:
    import pickle

from libdlg.dlgStore import Storage, Lst, StorageIndex
from libdlg.dlgQuery_and_operators import AND,OR, XOR, NOT

'''  [$File: //dev/p4dlg/libdlg/dlgFileIO.py $] [$Change: 467 $] [$Revision: #10 $]
     [$DateTime: 2024/08/24 08:15:42 $]
     [$Author: mart $]
'''

def raiseException(exception, msg):
    return exception(msg)

def bail(err, exit=True, exception=None, logger=None):
    msg = err.message if (hasattr(err, 'message')) \
        else err.args[0] if (hasattr(err, 'args')) \
        else pformat(err) if (isinstance(err, dict) is True) \
        else str(err)
    if msg == "No module named 'libpy4.p4model'":
        print('here')
    msg = f'Bailing...{msg}'
    if (logger is not None):
        logger(msg)
    #else:
    #    print(msg)
    if AND(
            (exit is True),
            (exception is None)
    ):
        sys.exit(msg)
    elif (exception is not None):
        raiseException(exception, msg)

try:
    import fcntl

    posixflags = {'LOCK_SH': fcntl.LOCK_SH \
        , 'LOCK_EX': fcntl.LOCK_EX \
        , 'LOCK_NB': fcntl.LOCK_NB \
        , 'LOCK_UN': fcntl.LOCK_UN}
except:
    '''     not sure on the necessity
            to add windows support
    '''
    try:
        import win32con
        import win32file
        import pywintypes

        windowsflags = {'LOCK_SH': 0 \
            , 'LOCK_EX': win32con.LOCKFILE_EXCLUSIVE_LOCK \
            , 'LOCK_NB': win32con.LOCKFILE_FAIL_IMMEDIATELY \
            , 'OVERLAPPED': pywintypes.OVERLAPPED}
    except ImportError as err:
        #bail('no file locking unless you install the win32 extensions' \
        #        if (platform.system() == 'Windows') \
        #        else f'ImportError: {err}')
        raise raiseException(ImportError,
            'no file locking unless you install the win32 extensions' \
                if (platform.system() == 'Windows') \
                else f'ImportError: {err}')

try:
    from functools import wraps as wraps
except ImportError:
    wraps = lambda f: f

__all__ = [
            'ispath', 'is_compressed', 'isanyfile', 'isclientfile', 'isdepotfile', 'file_isempty',
            'isfile', 'isfsfile', 'isrecursive', 'iswildcard',
            'get_fileobject', 'fileopen', 'readlines', 'readfile', 'writefile',
            'readwrite', 'readwritepickle', 'loadpickle', 'loadspickle', 'dumppickle', 'dumpspickle',
            'Hash', 'guessfilemode', 'is_readable', 'is_writable', 'make_writable',
            'lopen', 'definemode', 'loadconfig', 'dumpconfig', 'loaddumpconfig'
]

def readwritepickle(filename, value=None):
    oFile = get_fileobject(filename, 'r+b')
    try:
        objFile = loadpickle(filename)
        oFile.seek(0)
        oFile.write(objFile)
        oFile.truncate()
        out = ast.literal_eval(str(objFile))
        return Storage(out) \
            if (isinstance(out, dict)) \
            else out or Storage()
    finally:
        oFile.close()

def loadpickle(filename):
    if (os.path.exists(filename) is False):
        return Storage()
    pFile = get_fileobject(filename, 'rb')
    try:
        out = pickle.load(pFile)
        return Storage(out) \
            if (isinstance(out, dict)) \
            else out or Storage()
    except pickle.UnpicklingError as err:
        return readwritepickle(filename)
    except EOFError:
        return Storage()
    except Exception as err:
        e = raiseException(Exception, err)
        return Storage()
    finally:
        pFile.close()

def loadspickle(obj):
    try:
        out = ast.literal_eval(str(pickle.loads(obj)))
        return Storage(out) \
            if (isinstance(out, dict)) \
            else out or Storage()
    except pickle.UnpicklingError as err:
        print(err, False)
    except EOFError:
        return obj

def dumppickle(obj, filename):
    pFile = get_fileobject(filename, 'wb')
    try:
        obj = dict(obj) \
            if (type(obj) in (Storage, StorageIndex)) \
            else obj
        pickle.dump(obj, pFile)
    finally:
        pFile.close()

def dumpspickle(obj):
    obj = dict(obj) \
        if (type(obj) in (Storage, StorageIndex)) \
        else list(obj) \
        if (type(obj) is Lst) \
        else obj
    return pickle.dumps(obj)

def definemode(mode):
    for m in (
                'wr',
                'r',
                'a',
                'w'
    ):
        if AND(
                (m in mode),
                (not 'b' in mode)
        ):
            return re.sub(m, f'{m}b', mode)
    return mode

""" https://stackoverflow.com/questions/13371444/python-check-file-is-locked 

    Interesting solution from StackOverflow for validating locked files on windows

    os.access on windows return bad value (returns True even if you have no permissions on it), 
"""
def isFileLocked(filePath):
    '''
    Checks to see if a file is locked. Performs three checks
        1. Checks if the file even exists
        2. Attempts to open the file for reading. This will determine if the file has a write lock.
            Write locks occur when the file is being edited or copied to, e.g. a file copy destination
        3. Attempts to rename the file. If this fails the file is open by some other process for reading. The
            file can be read, but not written to or deleted.
    @param filePath:
    '''
    if not (os.path.exists(filePath)):
        return False
    try:
        f = open(filePath, 'r')
        f.close()
    except IOError:
        return True

    lockFile = filePath + ".lckchk"
    if (os.path.exists(lockFile)):
        os.remove(lockFile)
    try:
        os.rename(filePath, lockFile)
        import time
        time.sleep(1)
        os.rename(lockFile, filePath)
        return False
    except WindowsError:
        return True

class lopen(object):
    ''' autolock files on open and auto unlock on close

        use as any other file opener, eg.:

            >>> oFile=lopen(filename,'rb')
            >>> try:
            >>>     return oFile.readlines()
            >>> finally:
            >>>     oFile.close()
    '''

    def __init__(self, filename, mode='rb'):
        self.closed = False
        self.locking = 'posix' \
            if (os.name == "posix") \
            else 'windows'
        mode = definemode(mode)

        openfile = gzip.GzipFile \
            if (is_compressed(filename) is True) \
            else tarfile.TarFile \
            if (tarfile.is_tarfile(filename) is True) \
            else open

        self.ofile = openfile(filename, mode)

        _flag = 'LOCK_SH' \
            if AND(
                    ('r' in mode),
                    (os.path.exists(filename))
        ) \
            else 'LOCK_EX' \
            if OR(
                    ('w' in mode),
                    ('a' in mode)
        ) \
            else None
        self.lock(_flag)
        if AND(
                ('w' in mode),
                (not 'a' in mode)
        ):
            self.ofile.seek(0)
            self.ofile.truncate()

    def __del__(self): self.ofile.close()

    def __enter__(self): return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __call__(self):
        return self

    def get_flag(self, flag):
        return posixflags[flag] \
            if (self.locking == 'posix') \
            else windowsflags[flag]

    def lock(self, flag):
        if (self.locking == 'posix'):
            fcntl.flock(
                        self.ofile.fileno(),
                        self.get_flag(flag)
            )
        else:
            win32file.LockFileEx(
                                 win32file._get_osfhandle(self.ofile.fileno()),
                                 self.get_flag(flag),
                                 0,
                                 0x7fff0000,
                                 self.get_flag('OVERLAPPED')()
            )
        self.locked = True

    def unlock(self):
        if (self.locking == 'posix'):
            fcntl.flock(self.ofile.fileno(), self.get_flag('LOCK_UN'))
        else:
            win32file.UnlockFileEx(
                                    win32file._get_osfhandle(self.ofile.fileno()),
                                    0,
                                    0x7fff0000,
                                    self.get_flag('OVERLAPPED')()
            )
        self.locked = False

    def extract(self, member, path):
        try:
            return self.oFile.extract(member, path)
        except Exception as err:
            bail(err)

    def extractfile(self, member):
        try:
            return self.oFile.extractfile(member)
        except Exception as err:
            bail(err)

    def extractall(self, path, members):
        try:
            return self.oFile.extractall(path, members)
        except Exception as err:
            bail(err)

    def read(self, size=None):
        return self.ofile.read() \
            if (size is None) \
            else self.ofile.read(size)

    def readline(self):
        return self.ofile.readline()

    def readlines(self):
        return self.ofile.readlines()

    def append(self, data):
        self.self.oFile.append(data)
        self.ofile.flush()

    def write(self, data):
        self.ofile.write(data)
        self.ofile.flush()

    def writelines(self, data):
        sequence = self.force_list(data)
        if (len(sequence) > 0):
            self.ofile.writelines(sequence)
            self.ofile.flush()

    def force_list(self, data):
        return list(data) \
            if (type(data) not in (list, Lst)) \
            else data

    def close(self):
        self.unlock()
        self.ofile.close()
        self.closed = True

''' file permision bits (readable/writable)

    S_IREAD     read by owner
    S_IWRITE    write by owner
    S_IEXEC     execute by owner

    S_IRWXU     read, write, execute by owner
    S_IRUSR     read by owner
    S_IWUSR     write by owner
    S_IXUSR     execute by owner

    S_IRWXG     read, write, execute by group
    S_IRGRP     read by group
    S_IWGRP     write by group
    S_IXGRP     execute by group0

    S_IRWXO     read, write, execute by others
    S_IROTH     read by others
    S_IWOTH     write by others
    S_IXOTH     execute by others
'''
def is_writable(filename):
    try:
        return os.access(filename, os.W_OK)
    except:
        return False

def is_readable(filename):
    try:
        return os.access(filename, os.R_OK)
    except:
        return False

def is_exec(filename):
    try:
        return os.access(filename, os.X_OK)
    except:
        return False

def make_writable(filename):
    try:
        os.chmod(filename, S_IWUSR | S_IREAD)
    except OSError as err:
        pass

def make_readable(filename):
    try:
        os.chmod(filename, S_IREAD | S_IRGRP | S_IROTH)
    except OSError as err:
        pass

def make_executable(filename):
    try:
        os.chmod(filename, S_IXUSR | S_IXGRP)
    except OSError as err:
        pass

def recurse(root, callback):
    for filename in os.listdir(root):
        filepath = os.path.join(root, filename)
        fmode = os.lstat(filepath).st_mode
        if (S_ISDIR(fmode) is True):
            recurse(filepath, callback)
        elif (S_ISREG(fmode) is True):
            callback(filepath)
        else:
            print(f'Skipping {filepath}')

def file_isempty(p):
    if (os.path.isfile(p)):
        return False \
            if (os.path.getsize(p) > 0) \
            else True

def ispath(p):
    ''' is file & exists on FS '''
    if (isinstance(p, str)):
        return (os.path.exists(p))
    return False

def isserialized(filename):
    return (re.match(r'^[\<{].*[}\/\>]$', re.sub('\n', '', filename)) is not None)

def isxmldoc(_file):
    if AND(
            (isinstance(_file, str)),
            (isfile(_file) is False)
    ):
        return (re.match(r"^\<\?xml (.*?)\?\>$", re.sub('\n', '', _file)) is not None)
    return False

def is_compressed(f):
    ''' check if gzip:
           if (fileobj.read(2)=='\037\213'):
               yes, this is a gzip file         '''
    if (hasattr(f, 'read') is True):
        return False
    mtype = mimetypes.guess_type(f)
    return True \
        if (mtype[1] == 'gzip') \
        else False

def isfsfile(_file):
    return (re.match(r'^\/[^/]|^[^/].*\/.*$', _file) is not None)

def isdepotfile(_file):
    return AND(
                (re.match(r'//.*$', _file) is not None),
                (not _file.endswith('/'))
    )

def isclientfile(_file):
    return (re.match(r'^//[^0-9].*/[^0-9].*[^/]$', _file) is not None)

def isanyfile(_file):
    for (name, isfunc) in {
                            'is_depotfile': isdepotfile,
                            'is_fsfile': isfsfile,
                            'is_clientfile': isclientfile
                        }.items():
        if (isfunc(_file) is True):
            return True
    return False

def isfile(_file):
    return (re.match(r'^[.*]|[//]|/.*$', _file) is not None)

def isrecursive(_file):
    return (re.match(r"^.*(\.\.\.)|^.*(\*\*).*$", _file) is not None)

def iswildcard(_file):
    return (re.match(r"^.*(\.\.\.)|.*\*$", _file) is not None)

def get_fileobject(filename, mode='rb', lock=False):
    '''  return something with attribute 'read'

            ** not responsible for closing the file
    '''
    attr = 'write' \
        if OR(
                ('w' in mode),
                ('+' in mode)
    ) \
        else 'append' \
        if ('a' in mode) \
        else 'read'

    if (hasattr(filename, attr)):
        return filename

    error = None
    filename = os.path.abspath(filename)

    if (isfile(filename) is True):
        try:
            if (attr in ('write', 'append')):
                if (not is_writable(filename)):
                    make_writable(filename)
            elif (attr == 'read'):
                if (not is_readable(filename)):
                    make_readable(filename)
        except Exception as err:
            print(err)

        openfile = lopen \
            if (lock is True) \
            else gzip.GzipFile \
            if (is_compressed(filename) is True) \
            else open
        #else tarfile.TarFile if (filename.endswith('.tar') is True) \
        #else open
        try:
            return openfile(filename, mode)
        except Exception as err:
            error = f"No such file '{filename}.\n{err}'"
    if AND(
                (isinstance(filename, str) is True),
                (not (ispath(filename))
            )
    ):
        try:
            return StringIO(filename)
        except Exception as err:
            error = err
    if (error is not None):
        bail(error)

def fileopen(
                 filename,
                 mode='rb',
                 lock=False
):
    return get_fileobject(filename, mode=mode, lock=lock)

def writefile(
                outfile,
                strData,
                mode='w',
                lock=False
):
    oFile = fileopen(outfile, mode, lock=lock)
    try:
        return oFile.write(strData)
    finally:
        oFile.close()

def readfile(
                filename,
                mode='r',
                lock=False
):
    f = fileopen(filename, mode, lock=lock)
    try:
        return f.read()
    finally:
        f.close()

def readlines(
                filename,
                mode='r',
                lock=False
):
    f = fileopen(filename, mode, lock=lock)
    try:
        return f.readlines()
    finally:
        f.close()

def readwrite(
                filename,
                value=None,
                lock=False
):
    oFile = fileopen(filename, mode='r+b', lock=lock)
    try:
        content = oFile.read() \
            if (value is None) \
            else value
        oFile.seek(0)
        oFile.write(content)
        oFile.truncate()
        return content
    finally:
        oFile.close()

def loadconfig(filename):
    oFile = fileopen(filename, 'r')
    try:
        return Storage(json.load(oFile))
    finally:
        oFile.close()

def dumpconfig(
                filename,
                value,
                indent=4,
                lock=False
):
    oFile = fileopen(filename, 'w', lock=lock)
    try:
        return json.dump(value, oFile, indent=indent)
    finally:
        oFile.close()

def loaddumpconfig(
                    filename,
                    value=None,
                    indent=4,
                    lock=False
):
    oFile = fileopen(filename, mode='r+b', lock=lock)
    try:
        content = dict(json.load(oFile))  \
            if (value is None) \
            else value
        oFile.seek(0)
        json.dump(content, oFile, indent=indent)
        oFile.truncate()
        return Storage(content)
    finally:
        oFile.close()

def guessfilemode(*args, **kwargs):
    '''  guess a file's mode (read, write, read/write, etc...) - includes p4 opened file flags

            idex values:  =1   ->  default / filemode not in args (read)
                          =2   ->  filemode is a keyword (_index,keyword)
                          >=0  ->  the actual index of filemode (args does contain mode)
    '''
    regex = {
        r'^[r+b+]+$': 'read',
        r'^[w+b+]+$': 'write',
        r'^[rw][wr][b+]+$': 'read_write',
        r'^[a+]+$': 'append'
    }

    (args, kwargs) = (Lst(args), Storage(kwargs))

    def iterreg(*args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        (
            idex,
            mode,
            opened
        ) = \
            (
                None,
                'rb',
                'read'
            )
        for iarg in args:
            opened = 'read'
            if (isinstance(iarg, str)):
                for (rx, bl) in regex.items():
                    if (re.match(rx, iarg) is not None):
                        (idex, opened) = (args.index(iarg), regex[rx])
                        mode = args.pop(idex)
                        return idex, mode, opened
            elif (isinstance(iarg, list)):
                (idex, mode, opened) = iterreg(*iarg)
                return idex, mode, opened
        if (len(kwargs) > 0):
            (idex, mode, opened) = iterreg(*kwargs.values())
            keyidx = kwargs.getvalues().index(mode)
            idex = (-2, kwargs.getkeys()[keyidx])
            return idex, mode, opened
        return idex, mode, opened

    (
        idx,
        nargs,
        opened
    ) = (
        iterreg(*args, **kwargs)
    )
    if (isinstance(idx, tuple)):
        (index, key) = idx
        if AND(
                (isinstance(key, str)),
                (index == -2)
        ):
             return index, kwargs[key], opened
    return idx, nargs, opened

class Hash(object):
    def __init__(
                    self,
                    hash='sha224',
                    digest='hextdigest'
    ):
        self.hash = hash
        self.digest = digest

    def __call__(self, value):
        hashdict = Storage(
                            {
                             'md5': hashlib.md5,
                             'sha1': hashlib.sha1,
                             'sha224': hashlib.sha224,
                             'sha256': hashlib.sha256,
                              'sha384': hashlib.sha384,
                             'sha512': hashlib.sha512
                             }
                        )

        if (hash in hashdict.keys()):
            objHash = hashdict[hash]()
            objHash.update(value)
            return objHash.hexdigest() \
                if (self.digest == 'hexdigest') \
                else objHash.digest() \
                if (self.digest == 'digest') \
                else None