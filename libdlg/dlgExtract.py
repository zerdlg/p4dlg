import os
import re
import tarfile
from libfs.fsFileIO import (
    fileopen,
    bail,
    Lst,
    is_compressed,
)

'''  [$File: //dev/p4dlg/libdlg/dlgExtract.py $] [$Change: 609 $] [$Revision: #7 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
'''

''' TODO: Needs LOTS of work!
'''

__all__ = [
    'tar',
    'untar',
    'tar_root',
    'DLGExtract',
    'DLGCompress',
    'extract_compressed_archive'
]

'''  compress/decompress & tar/extract tarfile       
'''

def fopen(
        filename,
        mode='rb',
        lock=False
):
    try:
        return fileopen(
            filename,
            mode=mode,
            lock=lock
        )
    except Exception as err:
        bail(err)

def tar(file, dir, expression='^.+$'):
    tar = tarfile.TarFile(file, 'w')
    try:
        [tar.add(os.path.join(dir, file), file, False) for file \
         in os.listdir(dir, expression, add_dirs=True)]
    except Exception as err:
        bail(err)
    finally:
        tar.close()

def unzip(filename):
    oGzip = fopen(
        filename,
        mode='wb',
        lock=False
    )
    return oGzip

def untar(filename, path):
    return DLGExtract()(filename, path)

def tar_root(fpath):
    return DLGExtract().tar_root(fpath)

class DLGExtract(object):
    def __init__(self, *args):
        (
            self.filename,
            self.path,
            self.members
        ) = \
            (
                None,
                None,
                None
            )

    def __call__(self, filename=None, path='.', members=None):
        (
            self.filename,
            self.path,
            self.members
        ) = \
            (
                filename,
                path,
                members
            )
        return self

    def get_dirnames(self, filename=None):
        tfile = filename or self.filename
        if (os.path.isfile(tfile)):
            archive = tarfile.open(tfile, mode='r')
            try:
                return archive.getnames()
            finally:
                archive.close()
        return []

    def tar_root(self, filename=None):
        tfile = filename or self.filename
        dirnames = self.get_dirnames(tfile)
        if (len(dirnames) > 0):
            return os.path.commonprefix(dirnames)

    def extract(self, member, filename=None):
        tfile = filename or self.filename
        tar = tarfile.TarFile(tfile, 'r')
        try:
            return tar.extract(member, self.path)
        finally:
            tar.close()

    def extractfile(self, member, filename=None):
        tfile = filename or self.filename
        tar = tarfile.TarFile(tfile, 'r')
        try:
            return tar.extractfile(member)
        finally:
            tar.close()

    def extractall(self, filename=None):
        tfile = filename or self.filename
        tar = tarfile.TarFile(tfile, 'r')
        try:
            return tar.extractall(self.path, self.members)
        finally:
            tar.close()

class DLGCompress(object):
    def __init__(self, *args):
        (
            self.infile,
            self.infile_noext
        ) = \
            (
                None,
                None
            )

    def __call__(self, fname):
        self.inFile = fname
        self.inFile_noext = self.get_noext_name(self.inFile)
        return self

    def get_noext_name(self, name=None):
        try:
            archbits = Lst(os.path.split(name))
            archpath = archbits(0)
            archname = Lst(os.path.splitext(archbits(1)))(0)
            inFile_noext = os.path.join(*[archpath, archname])
            return inFile_noext
        except:
            return name

    def compressfile(self, outfile=None):
        compress_outfile = outfile or self.inFile_noext
        objInFile = fopen(self.infile, mode='rb')
        objOutFile = fopen(compress_outfile, 'wb')
        try:
            inFileContent = objInFile.read()
            objOutFile.write(inFileContent)
        finally:
            [obj.close() for obj in (objInFile, objOutFile)]

    def decompress(self, outfile):
        objInFile = fopen(self.inFile, mode='rb')
        objOutFile = fopen(outfile, mode='wb')
        try:
            InFileContent = objInFile.read()
            objOutFile.write(InFileContent)
        except Exception as err:
            bail(err)
        finally:
            [obj.close() for obj in (objInFile, objOutFile)]

def extract_compressed_archive(
        arch,
        destdir='.',
        outfile=None,
        clean=True,
        lock=False
):
    def to_decomped_name(afile):
        return re.sub('.tgz', '.tar', afile) \
            if (afile.endswith('.tgz')) \
            else afile.rstrip('.gz') \
            if ((afile.endswith('.gz'))) \
            else afile

    def gzip_is_empty(afile):
        oFile = open(afile, 'rb')
        try:
            content = oFile.read(1)
            return False \
                if (len(content) > 0) \
                else False
        finally:
            oFile.close()

    def extract(tname, destpath):
        dest = destpath or destdir
        oExtract(tname, path=dest).extractall()

    def decompress(archive, out=None):
        filename = out or outfile
        if (filename is None):
            filename = 'archive'
        oCompress(archive).decompress(filename)

    def cleanfile(fname):
        if (clean is True):
            try:
                os.remove(fname)
                print(f"removed file'{fname}'")
            except OSError as err:
                print(err)

    def get_versionfilename(arch):
        parentdir = os.path.dirname(arch)
        outfile = re.sub(',d', '', parentdir) \
            if (re.match(r'^.*,d$', parentdir) is not None) \
            else None
        return outfile

    if (os.path.exists(arch) is False) or (gzip_is_empty(arch) is True):
        print(f'no such compressed file or has 0 length: {arch}')
    else:
        extract_path = os.path.abspath(
            os.path.join(
                *[
                    destdir,
                    os.path.dirname(arch),
                ]
            )
        )

        (
            oCompress,
            oExtract
        ) = \
            (
                DLGCompress(),
                DLGExtract()
            )

        ''' the thing should work like this:
    
            if extension is .gz     -->  DLGCompress                  -> gzfile     (./filename.gz)
            if extension is .tar    -->  DLGExtract                   -> tarfile    (./filename.tar)
            if extension is .tar.gz -->  DLGCompress / DLGExtract     -> targzfile  (./filename.tar.gz)
            if extension is .tgz    -->  DLGExtract  / DLGExtract     -> gziptar    (./filename.tgz)
        '''
        rname = to_decomped_name(arch)
        destname = get_versionfilename(arch) or rname

        if (re.search(r'\.tar$', arch) is not None):
            ''' extract '''
            decompress(arch, destname)
            cleanfile(arch)

        elif (re.search(r'\.tar\.gz$', arch) is not None):
            ''' decompress / extract'''
            decompress(arch, destname)
            destname = to_decomped_name(destname)
            extract(destname, extract_path)
            cleanfile(arch)

        elif (re.search(r'\.tgz$', arch) is not None):
            ''' extract / extract '''
            extract(destname, extract_path)

        elif (re.search(r'\.gz$', arch) is not None):
            ''' decomp '''
            decompress(arch, destname)
        else:
            if (is_compressed(arch) is True):
                if (gzip_is_empty(arch) is False):
                    decompress(arch)

        """
        '''  decompress or extract, but not both!            -> archive is filename.tgz or filename.gz             
        '''
        if (type(archivetype(0)).__name__ == 'DLGCompress'):
            decompressObj(archivetype(0), arch, rname)
            cleanfile(arch)
        elif (type(archivetype(0)).__name__ == 'DLGExtract'):
            extractObj(archivetype(0), rname, extract_path)
            cleanfile(rname)

        '''  archive is decompressed and dropped a tarball!  -> archive is filename.tar.gz or filename.targz       
        '''
        if (type(archivetype(1)).__name__ == 'DLGExtract'):
            extractObj(archivetype(1), rname, extract_path)
            cleanfile(rname)
        """
def test():
    '''  set thing up for testing
    '''
    sRoot = os.path.abspath('./archive_samples')
    sFilename = os.path.join(*[sRoot, 'COPY_sample_depot.tar.gz'])
    dFilename = os.path.join(*[sRoot, 'sample_depot.tar.gz'])
    oFile_src = open(sFilename, 'rb')
    oFile_dst = open(dFilename, 'wb')
    try:
        oFile_dst.write(oFile_src.read())
    finally:
        oFile_src.close();
        oFile_dst.close()

    '''  uncompress and extract a tar.gz file
    '''
    f = '/Users/gc/depot/AudioFormatOgg.h.gz'
    #decompress_extract('./archive_samples/sample_depot.tar.gz', clean=True, lock=False)
    extract_compressed_archive(f, clean=True, lock=False)

    '''  uncompress a .zip file
    '''
    #compressed_file = './archive_samples/gareth.zip'
    #oCompress = DLGCompress()(compressed_file)

if __name__ == '__main__':
    test()


