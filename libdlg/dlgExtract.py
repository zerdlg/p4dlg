import os
import re
import tarfile
from libdlg.dlgFileIO import (
    fileopen,
    bail,
    Lst
)
from libdlg.dlgUtilities import IsMatch

'''  [$File: //dev/p4dlg/libdlg/dlgExtract.py $] [$Change: 411 $] [$Revision: #3 $]
     [$DateTime: 2024/06/25 07:02:28 $]
     [$Author: mart $]
'''

''' TODO: Needs lots of work!
'''

__all__ = [
            'tar',
            'untar',
            'tar_root',
            'Extractor',
            'Compressor',
            'uncompress_extract'
           ]

'''  compress/decompress & tar/extract tarfile       
'''

def fopen(filename, mode='rb', lock=False):
    try:
        return fileopen(filename, mode=mode, lock=lock)
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
    oGzip = fopen(filename, mode='wb', lock=False)
    return oGzip

def untar(filename, path):
    return Extractor()(filename, path)
def tar_root(fpath):
    return Extractor().tar_root(fpath)

class Extractor(object):
    def __init__(self, *args):
        pass

    def __call__(self, filename=None, path='.', members=None):
        (self.filename, self.path, self.members) = (filename, path, members)
        return self

    def get_dirnames(self, _file=None):
        tfile = _file or self.filename
        if (os.path.isfile(tfile)):
            archive = tarfile.open(tfile, mode='r')
            try:
                return archive.getnames()
            except Exception as err:
                bail(err)
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
        except Exception as err:
            bail(err)
        finally:
            tar.close()

    def extractfile(self, member, filename=None):
        tfile = filename or self.filename
        tar = tarfile.TarFile(tfile, 'r')
        try:
            return tar.extractfile(member)
        except Exception as err:
            bail(err)
        finally:
            tar.close()

    def extractall(self, filename=None):
        tfile = filename or self.filename
        tar = tarfile.TarFile(tfile, 'r')
        try:
            return tar.extractall(self.path, self.members)
        except Exception as err:
            bail(err)
        finally:
            tar.close()

        #oFile = lopen(tfile)
        #try:
        #    oFile.extractall(self.path, self.members)
        #except Exception as err:
        #    bail(err)
        #finally:
        #    oFile.close()

class Compressor(object):
    def __init__(self, *args):
        pass

    def __call__(self, fname):
        self.inFile = fname
        self.inFile_noext = self.get_noext_name(self.inFile)
        return self

    def get_noext_name(self, name=None):
        if (name is not None):
            try:
                archbits = Lst(os.path.split(name))
                archpath = archbits(0)
                archname = Lst(os.path.splitext(archbits(1)))(0)
                inFile_noext = os.path.join(*[archpath, archname])
                return inFile_noext
            except Exception as err:
                bail(err)

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
        finally:
            [obj.close() for obj in (objInFile, objOutFile)]

def uncompress_extract(
        arch,
        destpath='.',
        clean=True,
        lock=False
):
    def to_decomped_name(afile):
        return re.sub('.tgz', '.tar', afile) if (afile.endswith('.tgz')) \
            else afile.rstrip('.gz') if ((afile.endswith('.gz'))) \
            else afile

    def gzip_is_empty(afile):
        oFile = open(afile, 'rb')
        try:
            content = oFile.read(1)
            return False \
                if (len(content) > 0) \
                else False
        except Exception as err:
            return False
        finally:
            oFile.close()

    def extractObj(obj, tname, extpath):
        try:
            obj(tname, path=extpath).extractall()
        except Exception as err:
            bail(err)

    def decompressObj(obj, archive, tname=None):
        try:
            obj(archive).decompress(tname)
        except Exception as err:
            bail(err)

    def cleanfile(fname):
        if (clean is True):
            try:
                os.remove(fname)
                print(f"removed file'{fname}'")
            except OSError as err:
                print(err)


    if ((os.path.exists(arch) is False) or (gzip_is_empty(arch) is True)):
        print(f'no such compressed file or has 0 length: {arch}')
    else:
        extract_path = os.path.abspath(os.path.join(*[destpath, os.path.dirname(arch),]))

        (oCompress, oExtract) = (Compressor(), Extractor())

        '''  things should work like this:
    
                if atype is .gz     -->  type is (1) Compressor & (2) None        -> gzfile     (./file.gz)
                if atype is .tar    -->  type is (1) Extractor  & (2) None        -> tarfile    (./file.tar)
                if atype is .tar.gz -->  type is (1) Compressor & (2) Extractor   -> targzfile  (./file.targz)
                if atype is .tgz    -->  type is (1) Extractor  & (2) Extractor   -> gziptar    (./file.tgz)
        '''

        a = re.match(r'^.*[^\.tar]\.gz$', arch)
        b = re.match(r'^.*\.tar$|.*.tgz$', arch)
        c = re.match(r'^.*\.tar.gz$|', arch)
        archivetype = {Lst(IsMatch(r'^.*[^\.tar]\.gz$')(arch))(1): Lst([oCompress, ]),
                       Lst(IsMatch(r'^.*\.tar$|.*.tgz$')(arch))(1): Lst([oExtract, ]),
                       Lst(IsMatch(r'^.*\.tar.gz$|')(arch))(1): Lst([oCompress, oExtract, ])}[True]

        rname = to_decomped_name(arch)
        '''  decompress or extract, but not both!            -> archive is filename.tgz or filename.gz             
        '''
        if (type(archivetype(0)).__name__ == 'Compressor'):
            decompressObj(archivetype(0), arch, rname)
            cleanfile(arch)
        elif (type(archivetype(0)).__name__ == 'Extractor'):
            extractObj(archivetype(0), rname, extract_path)
            cleanfile(rname)

        '''  archive is decompressed and dropped a tarball!  -> archive is filename.tar.gz or filename.targz       
        '''
        if (type(archivetype(1)).__name__ == 'Extractor'):
            extractObj(archivetype(1), rname, extract_path)
            cleanfile(rname)

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
    #uncompress_extract('./archive_samples/sample_depot.tar.gz', clean=True, lock=False)
    uncompress_extract(f, clean=True, lock=False)

    '''  uncompress a .zip file
    '''
    compressed_file = './archive_samples/gareth.zip'
    oCompress = Compressor()(compressed_file)

if __name__ == '__main__':
    test()


