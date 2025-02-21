import sys
from copy import copy
import os.path
from _csv import writer as csvwriter, reader as csvreader
from _csv import Error, register_dialect, QUOTE_MINIMAL, field_size_limit
from _csv import Dialect as _Dialect
import csv

#import dask.dataframe as df
#import pandas as pd
#import parquet
from libdlg.dlgUtilities import bail

def xrange(x):
    return iter(range(x))

__all__=['JNLFile']

'''  [$File: //dev/p4dlg/libjnl/jnlFile.py $] [$Change: 609 $] [$Revision: #7 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
'''

csv.field_size_limit(sys.maxsize)

''' A journal compatible dialect / reader / writer
'''
class JournalDialect:
    (_name, _valid) = ("", False)
    (
        delimiter,
        quotechar,
        escapechar,
        doublequote,
        skipinitialspace,
        lineterminator ,
        quoting,
        strict
    ) = \
        (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None
    )

    def __init__(self):
        self._valid = True \
            if (self.__class__ != JournalDialect) \
            else False
        self._validate()

    def _validate(self):
        try:
            _Dialect(self)
        except TypeError as err:
            raise Error(str(err))

class journal(JournalDialect):
    (
        delimiter,
        quotechar,
        doublequote,
        skipinitialspace,
        lineterminator,
        quoting,
        strict,
        escapechar
    ) = \
        (
            ' ',
            '@',
            True,
            False,
            '\r\n',
            QUOTE_MINIMAL,
            False,
            '\\'
    )

register_dialect("journal", journal)
class journal_tab(journal):
    delimiter = ' '

register_dialect("journal-tab", journal_tab)
field_size_limit(1000000000)

class JNLFile(object):

    def __iter__(self):
        for i in xrange(len(self)):
            yield self[i]

    def __init__(
            self,
            journalfile,
            outfile=None,
            dialect='journal',
            reader='csv'
    ):
        try:
            self.journalfile = os.path.abspath(journalfile)
            self.jnlFileReader = open(self.journalfile, 'r')
            (self.outfile, self.jnlFileWriter)  = (outfile, None)
            if (self.outfile is not None):
                self.outfile = os.path.abspath(outfile)
                self.jnlFileWriter = open(self.outfile, 'w')
            self.dialect = dialect
            readers = {
                       'csv': self.csvread,
                       #'pandas': self.pdread,
                       #'pasrquet': self.parquetread,
                       # 'dask': self.daskread
            }
            self.reader = readers[reader]
        except Exception as err:
            bail(err, True)

    def copy(self):
        return copy(self)

    def __call__(self): return self

    def oFile_read(self, filename=None):
        return self.jnlFileReader \
            if (filename is None) \
            else open(os.path.abspath(filename), 'r') \
            if (not hasattr(filename, 'read')) \
            else filename

    def oFile_write(self, filename=None):
        return self.jnlFileWriter \
            if (filename is None) \
            else open(filename, 'w') \
            if (not hasattr(filename, 'write')) \
            else filename

    ''' builtin csv reader & writer
    '''
    def csvread(self, filename=None):
        oFile = self.oFile_read(filename)
        return csvreader(oFile, dialect=self.dialect)

    def csvwrite(self, filename=None):
        oFile = self.oFile(filename)
        writer = csvwriter(oFile, dialect=self.dialect)
        return writer

    ''' pandas dataframe reader & writer
    
    def pdread(self, filename=None, **kwargs):
        oFile = self.oFile_read(filename)
        return pd.read_csv(
            oFile,
           dialect=self.dialect,
           names=range(25),
           low_memory=False,
           #header=None,
           on_bad_lines='skip',
           #iterator=True,
           #index_col=False
        )
    
    def pd_to_parquet(self, infile=None, outfile=None):
        infile = infile or self.journalfile
        outfile = self.oFile_write(outfile)
        reader = self.pdread(infile)
        try:
            reader.to_parquet(outfile)
        finally:
            outfile.close()

    def pdwrite(self, filename=None):
        oFile = self.oFile_write(filename)
        writer = pd.read_csv(oFile, dialect=self.dialect)
        # writer.to_csv(rows)
        return writer
    '''

    ''' parquet reader & writer
    
    def parquetread(self, filename=None):
        oFile = self.oFile_read(filename)
        return parquet.reader(oFile)

    def parquetwrite(self, filename=None):
        oFile = self.oFile_write(filename)
        writer = parquet.csv.writer(oFile, dialect=self.dialect)
        # writer.writerow(row) or writer.writerows(rows)
        return writer
    '''

    ''' dask
    def daskread(self, filename=None):
        oFile = self.oFile_read(filename)
        return df.read_csv(oFile, dialect=self.dialect)
    '''

"""
jfile = '../../resc/journals/journal.8'
oJnlReader = JNLFile(jfile)

def opentest():
    oFile = open(jfile, 'r')
def testcsv():
    oJnlReader.csvread()
def testpd():
    oJnlReader.pdread(**{'header':None, 'index_col':False})
def testparquet():
    oJnlReader.parquetread()
def testdask():
    oJnlReader.daskread()


''' standard file descriptor
'''
#print(timeit.timeit('opentest()', globals=globals()))
''' csv reader
'''
#print(timeit.timeit('testcsv()', globals=globals()))
''' parquet reader
'''
#print(timeit.timeit('testparquet()', globals=globals()))
''' dask reader
'''
#print(timeit.timeit('testdask()', globals=globals()))
''' pandas reader
'''
#print(timeit.timeit('testpd()', globals=globals()))

#if (__name__ == '__main__'):
#    main()
"""