import os
import re
from copy import copy

from libdlg.dlgRecordset import DLGRecordSet
from libdlg.dlgStore import Storage, Lst
from libdlg.dlgFileIO import loadpickle, fileopen

from libjnl.jnlFile import *
#from libjnl.jnlInit import JnlInitialize
from libdlg.dlgSchema import SchemaXML
import schemaxml
from os.path import dirname
schemadir = dirname(schemaxml.__file__)
objectify = Storage.objectify

__all__ = ['GuessRelease']

'''  [$File: //dev/p4dlg/libjnl/jnlGuess.py $] [$Change: 461 $] [$Revision: #11 $]
     [$DateTime: 2024/08/09 18:15:55 $]
     [$Author: mart $]
'''

# this is now broken - TODO: investigate and fix!

class GuessRelease(object):
    def __init__(self, schemaxmldocs=schemadir):
        schemaxmldocs = os.path.abspath(schemaxmldocs)
        self.schemaxmldocs = schemaxmldocs
        self.oSchema = SchemaXML(schemadir=schemaxmldocs)
        self.schemafiles = self.oSchema.listxmlfiles_local()
        self.schemaversions = Lst([self.oSchema.getversion_from_filename(f)\
                                   for f in self.schemafiles])
        self.xmlPath = self.oSchema.localpath
        self.picklespath = os.path.join(self.xmlPath, 'pickles')
        self.fieldsdatapath = os.path.join(self.picklespath, 'fieldsdata')
        self.fieldsdatafile = os.path.join(self.fieldsdatapath, 'fieldsdata')
        self.oJnlReader = JNLFile()

    def __call__(self, *args, **kwargs):
        return self

    def guess(self, journal):
        try:
            datafile = self.fieldsdatafile
            datarecords = objectify(loadpickle(datafile))
        except Exception as err:
            '''  doh! no file to read, make the data records!

                    [___ GUESS JOURNAL VERSION ___]
                    +-----+---------+--------+--------------+-----------+
                    | id  | release | table  | tableVersion | numFields |
                    +-----+---------+--------+--------------+-----------+
                    | 496 | r13.3   | domain | 6            | 18        |
                    | 500 | r16.2   | domain | 6            | 18        |
                    | 501 | r18.2   | domain | 7            | 18        |
                    +-----+---------+--------+--------------+-----------+
            '''
            oInit = JnlInitialize(schemaxmldocs=self.schemaxmldocs)
            oInit.__call__(overwrite_objects=False)
            datarecords = objectify(oInit.records)

        results = Storage()
        '''  a journal reader

                +-------+------------------+---------------+
                | Field | function         | value example |
                +-------+------------------+---------------+
                | 0     | record ID        | 1             |
                +-------+------------------+---------------+
                | 1     | operation handle | @pv@          |
                +-------+------------------+---------------+
                | 2     | table version    | 6             |
                +-------+------------------+---------------+
                | 3     | table name       | db.domain     |
                +-------+------------------+---------------+
        '''
        with fileopen(journal, 'rb') as oFile:
            journalrecords = self.oJnlReader.csvread(oFile)
            (seentables, EOR, counter) = (Lst(), False, 0)
            while (EOR is False):
                try:
                    journalrecord = next(journalrecords)
                    counter += 1
                    table = journalrecord[2]
                    if ((not table in seentables) & (table.startswith('db.'))):
                        seentables.append(table)
                        table = re.sub('db\.', '', table)
                        journaltableversion = journalrecord[1]
                        fields = journalrecord[3:]
                        '''  Query each schemaxml files, collect only those that are a good fit
                                with the passed in journal/checkpoint

                                db.change (common record fields)
                                +----+--------+---------+-------------+
                                | id | handle | version | table       |
                                +----+--------+---------+-------------+
                                | 1  | @pv@   | 2       | @db.change@ |
                                +----+--------+---------+-------------+
                                | 0  | 1      | 2       | 3           |
                                +----+--------+---------+-------------+

                                fields unique to db.change
                                +---+---+-------+-------+-----------+---+-------+---------+
                                | 1 | 1 | @bla@ | @bob@ | 894209293 | 1 | @bla@ | @//...@ |
                                +---+---+-------+-------+-----------+---+-------+---------+
                                | 4 | 5 | 6     | 7     | 8         | 9 | 10    | 11      |
                                +---+---+-------+-------+-----------+---+-------+---------+

                                So this journal is compatable with any schema version
                                in the 'versions' set. Why? because for now all we need
                                is a p4model to help us navigate those fields. With a
                                compatable model we would be blind. Let's just grab the
                                latest in the list
                        '''

                        query1 = (lambda rec: rec.table == table)
                        query2 = (lambda rec: rec.numFields == (len(fields) + 4))
                        query3 = (lambda rec: rec.tableVersion == journaltableversion)
                        drecords = DLGRecordSet(datarecords)(*[query1, query2, query3]).select(orderby=['table', 'release'])
                        results.merge({table: [record.release for record in drecords]})
                except StopIteration:
                    EOR = True

        versions = objectify(copy(self.schemaversions))
        for result in results:
            resl = Lst(results[result])
            if (len(resl) > 0):
                versions = versions.intersect(resl)
        return versions

def main():
    schemaxmldocs = os.path.abspath('../../schemaxml')
    journal = os.path.abspath('../../resc/journals/checkpoint')
    compatible_releases = GuessRelease(schemaxmldocs).guess(journal)
    likely_release = max(compatible_releases)
    print(f"Compatible releases: {compatible_releases}")
    print(f'likely releases: {likely_release}')
    ''' after all said and done, consider running >>> ./p4d -r {P4DROOT} -J journal -xu
    '''

# if __name__=='__main__':main()
