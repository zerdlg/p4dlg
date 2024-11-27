import unittest
from os.path import dirname
import schemaxml
from resc import journals

from libdlg import AND, Storage, Lst, DLGDateTime
from libjnl.jnlIO import P4Jnl
from libpy4.py4IO import Py4
from libdlg.dlgSchema import SchemaXML, to_releasename

class TestSQL(unittest.TestCase):
    ''' setup / teardown
    '''
    def setUp(self):
        schemadir = dirname(schemaxml.__file__)                 # where the schemaxml files live
        journaldir = dirname(journals.__file__)                 # default journal location
        journalfile = 'journal.8'                               # the journalfile name
        default_schema_version = 'r16.2'                        # if none is provided, use this default p4 release
        journal = f'{journaldir}/{journalfile}'                 # load this journal
        schemaversion = to_releasename(default_schema_version)  # format the given p4 release version
        oSchema = SchemaXML(schemadir, schemaversion)           # a reference to class SchemaXML
        p4args = {                                              # the p4 user
            'user': 'mart',
            'port': 'anastasia.local:1777',
            'client': 'computer_p4q',
            'oSchema': oSchema
        }
        self.oP4 = Py4(**p4args)                                # the p4 connector
        self.oJnl = P4Jnl(journal, oSchema)                     # the jnl connector

    def tearDown(self):
        del self.oP4; del self.oJnl

    '''--------------------------------------------------------------------------------------------------------------'''
    ''' test SQL functionality in both abstractions (p4 & jnl)                                                       '''
    '''                                                                                                              '''
    ''' test functions should run twice (once for p4 & again for jnl)                                                '''
    '''--------------------------------------------------------------------------------------------------------------'''

    def testSimpleQuery(self):
        ''' simple queries - returns DLGRecordSets
        '''
        p4qry = (self.oP4.clients.client.contains('fred'))      # query p4
        p4_recordset = self.oP4(p4qry)                          # pass it to the connector, retrieve a valid recordset

        jnlqry = (self.oJnl.domain.type == 99)                  # query jnl
        jnl_recordset = self.oJnl(jnlqry)                       # pass it to the connector, retrieve a valid recordset

        p4_assertion = (type(p4_recordset).__name__ == 'DLGRecordSet')
        jnl_assertion = (type(jnl_recordset).__name__ == 'DLGRecordSet')
        self.assertTrue(p4_assertion)
        self.assertTrue(jnl_assertion)


    def testSelect(self):
        ''' simple select statement
        '''
        p4qry = (self.oP4.clients.client.contains('fred'))      # create p4 query
        p4_records = self.oP4(p4qry).select()                   # select records

        jnlqry = (self.oJnl.domain.type == 99)                  # create a jnl query
        jnl_records = self.oJnl(jnlqry).select()                # select records

        assertion = AND((len(p4_records) > 0), (len(jnl_records) > 0))
        self.assertTrue(assertion)

    def testBelongsNoneNested(self):
        ''' test non-nested belongs function (SQL IN)
        '''
        p4_clientnames = ('computer_dev', 'computer_git', 'computer_p4dlg')
        p4_clientrecords = self.oP4(self.oP4.clients.client.belongs(p4_clientnames)).select()
        p4_assertion = (type(p4_clientrecords).__name__ == 'DLGRecords')

        jnl_clientnames = ('myclient', 'pycharmclient', 'otherclient')
        jnl_clientrecords = self.oJnl(self.oJnl.domain.name.belongs(jnl_clientnames)).select()
        jnl_assertion = (type(jnl_clientrecords).__name__ == 'DLGRecords')

        self.assertTrue(p4_assertion)
        self.assertTrue(jnl_assertion)

    def testBelongsNested(self):
        ''' test nested belongs function (SQL IN)
        '''
        p4_qry1 = (self.oP4.files.action.contains('delete'))
        p4_deletedfiles = self.oP4(p4_qry1)._select(self.oP4.files.depotFile)
        p4_qry2 = (self.oP4.files.depotFile.belongs(p4_deletedfiles))
        p4_filerecords = self.oP4(p4_qry2).select()
        p4_assertion = (type(p4_filerecords).__name__ == 'DLGRecords')

        jnl_qry1 = AND(
            (self.oJnl.domain.type == '99'),
            (self.oJnl.domain.owner == 'mart')
        )
        jnl_myclients = self.oJnl(jnl_qry1)._select(self.oJnl.domain.name)
        jnl_qry2 = (self.oJnl.domain.name.belongs(jnl_myclients))
        jnl_clientrecords = self.oJnl(jnl_qry2).select()
        jnl_assertion = (type(jnl_clientrecords).__name__ == 'DLGRecords')

        self.assertTrue(p4_assertion)
        self.assertTrue(jnl_assertion)

    '''--------------------------------------------------------------------------------------------------------------'''
    ''' test SQL functionality in p4 abstraction only                                                                '''
    '''--------------------------------------------------------------------------------------------------------------'''

    '''--------------------------------------------------------------------------------------------------------------'''
    ''' test SQL functionality in jnl abstraction only                                                               '''
    '''--------------------------------------------------------------------------------------------------------------'''


'''------------------------------------------------------------------------------------------------------------------'''
''' test DLG classes/modules directly                                                                                '''
'''------------------------------------------------------------------------------------------------------------------'''
class TestDLG(unittest.TestCase):

    def testDateTime(self):
        ''' /libdlg/dlgDateTime/DLGDateTime
        '''
        oDateTime = DLGDateTime()                           # DLGDateTime instance

        ''' str to datetime / guess
        '''
        oDateTime.guess('2019/07/26')                       # guess date
        oDateTime.guess('16:30.45')                         # guess time
        oDateTime.guess('2019/07/26 16:30:45')              # guess datetime

        ''' string_to_datetime from ...
        '''
        oDateTime.string_to_datetime('2019/08/29')          # given a date with / separator
        oDateTime.string_to_datetime('2019 08 29')          # given a date with ' ' separator
        oDateTime.string_to_datetime('2019-08-29')          # given a date with / separator
        oDateTime.string_to_datetime('2019:08:29')          # given a date with : separator

        ''' to_datetime from ...
        '''
        oDateTime.to_datetime("1547856000.0")
        oDateTime.to_datetime(1547856000.0)
        oDateTime.to_datetime(2019, 8, 19)
        oDateTime.to_datetime(*[2019, 8, 19])
        oDateTime.to_datetime("2019/8/19")
        oDateTime.to_datetime("2019-8-19")
        oDateTime.to_datetime(*[2019, 6, 9])

        ''' epoch
        '''
        oDateTime.to_epoch('2019/09/03')                    # to epoch from str
        d = oDateTime.guess('2019/09/03')                   # create date
        oDateTime.to_epoch(d)                               # to epoch from date
        oDateTime.to_epoch(2019, 9, 3)                      # args[0], args[1], args[2]
        oDateTime.to_epoch([2019, 9, 3])                    # [args[0], args[1], args[2]]
        oDateTime.to_epoch(*[2019, 9, 3])                   # *[args[0], args[1], args[2]]

        ''' internally, perforce uses unix time
            externally, to the user, it uses this 
            format '2019/8/1' (the 'p4date')
        '''
        oDateTime.to_p4date(2019, 8, 19)
        oDateTime.to_p4date('2019', '8', '19')
        oDateTime.to_p4date('2019,8,19')
        oDateTime.to_p4date('2019-8-19')
        oDateTime.to_p4date('2019 8 19')
        oDateTime.to_p4date('2019:8:19')
        oDateTime.to_p4date('2019:8,19')
        oDateTime.to_p4date(2019, 8, 19)
        oDateTime.to_p4date(*[2019, 8, 19])
        oDateTime.to_p4date('1547856000.0')
        oDateTime.to_p4date(1547856000.0)

if (__name__ == '__main__'):
    (
    loader,
    suite
    ) = \
    (
    unittest.TestLoader(),
    unittest.TestSuite()
    )
    suite.addTests(loader.loadTestsFromTestCase(TestSQL))
    suite.addTests(loader.loadTestsFromTestCase(TestDLG))