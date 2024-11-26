import unittest
from os.path import dirname
import schemaxml
from resc import journals

from libdlg import AND, Storage, Lst
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
    ''' SQL tests                                                                                                    '''
    '''--------------------------------------------------------------------------------------------------------------'''

    ''' simple queries - returns DLGRecordSets
    '''
    def testSimpleQuery(self):
        p4qry = (self.oP4.clients.client.contains('fred'))
        jnlqry = (self.oJnl.domain.type == 99)
        p4_recordset = self.oP4(p4qry)
        jnl_recordset = self.oJnl(jnlqry)
        p4_assertion = (type(p4_recordset).__name__ == 'DLGRecordSet')
        jnl_assertion = (type(jnl_recordset).__name__ == 'DLGRecordSet')
        self.assertTrue(p4_assertion)
        self.assertTrue(jnl_assertion)

    ''' simple statements
    '''
    def testSelect(self):
        p4qry = (self.oP4.clients.client.contains('fred'))
        jnlqry = (self.oJnl.domain.type == 99)
        p4_records = self.oP4(p4qry).select()
        jnl_records = self.oJnl(jnlqry).select()
        assertion = AND((len(p4_records) > 0), (len(jnl_records) > 0))
        self.assertTrue(assertion)

    def testJNLBelongs(self):
        clientnames = ('myclient', 'pycharmclient', 'otherclient')
        clientrecords = self.oJnl(self.oJnl.domain.name.belongs(clientnames)).select()
        assertion = (type(clientrecords).__name__ == 'DLGRecords')
        self.assertTrue(assertion)

    def testP4Belongs(self):
        clientnames = ('computer_dev', 'computer_git', 'computer_p4dlg')
        clientrecords = self.oP4(self.oP4.clients.client.belongs(clientnames)).select()
        assertion = (type(clientrecords).__name__ == 'DLGRecords')
        self.assertTrue(assertion)

    def testJNLNestedBelongs(self):
        qry1 = AND(
            (self.oJnl.domain.type == '99'),
            (self.oJnl.domain.owner == 'mart')
        )
        myclients = self.oJnl(qry1)._select(self.oJnl.domain.name)
        qry2 = (self.oJnl.domain.name.belongs(myclients))
        clientrecords = self.oJnl(qry2).select()
        assertion = (type(clientrecords).__name__ == 'DLGRecords')
        self.assertTrue(assertion)

    def testP4NestedBelongs(self):
        qry1 = (self.oP4.files.action.contains('delete'))
        deletedfiles = self.oP4(qry1)._select(self.oP4.files.depotFile)
        qry2 = (self.oP4.files.depotFile.belongs(deletedfiles))
        filerecords = self.oP4(qry2).select()
        assertion = (type(filerecords).__name__ == 'DLGRecords')
        self.assertTrue(assertion)

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
    #suite.addTests(loader.loadTestsFromTestCase(TestSimpleJNLQuery))