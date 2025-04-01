import unittest

from pprint import pprint

from os.path import dirname
import schemaxml

from libpy4.py4IO import Py4
from libsql.sqlSchema import SchemaXML, to_releasename


class TestPy4(unittest.TestCase):
    schemadir = dirname(schemaxml.__file__)  # where the schemaxml files live
    default_schema_version = 'r15.2'  # if none is provided, use this default p4 release
    schemaversion = to_releasename(default_schema_version)  # format the given p4 release version
    oSchema = SchemaXML(schemaversion, schemadir)  # a reference to class SchemaXML
    p4args = {  # the p4 user
        'user': 'zerdlg',
        'port': 'anastasia.local:1777',
        'client': 'computer_p4dlg',
        'oSchema': oSchema
    }
    oP4 = Py4(**p4args)  # the p4 connector

    '''--------------------------------------------------------------------------------------------------------------'''
    ''' test SQL functionality in both abstractions (p4 & jnl)                                                       '''
    '''                                                                                                              '''
    ''' test functions should run twice (once for p4 & again for jnl)                                                '''
    '''--------------------------------------------------------------------------------------------------------------'''

    def testSimpleQuery(self):
        oP4 = self.oP4
        ''' simple queries - returns RecordSets
        '''
        p4qry = (oP4.clients.client.contains('fred'))  # query p4
        p4_recordset = oP4(p4qry)  # pass it to the connector, retrieve a valid recordset
        p4_assertion = (type(p4_recordset).__name__ == 'RecordSet')
        self.assertTrue(p4_assertion)

    def testSelect(self):
        oP4 = self.oP4
        ''' simple select statement
        '''
        p4qry = (oP4.clients.client.contains('fred'))  # create p4 query
        p4_records = oP4(p4qry).select()  # select records

        print('SELECT - P4')
        pprint(p4_records(0))

        assertion = (len(p4_records) > 0)
        self.assertTrue(assertion)

    def testBelongsNonNested(self):
        oP4 = self.oP4
        ''' test non-nested belongs function (SQL IN)
        '''
        p4_clientnames = ('computer_dev', 'computer_git', 'computer_p4dlg')
        p4_clientrecords = oP4(self.oP4.clients.client.belongs(p4_clientnames)).select()
        print('BELONGS - p4')
        pprint(p4_clientrecords(0))
        p4_assertion = (type(p4_clientrecords).__name__ == 'Records')
        self.assertTrue(p4_assertion)

    def testBelongsNested(self):
        oP4 = self.oP4
        ''' test nested belongs function (SQL IN)
        '''
        p4_qry1 = (oP4.files.action.contains('delete'))
        p4_deletedfiles = oP4(p4_qry1)._select(oP4.files.depotFile)
        p4_qry2 = (oP4.files.depotFile.belongs(p4_deletedfiles))
        p4_filerecords = oP4(p4_qry2).select()
        print('BELONGSNESTED - P4')
        pprint(p4_filerecords(0))
        p4_assertion = (type(p4_filerecords).__name__ == 'Records')
        self.assertTrue(p4_assertion)

    def testInnerJoin(self):
        oP4 = self.oP4
        ''' test inner joins 
        '''
        p4_reference = (oP4.files.change == oP4.changes.change)
        p4_recs = oP4(p4_reference).select()  # easy peasy join
        print('INNERJOIN - P4')
        pprint(p4_recs(0))
        p4_recs_alt = oP4(oP4.files).select(join=oP4.changes.on(p4_reference))  # equivalent to above
        p4_assertion = (len(p4_recs(0).getkeys()) == 2)
        self.assertTrue(p4_assertion)
        self.assertDictEqual(p4_recs(0), p4_recs_alt(0))

    def testOuterJoin(self):
        oP4 = self.oP4
        ''' test inner joins 
        '''
        p4_reference = (oP4.files.change == oP4.changes.change)
        p4_recs = oP4(p4_reference).select()  # easy peasy join
        print('LEFTOUTER - P4')
        pprint(p4_recs(0))
        p4_recs_alt = oP4(oP4.files).select(left=oP4.changes.on(p4_reference))  # equivalent to above
        p4_assertion = (len(p4_recs(0).getkeys()) == 2)
        self.assertTrue(p4_assertion)
        self.assertDictEqual(p4_recs(0), p4_recs_alt(0))

    def testTableRefs(self):
        oP4 = self.oP4
        ''' Py4 command refs

            accessessing a connector's table attribute is all that is needed
            to force / trigger the Py4 object to define the table (aka the 
            command), attributes as well as it's cmd option references.
        '''
        #oP4.reconcile                                                               # access the `reconcile` table
        py4_tabledata = oP4.memoizetable('reconcile')                               # retrieve the table's data
        py4_assertion = (len(py4_tabledata.tableoptions.optionsmap) > 0)
        self.assertTrue(py4_assertion)                                              # once accessed, tabledata should
                                                                                    # contain a valid optionsmap

    def testCount(self):
        oP4 = self.oP4
        ''' test inner joins 
        '''
        qry = (oP4.changes)
        ''' count all results, return int
        '''
        count1 = oP4(qry).count()
        ''' same results but distinct on field `owner'
        '''
        count2 = oP4(qry).count(distinct='user')
        print('COUNT - P4')
        pprint(f"count1: {count1}")
        pprint(f"count2: {count2}")
        self.assertIsInstance(count1, int)
        self.assertIsInstance(count2, int)

    # def testRecordsSearch(self):
    #    pass





if (__name__ == '__main__'):
    (
        loader,
        suite
    ) = \
        (
            unittest.TestLoader(),
            unittest.TestSuite()
        )
    for item in (
            loader.loadTestsFromTestCase(TestPy4),
    ):
        suite.addTests(item)