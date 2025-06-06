import unittest

from pprint import pprint

from os.path import dirname
import schemaxml
from resc import journals

from libsql.sqlQuery import AND
from libjnl.jnlIO import P4Jnl
from libsql.sqlSchema import SchemaXML, to_releasename


class TestJNL(unittest.TestCase):
    schemadir = dirname(schemaxml.__file__)                     # where the schemaxml files live
    journaldir = dirname(journals.__file__)                     # default journal location
    journalfile = 'journal.8'                                   # the journalfile name
    default_schema_version = 'r16.2'                            # if none is provided, use this default p4 release
    journal = f'{journaldir}/{journalfile}'                     # load this journal
    schemaversion = to_releasename(default_schema_version)      # format the given p4 release version
    oSchema = SchemaXML(schemaversion, schemadir)               # a reference to class SchemaXML
    oJnl = P4Jnl(journal, oSchema)                              # the jnl connector

    '''--------------------------------------------------------------------------------------------------------------'''
    ''' test SQL functionality in both abstractions (p4 & jnl)                                                       '''
    '''                                                                                                              '''
    ''' test functions should run twice (once for p4 & again for jnl)                                                '''
    '''--------------------------------------------------------------------------------------------------------------'''

    def testDatatypeConverters(self):
        oJnl = self.oJnl
        oSchemaType = oJnl.oSchemaType
        count = 0
        for (key, value) in {
            oSchemaType.convert_flagvalue_to_flagname(oJnl.domain.type.type, '99'): 'client',
            oSchemaType.convert_flagname_to_flagvalue(oJnl.domain.type.type, 'client'): '99',
            oSchemaType.convert_maskvalue_to_maskname(oJnl.protect.perm.type, '0x0040'): 'Super',
            oSchemaType.convert_maskname_to_maskvalue(oJnl.protect.perm.type, 'Super'): '0x0040'
        }.items():
            count += (key == value)

        self.assertTrue(count == 4)

    def testSimpleQuery(self):
        oJnl = self.oJnl
        ''' simple queries - returns RecordSets
        '''
        jnlqry = (oJnl.domain.type == 99)                       # query jnl
        jnl_recordset = oJnl(jnlqry)                            # pass it to the connector, retrieve a valid recordset
        jnl_assertion = (type(jnl_recordset).__name__ == 'RecordSet')
        self.assertTrue(jnl_assertion)

    def testSelect(self):
        oJnl = self.oJnl
        ''' simple select statement
        '''
        jnlqry = (oJnl.domain.type == 99)                       # create a jnl query
        jnl_records = oJnl(jnlqry).select()                     # select records
        print('SELECT - JNL')
        pprint(jnl_records(0))
        assertion = (len(jnl_records) > 0)
        self.assertTrue(assertion)

    def testBelongsNonNested(self):
        oJnl = self.oJnl
        ''' test non-nested belongs function (SQL IN)
        '''
        jnl_clientnames = ('myclient', 'pycharmclient', 'otherclient')
        jnl_clientrecords = oJnl(self.oJnl.domain.name.belongs(jnl_clientnames)).select()
        print('BELONGS - JNL')
        pprint(jnl_clientrecords(0))
        jnl_assertion = (type(jnl_clientrecords).__name__ == 'Records')
        self.assertTrue(jnl_assertion)

    def testBelongsNested(self):
        oJnl = self.oJnl
        ''' test nested belongs function (SQL IN)
        '''
        jnl_qry1 = AND(
            (oJnl.domain.type == '99'),
            (oJnl.domain.owner == 'zerdlg')
        )
        jnl_myclients = oJnl(jnl_qry1)._select(oJnl.domain.name)
        jnl_qry2 = (oJnl.domain.name.belongs(jnl_myclients))
        jnl_clientrecords = oJnl(jnl_qry2).select()
        print('BELONGSNESTED - JNL')
        pprint(jnl_clientrecords(0))
        jnl_assertion = (type(jnl_clientrecords).__name__ == 'Records')
        self.assertTrue(jnl_assertion)

    def testInnerJoin(self):
        oJnl = self.oJnl
        ''' test inner joins 
        '''
        jnl_reference = (oJnl.rev.change == oJnl.change.change)
        jnl_recs = oJnl(jnl_reference).select()                                     # easy peasy join
        print('INNERJOIN - JNL')
        pprint(jnl_recs(0))
        jnl_recs_alt = oJnl(oJnl.rev).select(join=oJnl.change.on(jnl_reference))    # equivalent to above
        jnl_assertion = (len(jnl_recs(0).getkeys()) == 2)
        self.assertTrue(jnl_assertion)
        self.assertDictEqual(jnl_recs(0), jnl_recs_alt(0))

    def testOuterJoin(self):
        oJnl = self.oJnl
        ''' test inner joins 
        '''
        jnl_reference = (oJnl.rev.change == oJnl.change.change)
        jnl_recs = oJnl(jnl_reference).select()                                     # easy peasy join
        print('LEFTOUTER - JNL')
        pprint(jnl_recs(0))
        jnl_recs_alt = oJnl(oJnl.rev).select(left=oJnl.change.on(jnl_reference))    # equivalent to above
        jnl_assertion = (len(jnl_recs(0).getkeys()) == 2)
        self.assertTrue(jnl_assertion)
        self.assertDictEqual(jnl_recs(0), jnl_recs_alt(0))

    def testCount(self):
        oJnl = self.oJnl
        qry = (oJnl.domain.type == 99)
        ''' count all results, return int
        '''
        count1 = oJnl(qry).count()
        ''' same results but distinct on field `owner'
        '''
        count2 = oJnl(qry).count(distinct='owner')
        print('COUNT - JNL')
        pprint(f"count1: {count1}")
        pprint(f"count2: {count2}")
        self.assertIsInstance(count1, int)
        self.assertIsInstance(count2, int)

    def testTableRefs(self):
        oJnl = self.oJnl
        ''' P4Jnl command refs

            accessessing a connector's table attribute is all that is needed
            to force / trigger the P4Jnl object to define the table (aka the 
            command), attributes. As opposed to Py4, the references are table
            related only.
        '''
        #oJnl.domain                                                                 # access table `domain`
        jnl_tabledata = oJnl.memoizetable('domain')                                 # retrieve the table's data

        jnl_assertion = (len(jnl_tabledata.getkeys().intersect(                     # once accessed, tabledata should
            ['fieldsmap',                                                           # should contain the listed
             'fieldtypesmap',                                                       # attributes
             'fieldnames',
             '_rname',
             'name',
             'type',
             'version',
             'classic_lockseq',
             'peek_lockseq',
             'keying',
             'desc']
        )
        ) > 0)
        self.assertTrue(jnl_assertion)

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
            loader.loadTestsFromTestCase(TestJNL),
    ):
        suite.addTests(item)