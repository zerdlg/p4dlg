import unittest

from pprint import pprint

from os.path import dirname
import schemaxml
from resc import journals

from libdlg import (
    AND,
    Storage,
    Lst,
    DLGDateTime,
)
from libdlg.dlgExtract import DLGExtract
from libjnl.jnlIO import P4Jnl
from libpy4.py4IO import Py4
from libdlg.dlgSchema import SchemaXML, to_releasename

class TestSQL(unittest.TestCase):
    schemadir = dirname(schemaxml.__file__)                     # where the schemaxml files live
    journaldir = dirname(journals.__file__)                     # default journal location
    journalfile = 'journal.8'                                   # the journalfile name
    default_schema_version = 'r16.2'                            # if none is provided, use this default p4 release
    journal = f'{journaldir}/{journalfile}'                     # load this journal
    schemaversion = to_releasename(default_schema_version)      # format the given p4 release version
    oSchema = SchemaXML(schemadir, schemaversion)               # a reference to class SchemaXML
    p4args = {                                                  # the p4 user
        'user': 'mart',
        'port': 'anastasia.local:1777',
        'client': 'computer_p4dlg',
        'oSchema': oSchema
    }
    oP4 = Py4(**p4args)                                         # the p4 connector
    oJnl = P4Jnl(journal, oSchema)                              # the jnl connector

    '''--------------------------------------------------------------------------------------------------------------'''
    ''' test SQL functionality in both abstractions (p4 & jnl)                                                       '''
    '''                                                                                                              '''
    ''' test functions should run twice (once for p4 & again for jnl)                                                '''
    '''--------------------------------------------------------------------------------------------------------------'''

    def testSimpleQuery(self):
        (oP4, oJnl) = (self.oP4, self.oJnl)
        ''' simple queries - returns DLGRecordSets
        '''
        p4qry = (oP4.clients.client.contains('fred'))      # query p4
        p4_recordset = oP4(p4qry)                          # pass it to the connector, retrieve a valid recordset

        jnlqry = (oJnl.domain.type == 99)                  # query jnl
        jnl_recordset = oJnl(jnlqry)                       # pass it to the connector, retrieve a valid recordset

        p4_assertion = (type(p4_recordset).__name__ == 'DLGRecordSet')
        jnl_assertion = (type(jnl_recordset).__name__ == 'DLGRecordSet')
        self.assertTrue(p4_assertion)
        self.assertTrue(jnl_assertion)


    def testSelect(self):
        (oP4, oJnl) = (self.oP4, self.oJnl)
        ''' simple select statement
        '''
        p4qry = (oP4.clients.client.contains('fred'))      # create p4 query
        p4_records = oP4(p4qry).select()                   # select records
        print('SELECT - P4')
        pprint(p4_records(0))

        jnlqry = (oJnl.domain.type == 99)                  # create a jnl query
        jnl_records = oJnl(jnlqry).select()                # select records
        print('SELECT - JNL')
        pprint(jnl_records(0))

        assertion = AND((len(p4_records) > 0), (len(jnl_records) > 0))
        self.assertTrue(assertion)

    def testBelongsNoneNested(self):
        (oP4, oJnl) = (self.oP4, self.oJnl)
        ''' test non-nested belongs function (SQL IN)
        '''
        p4_clientnames = ('computer_dev', 'computer_git', 'computer_p4dlg')
        p4_clientrecords = oP4(self.oP4.clients.client.belongs(p4_clientnames)).select()
        print('BELONGS - p4')
        pprint(p4_clientrecords(0))
        p4_assertion = (type(p4_clientrecords).__name__ == 'DLGRecords')

        jnl_clientnames = ('myclient', 'pycharmclient', 'otherclient')
        jnl_clientrecords = oJnl(self.oJnl.domain.name.belongs(jnl_clientnames)).select()
        print('BELONGS - JNL')
        pprint(jnl_clientrecords(0))
        jnl_assertion = (type(jnl_clientrecords).__name__ == 'DLGRecords')

        self.assertTrue(p4_assertion)
        self.assertTrue(jnl_assertion)

    def testBelongsNested(self):
        (oP4, oJnl) = (self.oP4, self.oJnl)
        ''' test nested belongs function (SQL IN)
        '''
        p4_qry1 = (oP4.files.action.contains('delete'))
        p4_deletedfiles = oP4(p4_qry1)._select(oP4.files.depotFile)
        p4_qry2 = (oP4.files.depotFile.belongs(p4_deletedfiles))
        p4_filerecords = oP4(p4_qry2).select()
        print('BELONGSNESTED - P4')
        pprint(p4_filerecords(0))
        p4_assertion = (type(p4_filerecords).__name__ == 'DLGRecords')

        jnl_qry1 = AND(
            (oJnl.domain.type == '99'),
            (oJnl.domain.owner == 'mart')
        )
        jnl_myclients = oJnl(jnl_qry1)._select(oJnl.domain.name)
        jnl_qry2 = (oJnl.domain.name.belongs(jnl_myclients))
        jnl_clientrecords = oJnl(jnl_qry2).select()
        print('BELONGSNESTED - JNL')
        pprint(jnl_clientrecords(0))
        jnl_assertion = (type(jnl_clientrecords).__name__ == 'DLGRecords')

        self.assertTrue(p4_assertion)
        self.assertTrue(jnl_assertion)

    def testInnerJoin(self):
        (oP4, oJnl) = (self.oP4, self.oJnl)
        ''' test inner joins 
        '''
        p4_reference = (oP4.files.change == oP4.changes.change)
        p4_recs = oP4(p4_reference).select()                                        # easy peasy join
        print('INNERJOIN - P4')
        pprint(p4_recs(0))
        p4_recs_alt = oP4(oP4.files).select(join=oP4.changes.on(p4_reference))      # equivalent to above
        p4_assertion = (len(p4_recs(0).getkeys()) == 2)

        self.assertTrue(p4_assertion)
        self.assertDictEqual(p4_recs(0), p4_recs_alt(0))

        jnl_reference = (oJnl.rev.change == oJnl.change.change)
        jnl_recs = oJnl(jnl_reference).select()                                     # easy peasy join
        print('INNERJOIN - JNL')
        pprint(p4_recs(0))
        jnl_recs_alt = oJnl(oJnl.rev).select(join=oJnl.change.on(jnl_reference))    # equivalent to above
        jnl_assertion = (len(jnl_recs(0).getkeys()) == 2)

        self.assertTrue(jnl_assertion)
        self.assertDictEqual(jnl_recs(0), jnl_recs_alt(0))

    def testOuterJoin(self):
        (oP4, oJnl) = (self.oP4, self.oJnl)
        ''' test inner joins 
        '''
        p4_reference = (oP4.files.change == oP4.changes.change)
        p4_recs = oP4(p4_reference).select()                                        # easy peasy join
        print('LEFTOUTER - P4')
        pprint(p4_recs(0))
        p4_recs_alt = oP4(oP4.files).select(left=oP4.changes.on(p4_reference))      # equivalent to above
        p4_assertion = (len(p4_recs(0).getkeys()) == 2)
        self.assertTrue(p4_assertion)
        self.assertDictEqual(p4_recs(0), p4_recs_alt(0))

        jnl_reference = (oJnl.rev.change == oJnl.change.change)
        jnl_recs = oJnl(jnl_reference).select()                                     # easy peasy join
        print('LEFTOUTER - JNL')
        pprint(jnl_recs(0))
        jnl_recs_alt = oJnl(oJnl.rev).select(left=oJnl.change.on(jnl_reference))    # equivalent to above
        jnl_assertion = (len(jnl_recs(0).getkeys()) == 2)
        self.assertTrue(jnl_assertion)
        self.assertDictEqual(jnl_recs(0), jnl_recs_alt(0))

    def testTableRefs(self):
        (oP4, oJnl) = (self.oP4, self.oJnl)
        ''' Py4 command refs

            accessessing a connector's table attribute is all that is needed
            to force / trigger the Py4 object to define the table (aka the 
            command), attributes as well as it's cmd option references.
            
            as for P4Jnl, the trigger is the same but the references are table
            related only.
        '''
        oP4.reconcile                                                               # access the `reconcile` table
        py4_tabledata = oP4.memoizetable('reconcile')                               # retrieve the table's data
        py4_assertion = (len(py4_tabledata.tableoptions.optionsmap) > 0)
        self.assertTrue(py4_assertion)             # once accessed, tabledata should
                                                                                    # contain a valid optionsmap

        oJnl.domain                                                                 # access table `domain`
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

    #def testRecordsSearch(self):
    #    pass

class TestDLG(unittest.TestCase):
    def testDateTime(self):
        ''' /libdlg/dlgDateTime/DLGDateTime
        '''
        oDateTime = DLGDateTime()                                                   # DLGDateTime instance

        ''' str to datetime / guess
        '''
        guessdate       =   oDateTime.guess('2019/07/26')                           # guess date
        guesstime       =   oDateTime.guess('16:30:45')                             # guess time
        guessdatetime   =   oDateTime.guess('2019/07/26 16:30:45')                  # guess datetime
        print('\n')
        print(f'DLGDateTime - guess_date:       {guessdate}')
        print(f'DLGDateTime - guess_time:       {guesstime}')
        print(f'DLGDateTime - guess_datetime:   {guessdatetime}')
        print('\n')

        ''' string_to_datetime from ...
        '''
        oDateTime.string_to_datetime('2019/08/29')                                  # given a date with a / separator
        oDateTime.string_to_datetime('2019 08 29')                                  # given a date with a ' ' separator
        oDateTime.string_to_datetime('2019-08-29')                                  # given a date with a / separator
        res = oDateTime.string_to_datetime('2019:08:29')                            # given a date with a : separator
        print(f'DLGDateTime - string_to_datetime: {res}')

        ''' to_datetime from ...
        '''
        oDateTime.to_datetime("1547856000.0")
        oDateTime.to_datetime(1547856000.0)
        oDateTime.to_datetime(2019, 8, 19)
        oDateTime.to_datetime(*[2019, 8, 19])
        oDateTime.to_datetime("2019/8/19")
        oDateTime.to_datetime("2019-8-19")
        res = oDateTime.to_datetime(*[2019, 6, 9])
        print(f'DLGDateTime - to_datetime: {res}')

        ''' epoch
        '''
        oDateTime.to_epoch('2019/09/03')                                            # to epoch from str
        d = oDateTime.guess('2019/09/03')                                           # create date
        oDateTime.to_epoch(d)                                                       # to epoch from date
        oDateTime.to_epoch(2019, 9, 3)                                              # args[0], args[1], args[2]
        oDateTime.to_epoch([2019, 9, 3])                                            # [args[0], args[1], args[2]]
        res = oDateTime.to_epoch(*[2019, 9, 3])                                     # *[args[0], args[1], args[2]]
        print(f'DLGDateTime - to_epoch: {res}')

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
        res = oDateTime.to_p4date(1547856000.0)
        print(f'DLGDateTime: {res}')

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
            loader.loadTestsFromTestCase(TestSQL),
            loader.loadTestsFromTestCase(TestDLG)
    ):
        suite.addTests(item)