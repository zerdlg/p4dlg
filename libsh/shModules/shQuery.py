import os
import re
from pprint import pprint

from libdlg.dlgStore import Storage, Lst
from libdlg.dlgQuery_and_operators import AND
from libdlg.dlgUtilities import remove
from libdlg.dlgFileIO import ispath, is_writable, make_writable
from libsh.shModules.shGuessRelease import GuessVersion
from libjnl.jnlIO import P4Jnl
from libdlg.dlgSchema import SchemaXML
from libdlg.dlgTables import DataGrid, DataTable

from os.path import dirname
import schemaxml
from resc import journals
schemadir = dirname(schemaxml.__file__)
journaldir = dirname(journals.__file__)

'''  [$File: //dev/p4dlg/libsh/shModules/shQuery.py $] [$Change: 458 $] [$Revision: #14 $]
     [$DateTime: 2024/08/07 05:21:26 $]
     [$Author: mart $]
'''

__all__ = ['RunQuery']


class RunQuery(object):
    '''     NO LONGER WORKING - MUST FIX

            QUERY OPTIONS
                {'columns': 'id,owner,mount,name,options',
                 'delete':False,
                 'delimiter': '@',
                 'dialect': 'journal',
                 'exclude': None,
                 'fetch': None
                 'filter': None
                 'find': None,
                 'groupby': None,
                 'insert': None
                 'journal': '/Users/gc/swerve.client/databraid/main/resc/journals/journal2',
                 'limitby': None,
                 'maxrows': 10,
                 'newcolumns':  None,
                 'oSchema': <resc.p4.libpy4.p4Schema.SchemaXML object at 0x10396df00>
                 'orderby': None,
                 'output': 'DataGrid'
                 'query': 'domain.type=99',
                 'schemadir': 'r15.2',
                 'schemadir': os.path.abspath('../../../resc/p4/libpy4/schemaxml'),
                 'search': False,
                 'select':True,
                 'sort': 'id',
                 'update':False}

            + QUERY SETTINGS

                 {
                 'queryformat': 'strQuery',
                 'check_keywords': False,
                 'normalize_tablenames': True,
                 'tableformat': 'remove',
                 'maxrows': 50,
                 'recordchunks': 150000,
                 'keyed': True,
                 '_table': 'a table name' or None,
                 'tablepath': os.path.abspath('../../../storage')
                 }
    '''

    def __init__(self, **options):

        # ADD ALL OPTIONS AND SETTING DEFAULT TO self.options!!!!!

        options = Storage(options)
        '''  select / fetch / delete / update / insert / search
        '''
        [options.update(**{item: False}) for item in ('fetch',
                                                      'update',
                                                      'delete',
                                                      'insert',
                                                      'search',) \
         if (options[item] is None)]
        options.select = False if (True in (options.fetch,
                                            options.update,
                                            options.delete,
                                            options.insert,
                                            options.search)) \
            else True
        '''  set/define columns/newcolumns & output
        '''
        if (options.output is None):
            options.update(**{'output': 'DataGrid'})
        ''' set/define oSchema
        '''
        if AND((options.oSchema is None), (options.schemaversion is None)):
            options.schemaversion = Lst(GuessVersion()(options.journal))(1)

        if (options.oSchema is None):
            options.oSchema = SchemaXML(schemadir=schemadir)(version=options.version)
        options.merge({"oSchema": options.oSchema})

        self.options = options

    def __call__(self, *fields, **kwargs):
        p4Jnl = P4Jnl(self.options.journal, self.options.oSchema, **self.options)
        records = Lst()
        record = {}

        if (re.match(r"^[gG]rid$", self.options.output)):
            self.options.output = 'DataGrid'

        if (isinstance(self.options.fieldnames, str)):
            self.options.fieldnames = Lst(self.options.fieldnames.split(','))

        if (self.options.select is True):
            records = p4Jnl(self.options.query).select(*self.options.columns or [], **self.options)

        elif (self.options.fetch is True):
            records = p4Jnl(self.options.query).fetch(*self.options.columns or [], **self.options)

        elif (self.options.update is True):
            records = p4Jnl(self.options.query).delete(*self.options.columns or [], **self.options)

        elif (self.options.delete is True):
            records = p4Jnl(self.options.query).delete(*self.options.columns or [], **self.options)

        elif (self.options.insert is True):
            records = p4Jnl(self.options.query).delete(*self.options.columns or [], **self.options)

        elif (self.options.search is True):
            records = p4Jnl(self.options.query).delete(*self.options.columns or [], **self.options)

        if (self.options.output in (False, None)):
            return records

        if (re.match(r"^[Dd]ata[gG]rid$", self.options.output)):
            return DataGrid(records)()
        elif (re.match(r"^[Dd]ata[Tt]able$")):
            return  DataTable(record)()

        elif (re.match(r"^[cCsSvV]$", self.options.output)):
            ''' not yet implemented '''

        elif (ispath(self.options.output) is True):
            self.options.output = os.path.abspath(self.options.output)
            if (os.path.exists(self.options.output)):
                if (is_writable(self.options.output) is False):
                    make_writable(self.options.output)
                remove(self.options.output)

        #elif (is_inst_RowsRecords(self.options.output)):
        #    pprint([Record(rec) for rec in records])

        #elif (self.options.output == 'dict'):
        #    pprint(records)
        print(records)
        #records.groupby('name').first()
        pprint([rec for rec in records])

def main():
    '''  {'sort':'id',
            'dialect':'journal',
            'journal':'./journals/journal2',
            'delimiter':'@',
            'which':'query',
            'output':'grid',
            'query':'domain.type=99',
            'schemadir':'r15.2',
            'columns':'id,owner,mount,name,options'}

    resolves as:

            {'sort': 'id',
            'dialect': 'journal',
            'schemadir': '/Users/gc/swerve.client/databraid/main/resc/p4/libpy4/schemaxml',
            'journal': '/Users/gc/swerve.client/databraid/main/resc/journals/journal2',
            'delimiter': '@',
            'which': 'query',
            'output': 'DataGrid',
            'query': 'domain.type=99',
            'select': True,
            'schemadir': 'r15.2',
            'columns': 'id,owner,mount,name,options',
            'fetch': False,
            'update': False,
            'delete': False,
            'insert': False,
            'search': False,
            'newcolumns': [],
            'oSchema': <resc.p4.libpy4.p4Schema.SchemaXML object at 0x105bd4040>}
    '''
    RunQuery(**{'sort': 'id',
                'dialect': 'journal',
                'schemadir': schemadir,
                'journal': f'{journaldir}/journal2',
                'delimiter': '@',
                'which': 'query',
                'output': 'DataGrid',
                'query': 'domain.type=99',
                'select': True,
                'schemadir': 'r15.2',
                'fieldnames': 'id,owner,mount,name,options'})()


if __name__ == '__main__':
    main()
