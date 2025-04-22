try:
    import cPickle as pickle
except ImportError:
    import pickle
from collections import OrderedDict

from libdlg.dlgStore import Storage, objectify, Lst
from libdlg.dlgUtilities  import fix_name, bail

__all__ = ['Py4Model']

'''  [$File: //dev/p4dlg/libdlg/sqlModel.py $] [$Change: 467 $] [$Revision: #12 $]
     [$DateTime: 2024/08/24 08:15:42 $]
     [$Author: zerdlg $]
'''

class Py4Model(object):
    def __init__(
            self,
            schema,
            normalize_tablenames=True,
            tableformat='remove'
    ):
        self.normalize_tablenames = normalize_tablenames
        self.fixname = fix_name(tableformat)
        self.modelized_tablefields = Storage()
        self.common_table_attributes = OrderedDict()
        #self.schema = objectify(schema)
        self.p4schema = objectify(schema)#self.schema
        self.version = self.p4schema.version
        self.upgrades = self.p4schema.numUpgrades
        ''' get a list of recordTypes  
        '''
        self.p4recordtypes = Storage(
            {
                row.name: row for row in self.p4schema.recordtypes.record
            }
        )

        ''' get a list of tables 
        '''
        self.p4tables = Storage(
            {
                row.name: row for row in self.p4schema.tables.table
            }
        )

    def __call__(self):
        return self

    def update_commonfields_on_tables(self, table=None):
        '''  update tables with common fields
        '''
        def updatetablefields(tbl):
            [self.p4tables[tbl].update(**{cfld: None}) \
             for cfld in self.common_table_attributes.keys() \
             if (not cfld in self.p4tables[tbl].keys())]

        updatetablefields(table) \
            if (table is not None) \
            else [updatetablefields(tbl)
                    for tbl in self.p4tables]

    def modelize_schema(self):
        model = Storage()
        for (table, tbl_attributes) in self.p4tables.items():
            normalized_tablename = self.fixname(table, 'table') \
                if (self.normalize_tablenames is True) \
                else table
            ''' collect all common table attributes, then once more around
                the post at the end of this loop... add missing attributes
                to all tables with a 'None' value (for consistency)    
            '''
            [self.common_table_attributes.update(**{tableatt: None})
                for tableatt in self.p4tables[table].keys()
                if (not tableatt in self.common_table_attributes)]
            ''' set the current record 
            '''
            currentrecord = tbl_attributes#Storage(tbl_attributes)#.copy())
            ''' we need to add extra default fields to each table! 
            '''
            tablefields = objectify(
                [
                    {
                        'type': 'integer',
                        'name': 'idx',
                        'desc': 'index value of the current record',
                        'default': 0,
                        'comment': '',
                        'label': '',
                        'fieldname': 'idx',
                        'requires': None
                    },
                    {
                        'type': 'string',
                        'name': 'db_action',
                        'desc': 'Operation on table',
                        'default': '',
                        'comment': '',
                        'label': '',
                        'fieldname': 'db_action',
                        'requires': None
                    },
                    {
                        'type': 'integer',
                        'name': 'table_revision',
                        'desc': 'Revision of the p4 schema table',
                        'default': '',
                        'comment': '',
                        'label': '',
                        'fieldname': 'table_revision',
                        'requires': None
                    },
                    {
                        'type': 'string',
                        'name': 'table_name',
                        'desc': 'Redundant... but this is the table name',
                        'default': '',
                        'comment': '',
                        'label': '',
                        'fieldname': 'table_name',
                        'requires': None
                    }
                ]
            )

            ''' define the record type.
            '''
            if (tbl_attributes.type != 'String'):
                if (tbl_attributes.type.lower() == tbl_attributes.type):
                    tbl_attributes.type = tbl_attributes.type.capitalize()
                recordDefinition = self.p4recordtypes[tbl_attributes.type]

                if (isinstance(recordDefinition.column, list) is False):
                    recordDefinition.column = Lst([recordDefinition.column])

                for colatt in recordDefinition.column:
                    colatt.update(
                        **{
                             'default': '',
                             'comment': '',
                             'label': '',
                             'requires': None,
                             'fieldname': self.fixname(colatt.name, 'field')
                        }
                    )
                    tablefields.append(colatt)
            self.modelized_tablefields.update(**objectify(OrderedDict({table: tablefields})))
            currentrecord.update(
                **{
                    'fields': tablefields,
                    'tablename': normalized_tablename,
                    'version': self.version
                }
            )
            ''' add the fields key to current record 
            '''
            model.merge(**{normalized_tablename: currentrecord})
        self.update_commonfields_on_tables()
        return objectify(model)