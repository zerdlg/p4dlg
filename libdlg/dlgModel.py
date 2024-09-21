import re
try:
    import cPickle as pickle
except ImportError:
    import pickle
from collections import OrderedDict

from libdlg.dlgStore import Storage, objectify, Lst
from libdlg.dlgQuery_and_operators import AND, OR
from libdlg.dlgUtilities  import fix_name, bail

__all__ = ['Py4Model']

'''  [$File: //dev/p4dlg/libdlg/dlgModel.py $] [$Change: 467 $] [$Revision: #12 $]
     [$DateTime: 2024/08/24 08:15:42 $]
     [$Author: mart $]
'''

''' the p4 schema / model

    * Though the p4 schema does describe journal records, keep in mind that
      its description names the fields (in order) starting with field 3 (field[2])
      of each record.

      eg.
                 table version
                 |
                 |
            @pv@ 6 @db.domain@ @depot@ 100 @@ @@ @@ @@ @@ 1337399100 1337399100 0 @Default depot@ @@ @@ 0
            |           |
            action      |
                        |
                        table


            in this case, the first 3 fields (operator/table version/tablename) are skipped in each
            description in the schema

            the schema would define this record as follows::

            --------------|----------------|---------------------------------------------------------------------------
            Name          |  type          |  description
            --------------|----------------|---------------------------------------------------------------------------
            name          |  Domain        |  domain name
            type          |  DomainType    |  type of domain
            extra         |  Text          |  host
            mount         |  Text          |  the client Root
            mount2        |  Text          |  alternate client Root
            mount3        |  Text          |  alternate client Root
            owner         |  User          |  name of user who owns the table
            updateDate    |  Date          |  date of last update to the spec
            accessDate    |  Date          |  date of last access to the spec
            options       |  DomainOpts    |  options for client, label & branch domains
            description   |  Text          |  description of domain
            stream        |  Domain        |  associated stream for client records
            serverId      |  Key           |  associated server ID for client records
            contents      |  Int           |  Client domain contents:
                                                1. Maps only classic depot files
                                                2. Maps only graph depot files 
                                                3. Maps both classic & graph depo files 
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
        ''' lists of references & table attributes
        '''
        self.fieldreference = Storage()
        self.datatypereference = Storage()
        self.common_table_attributes = OrderedDict()
        self.schema = objectify(schema)
        self.p4schema = self.schema
        self.version = self.p4schema.version
        self.upgrades = self.p4schema.numUpgrades
        ''' get a list of recordTypes  
        '''
        self.p4recordtypes = Storage(
            {
                row.name: row for row in self.p4schema.recordtypes.record
            }
        )
        ''' get a list of datatypes 
        '''
        self.p4datatypes = Storage(
            {
                row.name: row for row in self.p4schema.datatypes.datatype
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

    def guessFieldType(
            self,
            fieldtype,
            fieldname,
            tablename=None
    ):
        types = Storage(
            {
                 'flag': 'string',
                 'Flag': 'string',
                 'integer': 'integer',
                 'string': 'string',
                 'bitmask': 'hex',
                 'Bitmask': 'hex',
                 'text': 'text',
                 'Text': 'text',
                 'Domain': 'string',
                 'String': 'string',
                 'User': 'string',
                 'Int': 'integer',
                 'int': 'integer',
                 'File': 'string',
                 'Change': 'integer',
                 'Counter': 'string',
                 'Date': 'integer',
                 'ServerType': 'string',
                 'Partition': 'integer',
                 'TraitLot': 'integer'
            }
        )

        def getfieldtype(field):
            try:
                return Storage(
                    {
                        'fieldtype': 'string'
                            if AND(
                                    (self.p4datatypes[field] is None),
                                    (types[field] == 'hex')
                        ) \
                            else 'text' if (self.p4datatypes[field] in ('Text', 'text')) \
                            else types[self.p4datatypes[field].type],
                        'datatypetype': self.p4datatypes[field].type,
                        'datatypename': self.p4datatypes[field].name,
                        'datatypedesc': self.p4datatypes[field].desc,
                        'datatypesummary': self.p4datatypes[field].summary
                    }
                )
            except Exception as err:
                print(f"Exception while guessing field type, arg 'field' isn't recognized :( {err}")
                return Storage()

        def is_p4_valid_field(field):
            ''' requires validation
            '''
            firstfields = [
                'idx',
                'db_action',
                'table_revision',
                'table_name'
            ]
            if tablename in [
                self.fixname(t, 'table') for t in \
                    self.modelized_tablefields.keys()
            ]:
                for row in self.modelized_tablefields[tablename]:
                    if OR(
                            (row.name in firstfields),
                            (row.name == field.name)
                    ):
                        return True
            return False

        if (fieldtype != 'int'):
            return getfieldtype(fieldtype) \
                if (fieldtype in self.p4datatypes.keys()) \
                else Storage({'fieldtype': types[fieldtype]}) \
                if (fieldtype in types.keys()) \
                else 'string'
        elif AND(
                    (fieldtype == 'int'),
                    (fieldname in ('partition', 'trailtLot'))
        ):
            ''' this is a bad fix for a bad mapping/case mismatch from
                db.domain.datatypes.datatype.Int to field.type (int)
                
                * If I remember correctly, this issue was fixed in r13.x (or there abouts),
                  though I could be wrong, In any case, I don't think it matters much anymore 
            '''
            fieldtype = 'Int'
            return getfieldtype(fieldtype)
        else:
            valid_field = is_p4_valid_field(fieldname)
            if (valid_field is True):
                datatypedict = Storage(
                    {
                         'fieldtype': fieldtype,
                         'datatypetype': fieldtype,
                         'datatypename': '',
                         'datatypedesc': '',
                         'datatypesummary': ''
                    }
                )
                datatypedict.fieldtype = fieldtype \
                    if (is_p4_valid_field(fieldname) is True) \
                    else 'string'
                return datatypedict
            else:
                return Storage({'fieldtype': fieldtype})

    def normalizeTableName(self, name):
        tblname = Lst(name.split('.', 1))
        nname = tblname(1) \
            if (name.startswith('db.')) \
            else tblname(0) \
            if (name.endswith('.db')) \
            else name
        return ''.join(nname.split('.')) \
            if ('.' in nname) \
            else nname

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
            [self.common_table_attributes.update(**{tableatt: None}) \
             for tableatt in self.p4tables[table].keys() \
             if (not tableatt in self.common_table_attributes)]
            ''' define the name of the record type 
            '''
            recordtype = tbl_attributes.type
            recordDefinition = self.p4recordtypes[recordtype]
            ''' set the current record 
            '''
            currentrecord = Storage(tbl_attributes)#.copy())
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
            try:
                ''' adding a check and recordDefinition.column type update
                    because perforce appears to have forgotten to set the 
                    column type as a list. This breaks the flow.
                    
                    I.e. check db.streamq
                '''
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
                self.modelized_tablefields.update(objectify(OrderedDict({table: tablefields})))
                currentrecord.update(
                    **{
                        'fields': tablefields,
                        'tablename': normalized_tablename,
                        'schemadir': self.version
                    }
                )
                ''' add the fields key to current record 
                '''
                model.update(**{normalized_tablename: currentrecord})
            except Exception as err:
                bail(err)

        self.update_commonfields_on_tables()
        return objectify(model)

    def domaintypes(self):
        return self.p4datatypes.DomainType

    def domaintype_byname(self, dtype):
        try:
            DomainTypeRecord = Lst(
                ddata for ddata in self.domaintypes()['values'] \
                    if (ddata.desc == dtype)
            )(0)
            if (DomainTypeRecord is not None):
                TypeValue = DomainTypeRecord.value
                DomainType = Lst(re.split('\s', TypeValue))(0)
                return DomainType
        except Exception as err:
            return
