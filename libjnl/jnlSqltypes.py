import re
import datetime

from libdlg.dlgStore import (
    Storage,
    objectify,
    Lst
)
from libdlg.dlgQuery_and_operators import *
from libdlg.dlgSchemaTypes import SchemaType
from libdlg.dlgError import *
from libdlg.dlgUtilities import (
    reg_ipython_builtin,
    serializable,
    reg_valid_table_field,
    bail
)

__all__ = ['JNLTable', 'JNLField']

'''  [$File: //dev/p4dlg/libjnl/jnlSqltypes.py $] [$Change: 479 $] [$Revision: #17 $]
     [$DateTime: 2024/09/20 07:42:22 $]
     [$Author: mart $]
'''

''' the schema & mappings -> this is a good time to pay attention

Consider the following table.
      +---------<->---------------+------------------------<->------------------------+
      ^                           ^                                                   ^
      |                           |                                                   |
      V                           V                                                   V
+===========================================================|=========================================================+
|   TABLE   |                   RECORD                      |                     DATATYPE                            |
|-----------+-----------------------------------------------|                                                         |
|           |    NAME   |           COLUMN                  |                                                         |
|           +-----------+-----------------------------------|---------------------------------------------------------+
|           |           |    NAME     |     TYPE     | DESC |     NAME     |   TYPE  | SUMMARY | DESC | VALUES (...)  |
|           |           |             |              |      |              |         |         |      |---------------|
|           |           |             |              |      |              |         |         |      | VAL.| DESC    |
+===========+===========+=============+==============+======|==============+=========+=========+======+=====+=========+
| db.domain | Domain    | name        | Domain       | ...  | Domain       | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | type        | DomainType   | ...  | DomainType   | flag    | ...     | ...  | 98  | branch  |
|           |           |             |              |      |              |         |         |      | 99  | client  |
|           |           |             |              |      |              |         |         |      | 100 | depot   |
|           |           |             |              |      |              |         |         |      | 108 | label   |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | extra       | Text         | ...  | Text         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | mount       | Text         | ...  | Text         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | mount2      | Text         | ...  | Text         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | mount3      | Text         | ...  | Text         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | owner       | User         | ...  | User         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | updateDate  | Date         | ...  | Date         | integer | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | accessDate  | Date         | ...  | Date         | integer | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | options     | DomainOpts   | ...  | DomainOpts   | bitmask | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | description | Text         | ...  | Text         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | stream      | Text         | ...  | Text         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | serverid    | Text         | ...  | Text         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | partition   | Int          | ...  | Int          | integer | ...     | ...  | N/A | N/A     |
+===========+===========+=============+==============+======|==============+=========+=========+======+=====+=========+
| db.change | Change    | change      | Change       | ...  | Change       | integer | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | descKey     | Change       | ...  | Change       | integer | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | client      | Domain       | ...  | Domain       | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | user        | User         | ...  | User         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | date        | Date         | ...  | Date         | integer | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | status      | ChangeStatus | ...  | ChangeStatus | flag    | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | description | DescShort    | ...  | DescShort    | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | root        | Mapping      | ...  | Mapping      | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | importer    | String       | ...  | String       | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | identify    | String       | ...  | String       | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | access      | Date         | ...  | Date         | integer | ...     | ...  | N/A | N/A     |
+===========+===========+=============+==============+======|==============+=========+=========+======+=====+=========+
| db.depot  | DepotType | name        | Domain       | ...  | Domain       | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | type        | DepotType    | ...  | DepotType    | flag    | ...     | ...  | 0   | local   |
|           |           |             |              |      |              |         |         |      | 1   | remote  |
|           |           |             |              |      |              |         |         |      | 2   | spec    |
|           |           |             |              |      |              |         |         |      | 3   | stream  |
|           |           |             |              |      |              |         |         |      | 4   | archive |
|           |           |             |              |      |              |         |         |      | 5   | unload  |
|           |           |             |              |      |              |         |         |      | 6   | tangent |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | extra       | Text         | ...  | Text         | string  | ...     | ...  | N/A | N/A     |
|           |           +-------------+--------------+------|--------------+---------+---------+------+-----+---------+
|           |           | Map         | Text         | ...  | Text         | string  | ...     | ...  | N/A | N/A     |
+===========+===========+=============+==============+======|==============+=========+=========+======+=====+=========+

The schema is structured as follows:

    7 high level keys: 
            * commit_upgrades
            * datatypes
            * numUpgrades
            * recordtypes
            * tables
            * upgrades
            * version 

For now, let's focus on `datatypes`, `recordtypes` & `tables` only.

schema paths (db.domain as an example):

Schema 
    |__/datatypes
    |           |__/datatype
    |                  |
    |                  |__/[{'name': 'Domain',     <-------------<->---------|
    |                  |     'type': 'string',                               |
    |                  |     'desc': 'A string representing the name of a depot, label, client, branch, typemap, or stream.'
    |                  |     'summary': 'A domain name'},                    |
    |                  |                                                     |
    |                  |__/ {'name': 'DomainType',   <------<->----|         |
    |                  |     'type': 'flag',                       |         |
    |                  |     'desc': 'A integer value representing the type of a domain',
    |                  |     'summary': 'A domain type'}           |         |
    |                  |                                           |         |
    |                  |... ]                                      |         |
    |                                                              |         |
    |__/recordtypes                                                |         |
    |           |                                                  ^         ^ 
    |           |__record                                          |         | 
    |                  |                                           V         V
    |                  |__/{'name': 'Domain,                       |         |
    |                       'column':                              |         |
    |                            |                                 |         ^
    |                            |__/[{'name': 'name',             |         |
    |                            |     'type': 'Domain',    <------|---<->---|    
    |                            |     'desc': 'Domain name'},     |         |
    |                            |                                 ^         ^
    |                            |__/ {'name': 'type',             |         |
    |                            |     'type': 'DomainType'  <-->--|         V
    |                            |     'desc': 'Type of domain'},            |
    |                            |                                           |
    |                            |__/ {'name': 'extra',                      |
    |                            |     'type': 'Text'                        |
    |                            |     'desc': 'Formerly "host". Associated host or, for labels, revision number.'},
    |                            |                                           |
    |                            |__/ {'name': 'mount',                      |
    |                            |     'type': 'Text',                       |
    |                            |     'desc': 'The client root'},           |
    |                            |                                           |
    |                            |__/ {'name': 'mount2',                     |
    |                            |     'type': 'Text',                       |
    |                            |     'desc': 'Alternate client root'},     ^
    |                            |                                           |
    |                            |... ]                                      |
    |                                                                        V
    |__/tables                                                               |
            |                                                                |
            |__/table                                                        |
                   | [                                                       |
                   |__/{'name': 'db.domain',                                 |
                   |    'type': 'Domain',       <---------------<->----------|
                   |    'version': '6', 
                   |    'classic_lockseq': '17', 
                   |    'peek_lockseq': '17', 
                   |    'keying': 'name', 
                   |    'desc': 'Domains: depots, clients, labels, branches, streams, and typemap'}
                   |...]
            
Table, Datatypes & Records

a Table has 7 Attributes:
                            {'name': 'db.domain', 
                             'type': 'Domain', 
                             'version': '6', 
                             'classic_lockseq': '17', 
                             'peek_lockseq': '17', 
                             'keying': 'name', 
                             'desc': 'Domains: depots, clients, labels, branches, streams, and typemap'}

A Table contains recordtypes

A recordtype has 2 attributes: `name`           
                               `column' 
                                  
A column/field has 3 attributes: `name`           --> the field/column name
                                 `type`           --> the field/column type
                                 `description`    --> the field/column description
                           
A column's `type` maps to a `datatype` definition (specifically its type) 

A datatype definition has at least 4 attributes: `name`
                                                 `desc`
                                                 `summary'
                                                 `type`

Some datatypes are broken down into a number of flags. 
I.e. Table 'db.domain' store records that describe a number of domain types.
Each domain type is identified by a unique flag. A `Domain` datatype can define
a specification such as a `branch`, a `client`, a `depot`, a `label`, a `stream` 
or a `typemap`. Each domain type (spec) has a associated flag.   

    eg.
            branch: 98
            client: 99
            depot: 100
            label: 108
            stream: 115
            typemap: 116

That said, we can write queries to select domain records based on their type.
P4Q will understand a query that specifies the domain type by flag or by name.

therefore, the following queries are equivalent:           
    
    >>> qry = (oJnl.domain.type == 99) or '99'
    >>> qry = (oJnl.domain.type == 'client')

both will have the affect of selecting all client records stored in db.domain

    >>> client_records = oJnl(qry).select()
'''

class JNLTable(object):
    ''' Usage:

            >>> oTable = JNLTable('change',oSchema)

        .fields('change')

            >>> tbl = oTable.fields('change')
            >>> pprint(tbl)
            {'comment': '',
             'default': '',
             'desc': 'The change number',
             'fieldname': 'change',
             'label': '',
             'name': 'change',
             'requires': None,
             'sqltype': {'fieldtype': 'string'},
             'type': 'Change'}
    '''

    def __or__(self, othertable):
        return Lst(self, othertable)

    def __init__(
                    self,
                    objp4,
                    tablename,
                    oSchema=None,
                    *args,
                    **tabledata
    ):
        (args, tabledata) = (Lst(args), Storage(tabledata))
        self.objp4 = objp4
        self.tablename = tablename
        self.oSchema = oSchema or self.objp4.oSchema
        self.oSchemaType = SchemaType(self.objp4)
        self.jnlRecords = Lst()
        (
            self.fieldsmap,
            self.fieldtypesmap
        ) = \
            (
                tabledata.fieldsmap or Storage(),
                tabledata.fieldtypesmap or Storage()
            )
        ''' schema data
        '''
        self.p4schema = self.oSchema.p4schema or Storage()
        self.schemaversion = self.p4schema.version
        self.p4model = self.oSchema.p4model or Storage()
        self.modeltable = self.p4model[self.tablename] or Storage()
        self.modelfields = self.modeltable.fields \
            if (self.modeltable.fields is not None) \
            else Lst()
        self.fieldnames = Lst(mfield.name for mfield in self.modelfields)
        ''' schema table data
        '''
        self.name = tabledata.name
        self.type = tabledata.type
        self.version = tabledata.version
        self.classic_lockseq = tabledata.classic_lockseq
        self.peek_lockseq = tabledata.peek_lockseq
        self.keying = tabledata.keying
        self.desc = tabledata.desc
        self.tablename = tablename
        self.schemaversion = tabledata.schemaversion
        self.referenced_by = []

        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.objp4.logger,
                    f'log{logitem}'
                )
            ) for logitem in (
                'info',
                'warning',
                'error',
                'critical'
            )
        ]

        ''' maps and field names & references
        '''
        fields = Lst()#Storage()
        for field in self.modelfields:
            try:
                field.merge(
                    {
                        'fieldmap': {
                            field.name.lower(): field.name
                        },
                        'tablename': self.tablename,
                        'desc': re.sub('\t|    ', '', field.desc)
                    }
                )
            except Exception as err:
                pass
            oField = JNLField(
                                field,
                                objp4=objp4,
                                oSchema=self.oSchema,
                                oSchemaType=self.oSchemaType
            )
            #fields.merge({field.name: oField})
            setattr(self, field.name, oField)
            fields.append(getattr(self, field.name))
        self.fields = objectify(fields)

    __setitem__ = lambda self, key, value: setattr(self, str(key), value)
    __name__ = lambda self: self.oSchema.p4model[self.tablename].name
    __delitem__ = object.__delattr__
    __contains__ = lambda self, key: key in self.__dict__
    __nonzero__ = lambda self: (len(self.__dict__) > 0)
    keys = lambda self: self.oSchema.p4model[self.tablename].keys()
    items = lambda self: self.__dict__.items()
    values = lambda self: self.__dict__.values()
    __iter__ = lambda self: self.__dict__.__iter__()
    iteritems = lambda self: iter(self.__dict__.items())
    __str__ = lambda self: f'<JNLTable {self.tablename}>'

    def __call__(self, *fields):
        for field in fields:
            fieldatts = self.getfield(field)
        return self

    def as_dict(self):
        EXCLUDE = (
            'p4schema',
            'p4model',
            'modelfields',
            'modeltable',
            'cmdref'
        )

        def mergeKeyValue(qdict):
            dgen = {}
            for (key, value) in qdict.items():
                if key in ("left", "right"):
                    dgen[key] = mergeKeyValue(self.__class__) \
                        if isinstance(value, JNLTable) \
                        else value
                elif (type(value).__name__ in ('JNLField', 'Py4Field')):
                    dgen[key] = value.as_dict()
                elif AND(
                        (isinstance(value, serializable) is True),
                        (key not in EXCLUDE)
                ):
                    dgen[key] = mergeKeyValue(value) \
                        if (isinstance(value, dict)) \
                        else value
            return dgen
        return objectify(mergeKeyValue(self.__dict__))

    def fields(self, name=None):
        '''  same as getfield() - incase some places may still be making a call to this
        '''
        return self.getfield(name=name)

    def getfield(self, name=None):
        ''' return all p4 data for a given field

            >>> oJnl.rev.fields('depotfile')
            {'comment': '',
             'defaul': '',
             'desc': 'the file name',
             'fieldname': 'depotFile',
             'label': '',
             'name': 'depotFile',
             'requires': None,
             'type': 'File'}

            >>> oJnl.rev.fields()   #no args, returns list of all fields for for this table
            [{'comment': '',
              'default': '',
              'desc': 'Record number as read in',
              'fieldname': 'id',
              'label': '',
              'name': 'id',
              'type': 'integer'},
             {'comment': '',
              'default': '',
              'desc': 'Operation on table',
              'fieldname': 'db_action',
              'label': '',
              'name': 'db_action',
              'requires': None,
              'type': 'string'},
              ...
              ]
    '''
        if (name is None):
            return self.modelfields
        if (isinstance(name, str)):
            if (name.lower() in self.fieldsmap.getkeys()):
                modelfield = next(
                            filter(
                                    lambda field: field.name.lower() \
                                        == str(name).lower(), self.modelfields
                            )
                )
                return modelfield

    def __getattr__(self, key):
        ''' does pkey start with P4|p4 ?

            we get a 'p4' prefix when sQuery encounters a JNLTable name or a JNLField name
            happens to be an SQL reserved keyword for a given SQL system. I.e.: db.user
            loses its 'db.' prefix so then 'user' becomes problamatic. In this case the new
            'user' tablename gets renamed to 'p4user' while the 'User/user' fieldname gets
            renamed to 'P4user'. Anyways, its what I have
        '''
        rkey = re.sub(r"^[pP]4", '', key).lower()
        if (self.fieldsmap[rkey] is not None):
            key = rkey
        if (self.fieldsmap[key.lower()] is None):
            raise FieldNotBelongToTableError(self.tablename, key)
        if (key.lower() in self.fieldsmap):
            key = self.fieldsmap[key]

        key_dismiss_attr = [
            item for item in (
                                (reg_ipython_builtin.match(key)),
                                (re.match(r'^_.*$', key)),
                                (re.match(r'^shape$', key))
            ) \
            if (
                    item not in (None, False)
            )
        ]
        if (len(key_dismiss_attr) > 0):
            pass

        if AND(
                ('tablename' in self.__dict__.keys()),
                (not 'fieldname' in self.__dict__.keys())
        ):
            ''' is it a table attribute, or a JNLField, something else?
            '''
            if (self.tablename is None):
                return JNLField()
        try:
            '''  if key is a JNLField, then set it as class instance attribute
                 and with its real intended name provided by self.fieldsmap
            '''
            fieldatt = self.getfield(key)
            if AND(
                    (fieldatt is not None),
                    (hasattr(fieldatt, 'name'))
            ):
                if (key == fieldatt.name):
                    return getattr(self, key)
            self.logerror(f'[{key}] does not belong to table `{self.tablename}`.')
        except KeyError:
            self.logerror(f'[{key}] does not belong to table `{self.tablename}`.')

    __getitem__ = __getattr__

class JNLField(DLGExpression):
    __str__ = __repr__ = lambda self: f"<JNLField {self.fieldname}>"

    def update_instance(self, op=None, left=None, right=None, **kwargs):
        '''  attributes for operators instead of DLGQuery class reference?
        '''
        [setattr(self, argitem.__name__, argitem)
         for argitem in (op, left, right)
         if (argitem is not None)]

        [setattr(self,k, v) for (k, v) in kwargs.items()
         if (len(kwargs) > 0)]

    __hash__ = lambda self: hash((frozenset(self), frozenset(self.objp4)))

    def containschars(self):
        return self.__contains__(self)

    def __len__(self):
        return lambda i: len(self.__dict__[i])

    def __init__(
                 self,
                 field,
                 objp4,
                 oSchema,
                 oSchemaType,
                 required=False,
                 writable=True,
                 readable=True,
                 filter_in=None,
                 filter_out=None,
                 _rname=None,
                 table=None,
                 **kwargs
    ):
        self.fieldname = fieldname = field.fieldname
        self.tablename = tablename = field.tablename
        self.table = table
        super(JNLField, self).__init__(
            objp4,
            None,
            self,
            None,
            fieldname=fieldname,
            tablename=tablename
        )
        fieldattributes = [fielditem for fielditem in field]
        self.attributesmap = Storage({fitem.lower(): fitem for fitem in fieldattributes})
        [setattr(self, fielditem, field[fielditem]) for fielditem in field]
        self.__dict__ = objectify(self.__dict__)

        if OR(
                (not isinstance(self.fieldname, str)),
                (reg_valid_table_field.match(self.fieldname) is None)
        ):
            error = f'Invalid field name `{self.fieldname}`'
            objp4.logerror(error)
            bail(error)

        self.ignore_fields = (
            'idx',
            'db_action',
            'table_revision',
            'table_name'
        )

        self._rname = _rname
        self.oSchema = oSchema
        self.oSchemaType = oSchemaType
        self.objp4 = objp4
        self._table = None

        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.objp4.logger \
                        if (hasattr(self.objp4, 'logger')) \
                        else kwargs.logger or 'INFO',
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
        )
        ]

        self.op = None
        self.left = None
        self.right = None
        self.required = required
        self.writable = writable
        self.readable = readable
        self.filter_in = filter_in
        self.filter_out = filter_out
        ''' Set kwargs as instance attributes to overload 
            existing attributes or merge in new attributes. 
        '''
        [setattr(self, key, kwargs[key]) for key in kwargs]

    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, key):
        key_dismiss_attr = [
            item for item in (
                (reg_ipython_builtin.match(key)),
                (re.match(r'^_.*$', key)),
                (re.match(r'^shape$', key))
            ) \
            if (
                    item not in (None, False)
            )
        ]
        if (len(key_dismiss_attr) > 0):
            pass
        if OR(
                (re.match(r'^_([_aA-zZ])*_$', key) is not None),
                (key in ('objp4', '__members__', 'getdoc'))
        ):
            pass

        attkeys = Storage(self.__dict__).getkeys()
        if (not key in attkeys):
            if (self.attributesmap[key.lower()] is not None):
                return getattr(self, self.attributesmap[key.lower()])
            return
        return getattr(self, key)

    __getitem__ = __getattr__
    __get__ = lambda self: self.__getitem__

    def as_dict(self, flat=False, sanitize=True):
        attrs = (
                'fieldname', 'tablename',
                'op', 'left', 'right',
                'type', 'required',
                'label', 'comment',
                'writable', 'readable',
                '_rname'
        )

        def flatten(obj):
            flatobj = dict(
                (
                    flatten(k),
                    flatten(v)
                ) for k, v in obj.items()
            ) \
                if (isinstance(obj, dict) is True) \
                else [flatten(v) for v in obj] \
                if (isinstance(obj,
                               (
                                    tuple,
                                    list,
                                    set
                               )
                            )
            ) \
                else str(obj) \
                if (isinstance(obj,
                               (
                                   datetime.datetime,
                                   datetime.date,
                                   datetime.time
                               )
                            )
            ) \
                else obj \
                if (isinstance(obj, serializable)) \
            else None
            return flatobj

        fielddict = Storage()
        if NOT(
                AND(
                    (sanitize is True),
                    OR(
                        (self.readable is True),
                        (self.writable is True)
                    )
                )
        ):
        #if not (not (self.readable or self.writable)):
            for attr in attrs:
                if (flat is True):
                    try:
                        fielddict.update(
                            **{attr: flatten(getattr(self, attr))}
                        )
                    except Exception as err:
                        bail(err)
                else:
                    fielddict.update(
                        **{attr: getattr(self, attr)}
                    )
        return fielddict