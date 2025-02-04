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
from libdlg.dlgJoin import DLGJoin
from libdlg.dlgUtilities import (
    reg_ipython_builtin,
    serializable,
    reg_valid_table_field,
    bail,
    noneempty
)

__all__ = ['JNLTable', 'JNLField']

'''  [$File: //dev/p4dlg/libjnl/jnlSqltypes.py $] [$Change: 479 $] [$Revision: #17 $]
     [$DateTime: 2024/09/20 07:42:22 $]
     [$Author: mart $]
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
        self.tablename = self._name = tablename
        self.oSchema = oSchema or self.objp4.oSchema
        self.oSchemaType = SchemaType(self.objp4)
        self.jnlRecords = Lst()
        self.inversion = tabledata.inversion or False
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

        self.rtype = tabledata.type
        self.version = tabledata.version
        self.classic_lockseq = tabledata.classic_lockseq
        self.peek_lockseq = tabledata.peek_lockseq
        self.keying = tabledata.keying
        self.desc = tabledata.desc
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
        fields = Lst()
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
            (is_flag, is_bitmask, datatype) = (False, False, Storage())
            if (not field.name in ('id', 'idx', 'db_action', 'table_revision', 'table_name')):
                datatype = self.oSchemaType.datatype_byname(field.type)
                if (noneempty(datatype) is False):
                    datatype = datatype.name
                is_flag = self.oSchemaType.is_flag(field.type)
                is_bitmask = self.oSchemaType.is_bitmask(field.type)
            oField = JNLField(
                                field,
                                objp4=objp4,
                                oSchema=self.oSchema,
                                oSchemaType=self.oSchemaType,
                                _table=self,
                                datatype=datatype,
                                is_flag=is_flag,
                                is_bitmask=is_bitmask,
            )
            setattr(self, field.name, oField)
            fields.append(getattr(self, field.name))
        self.fields = objectify(fields)
        self.ALL = self.fields

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
        #for field in fields:
        #    fieldatts = self.getfield(field)
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
                elif (is_fieldType(value) is True):
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

    def set_attributes(self, *args, **attributes):
        self.__dict__.update(*args, **attributes)
        return self

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

    def on(self, reference, flat=False):
        return DLGJoin(self.objp4, reference, flat=flat)

class JNLField(DLGExpression):
    __str__ = __repr__ = lambda self: f"<JNLField {self.fieldname}>"
    __hash__ = lambda self: hash((frozenset(self), frozenset(self.objp4)))
    def __len__(self): return lambda i: len(self.__dict__[i])

    def containschars(self):
        return self.__contains__(self)

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
                 _table=None,
                 **kwargs
    ):
        self.fieldname = self._name = fieldname = field.fieldname \
            if (hasattr(field, 'fieldname')) \
            else field
        self.tablename = tablename = field.tablename
        self._table = _table or objp4[tablename]

        self._rname = _rname
        self.oSchema = oSchema
        self.oSchemaType = oSchemaType
        self.objp4 = objp4

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

    def set_attributes(self, *args, **attributes):
        self.__dict__.update(*args, **attributes)
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

    def masks(self):
        ''

    def flags(self):
        ''


    def as_dict(self, flat=False, sanitize=True):
        attrs = (
            'fieldname', '_rname', '_table', 'tablename',
            'op', 'left', 'right',
            'type', 'required',
            'label', 'comment',
            'writable', 'readable',
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
                    NOT(
                        OR(
                            (self.readable is True),
                            (self.writable is True)
                        )
                    )
                )
        ):
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