import datetime
import re

from libdlg.dlgRecords import DLGRecords
from libdlg.dlgRecord import DLGRecord
from libdlg.dlgStore import Lst, Storage, objectify
from libdlg.dlgError import *
from libdlg.dlgUtilities import (
    bail,
    Flatten,
    noneempty,
    serializable,
    reg_valid_table_field,
    reg_default,
    reg_ipython_builtin
)
from libdlg.dlgControl import DLGControl
from libdlg.dlgQuery_and_operators import *

'''  [$File: //dev/p4dlg/libpy4/py4Sqltypes.py $] [$Change: 479 $] [$Revision: #33 $]
     [$DateTime: 2024/09/20 07:42:22 $]
     [$Author: mart $]
'''

__all__ = ['Py4Table', 'Py4Field']

class Py4Table(object):
    ''' Usage:

                >>> oCmd = Py4Table('change')

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

    def __init__(self, objp4, *args, **tabledata):
        self.objp4 = objp4
        (args, tabledata) = (Lst(args), Storage(tabledata))
        args0_error = 'Py4Table args[0] must be str'
        args1_error = 'Py4Table args[1] must be Py4Run object'
        if (not isinstance(args(0), str)):
            bail(args0_error)
        if (type(args(1)).__name__ != 'Py4Run'):
            bail(args1_error)
        (
            self.tablename,
            self.oP4Run
        ) = \
            (
                args(0).lower(),
                args(1)
            )

        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.objp4.logger \
                        if (hasattr(self.objp4, 'logger')) \
                        else tabledata.logger or 'INFO',
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
        )
        ]

        ''' p4table attributes
        '''
        valid_tablenames = (self.objp4.tablenames + self.objp4.nocommands + self.objp4.p4spec)
        [setattr(self, cmdattribute, tabledata[cmdattribute]) for cmdattribute in tabledata]
        if AND(
                (len( self.objp4.tablenames) > 0),
                (not self.tablename in valid_tablenames)
        ):
            tablename_error = f' Invalid tablename `{self.tablename}`.\n'
            self.logerror(tablename_error)
            bail(tablename_error)

        self.oSchema = self.objp4.oSchema
        self.referenced_by = []
        (
            self.fieldsmap,
            self.typemap,
            self.fieldtypesmap
        ) = \
            (
                Storage(),
                Storage(),
                Storage()
            )
        self._reference_by = None

        ''' p4table attributes
        '''
        self.p4schema = self.oSchema.p4schema or Storage()
        self.schemaversion = self.p4schema.version
        self.p4model = self.oSchema.p4model or Storage()
        self.modeltable = self.p4model[self.tablename] or Storage()
        self.modelfields = self.modeltable.fields \
            if (self.modeltable.fields is not None) \
            else Lst()
        ''' p4fields 
        '''
        self.fields = Lst()
        if (len(self.fieldnames) > 0):
            if (len(self.fieldsmap) == 0):
                for fieldname in self.fieldnames:
                    self.fieldsmap.merge({fieldname.lower(): fieldname})
                    try:
                        oField = Py4Field(
                            fieldname,
                            tablename=self.tablename,
                            table=self,
                            objp4=self.objp4
                        )
                        setattr(self, fieldname, oField)
                        self.fields.append(getattr(self, fieldname))
                    except Exception as err:
                        bail(err)

    __setitem__ = lambda self, key, value: setattr(self, str(key), value)
    __name__ = lambda self: self.tablename
    __delitem__ = object.__delattr__
    __contains__ = lambda self, key: key in self.fieldnames
    __nonzero__ = lambda self: (len(self.__dict__) > 0)
    items = lambda self: self.__dict__.items()
    values = lambda self: self.__dict__.values()
    __iter__ = lambda self: self.__dict__.__iter__()
    iteritems = lambda self: self.__dict__.iteritems()
    __str__ = lambda self: '<Py4Table {}>'.format(self.tablename)

    def __call__(self, *args, **kwargs):
        args = Lst(str(arg) for arg in args)
        ''' run the thing!
        '''
        p4ret = getattr(self.oP4Run, '__call__')(*args, **kwargs)
        ''' catch error in str output with an arbitrary error message
        '''
        if (isinstance(p4ret, str)):
            if (re.match(r'^Perforce\sclient\serror:\n', p4ret) is not None):
                bail(p4ret)
        ''' expect a Storage... convert Flatten to Storage or grab index 0 if ListType 
        '''
        if (isinstance(p4ret, Flatten)):
            p4ret = Storage(p4ret)
        ''' Whatever the case, p4ret should end up being a Storage at this point!
        '''
        if (isinstance(p4ret, (Storage, DLGRecord))):
            try:
                if AND(
                        AND(
                            (p4ret.code == 'error'),
                            (p4ret.data is not None)
                        ),
                        (p4ret.severity is not None)
                ):
                    bail(p4ret.data)
            except Exception as err:
                bail(err)
            ''' got something!!!
            '''
            if (p4ret.data is not None):
                p4ret = p4ret.data
        if (isinstance(p4ret, Lst)):
            p4ret = DLGRecords(p4ret, cols=p4ret(0).getkeys(), objp4=self)
        return p4ret

    ''' p4 keyed tables... get a table's `keying` attributes
    '''
    def keying_fields(self, tablename):
        tables = self.oSchema.p4schema.table.table
        if (re.match(r'^db\.', tablename) is None):
            tablename = f"db.{tablename}"
        dtable = Lst(
            filter(
                lambda tbl: tbl.name == tablename, tables
            )
        )(0) or Storage()
        try:
            return re.split(r',\s', dtable.keying)
        except:
            return Lst()

    ''' same as getfield()
    '''
    def table_attributes(self):
        tablename = self.tablename
        tableattributes = self.oSchema.p4model[tablename]
        if (noneempty(tableattributes) is True):
            if (tablename in (
                              'depots',
                              'clients',
                              'labels',
                              'branches',
                              'streams',
                              'typemap'
                            )
            ):
                tableattributes = self.oSchema.p4model.domain
        return tableattributes

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

                >>> oJnl.rev.fields()               --> no args, returns list of all fields for for this table
                [{'comment': '',
                  'default': '',
                  'desc': 'DLGRecord number as read in',
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
                  ...   ]
        '''
        allfields = self.fieldnames
        if (name is None):
            return allfields if (len(allfields) > 0) else Storage()
        if (name.lower() in self.fieldsmap.keys()):
            return Lst(
                filter(
                    lambda field: field.lower() == str(name).lower(),
                    allfields
                )
            )(0)

    def truncate(self):
        [
            delattr(self, fname) for fname in self.fieldnames
        ]
        delattr(self, self.tablename)
        setattr(self, self.tablename, Lst())

    def get(self, name):
        return self.getfield(name=name)

    def __getitem__(self, key):
        return self.__getattr__(str(key))

    def __getattr__(self, key):
        key = str(key)
        key_dismiss_attr = [
            item for item in (
                                (reg_ipython_builtin.match(key)),
                                (re.match(r'^_.*$', str(key))),
                                (re.match(r'^shape$', key))
            ) if (
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

        if (self.tablename is not None):
            if (self.fieldsmap[key.lower()] is None):
                raise FieldNotBelongToTableError(self.tablename, key)
            if (key.lower() in self.fieldsmap):
                key = self.fieldsmap[key]
            if (key in self.fieldnames):
                ''' is it a table attribute, or a field attribute, something else?
                    and, more importantly, can we get the actual field value
                    from fieldsmap?
                '''
                if AND(
                        (len(self.fieldsmap) == 0),
                        (len(self.fieldnames) > 0)
                ):
                    self.fieldsmap = Storage({k.lower(): k for k in self.fieldnames})
                fieldkey = self.fieldsmap.get(key.lower())
                if (fieldkey is None):
                    bail(
                        f"p4field '{key}' does not belong to p4table '{self.tablename}'\n"
                    )
                key = re.sub(r"^[pP]4", '', fieldkey)
                try:
                    if (hasattr(self, key)):
                        return self.__dict__[key]
                    elif (key == self.getfield(key)):
                        setattr(
                            self,
                            key,
                            Py4Field(
                                key,
                                self.tablename,
                                self.objp4,
                                self.oSchema
                            )
                        )
                        return self.__dict__[key]
                except KeyError:
                    raise AttributeError
            else:
                keyname = self.fieldsmap[key.lower()]
                if (keyname is not None):
                    return getattr(self, keyname)
                else:
                    self.logerror(f'[{key}]: Invalid field for table `{self.tablename}`')

        else:
            return Py4Field()

    '''
                    >>> qry = objp4.clients.Client == 'myClientName'
                    >>> qry = qry.as_storage()
                    >>> pprint(qry)
                        { "op": "NE",
                          "left":{
                                "tablename": "clients",
                                "fieldname": "Client"
                                },
                          "right":'gc.pycharm'}
                    >>> clientrecord = objp4(qry).fetch()
                    >>> pprint(clientrecord)
                    {'Client': 'gc.pycharm',
                     'Description': 'Created by mart.\n',
                     'Host': 'computer.local',
                     'LineEnd': 'local',
                     'Options': 'noallwrite noclobber nocompress unlocked nomodtime normdir',
                     'Owner': 'mart',
                     'Root': '/Users/gc/pycharmprojects/sQuery',
                     'SubmitOptions': 'submitunchanged',
                     'View': ['//depotmart2/... //pc.pycharm/depotmart2/...',
                              '//depotmart/... //pc.pycharm/depotmart/...',
                              '//depot/... //pc.pycharm/depot/...'],
                     'code': 'stat'}
    '''

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
                                    if (isinstance(value, Py4Table) is True) \
                                    else value
                elif (type(value).__name__ in ('JNLField', 'Py4Field')):
                    dgen[key] = value.as_dict()
                elif (isinstance(value, serializable)):
                    if (key not in EXCLUDE):
                        dgen[key] = mergeKeyValue(value) \
                                        if (isinstance(value, dict) is True) \
                                        else value
            return dgen
        return objectify(mergeKeyValue(self.__dict__))

class Py4Field(DLGExpression):

    __str__ = __repr__ = lambda self: f"<Py4Field {self.fieldname}>"

    def update_instance(self, op=None, left=None, right=None, **kwargs):
        '''  attributes for operators instead of DLGQuery class reference?
        '''
        for argitem in (op, left, right):
            if (argitem is not None):
                argname = argitem.__name__
                setattr(self, argname, argitem)
        [setattr(self,k, v) for (k, v) in kwargs.items() if (len(kwargs) > 0)]

    def __len__(self):
        return lambda i: len(self.__dict__[i])


    def __init__(
                 self,
                 fieldname,
                 tablename=None,
                 table=None,
                 objp4=Storage(),
                 oSchema=Storage(),
                 default=lambda: None,
                 type='string',
                 length=None,
                 required=False,
                 label=None,
                 comment=None,
                 writable=True,
                 readable=True,
                 regex=None,
                 options=None,
                 compute=None,
                 filter_in=None,
                 filter_out=None,
                 _rname=None,
                 **kwargs
                ):
        kwargs = Storage(kwargs)
        self.fieldname = fieldname
        self.name = fieldname

        self.__dict__ = objectify(self.__dict__)

        if OR(
                (not isinstance(fieldname, str)),
                (reg_valid_table_field.match(fieldname) is None)
        ):
            error = f'Invalid field name `{fieldname}`'
            objp4.logerror(error)
            bail(error)
        self.table = table
        self.objp4 = objp4

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

        self.oSchema = oSchema or objp4.oSchema
        self.tablename = tablename
        self.op = None
        self.left = None
        self.right = None
        self.required = required
        self.label = label
        self.comment = comment
        self.writable = writable
        self.readable = readable
        self.options = options
        self.compute = compute
        self.filter_in = filter_in
        self.filter_out = filter_out
        self._rname = _rname or fieldname
        self.type = type \
            if (not isinstance(type, (Py4Table, Py4Field))) \
            else 'string'
        self.length = length or len(fieldname)
        self.default = default
        self.regex = regex
        if AND(
                (regex is None),
                (isinstance(self.type, str))
        ):
            self.regex = reg_default.get(self.type.split('(')[0])
        self.label = label or fieldname
        [setattr(self, key, kwargs[key]) for key in kwargs]

        super(Py4Field, self).__init__(
            objp4,
            None,
            self,
            None,
            fieldname=fieldname,
            tablename=tablename
        )


    def __bool__(self):
        return True

    def __hash__(self, key='id'):
        try:
            return (self.__dict__[key])
        except Exception as err:
            self.logwarning(err)

    keys = lambda self: Storage(self.__dict__).getkeys()

    __get__ = lambda self: self.__getitem__

    def __call__(self, *args, **kwargs):
        return self

    def as_dict(self, flat=False, sanitize=True):
        attrs = (
            'fieldname',
            'tablename',
            'op',
            'left',
            'right',
            'length',
            'type',
            'required',
            'label',
            'comment',
            'writable',
            'readable',
            'regex',
            'options',
            'compute',
            'filter_in',
            'filter_out',
            '_rname'
        )

        def flatten(obj):
            if (isinstance(obj, dict) is True):
                return dict((flatten(k), flatten(v)) for k, v in obj.items())
            elif (isinstance(obj, (tuple, list, set)) is True):
                return [flatten(v) for v in obj]
            elif (isinstance(obj, serializable) is True):
                return obj
            elif (isinstance(
                    obj,
                    (
                            datetime.datetime,
                            datetime.date,
                            datetime.time
                    ) is True
                )
            ):
                return str(obj)
            else:
                return obj

        fielddict = Storage()
        if not (sanitize and not (self.readable or self.writable)):
            for attr in attrs:
                flattened = {attr: flatten(getattr(self, attr))} \
                    if (flat is True) \
                    else {attr: getattr(self, attr)}
                fielddict.update(**flattened)
        return fielddict