import re
import datetime

from libdlg.dlgStore import (
    Storage,
    objectify,
    Lst,
)
from libsql.sqlQuery import *
from libsql.sqlRecords import Records
from libdlg.dlgUtilities import (
    reg_ipython_builtin,
    serializable,
    reg_valid_table_field,
    bail
)

'''  [$File: //dev/p4dlg/libno/noSqltypes.py $] [$Change: 683 $] [$Revision: #9 $]
     [$DateTime: 2025/04/07 18:39:56 $]
     [$Author: mart $]
'''

__all__ = ['NOTable', 'NOField']

class NOTable(object):
    ''' Usage:

            >>> oTable = NOTable(name)
    '''

    def __init__(
            self,
            objp4,
            tablename='notable',
            *records,
            **tabledata
    ):
        (records, tabledata) = \
            (Lst(records), Storage(tabledata))
        self.objp4 = objp4
        self.tablename = tablename
        (
            self.baseRecords,
            self.fieldnames,
            self.fieldsmap,
            self.fieldtypesmap
        ) = \
            (
                records or Lst(),
                tabledata.fieldnames or Lst(),
                tabledata.fieldsmap or Storage(),
                tabledata.fieldtypesmap or Storage()
            )

        ''' logger
        '''
        self.oLogger = self.objp4.oLogger
        self.criticallogger = self.objp4.criticallogger
        self.errorlogger = self.objp4.errorlogger
        self.warninglogger = self.objp4.warninglogger
        self.infologger = self.objp4.infologger
        self.LOGCRITICAL = self.objp4.LOGCRITICAL
        self.LOGERROR = self.objp4.LOGERROR
        self.LOGWARNING = self.objp4.LOGWARNING
        self.LOGINFO = self.objp4.LOGINFO
        ''' end logger

            maps and field names & references
        '''
        for field in self.fieldnames:
            oField = NOField(fieldname=field.name,
                             tablename=self.tablename,
                             objp4=objp4)
            setattr(self, field.name, oField)

    __setitem__ = lambda self, key, value: setattr(self, str(key), value)
    __name__ = lambda self: self.oSchema.p4model[self.tablename].name
    __delitem__ = object.__delattr__
    __contains__ = lambda self, key: key in self.__dict__
    __nonzero__ = lambda self: (len(self.__dict__) > 0)
    keys = lambda self: self.fieldnames
    items = lambda self: self.__dict__.items()
    values = lambda self: self.__dict__.values()
    __iter__ = lambda self: self.__dict__.__iter__()
    iteritems = lambda self: self.__dict__.iteritems()
    __str__ = lambda self: f'<NOTable {self.tablename}>'

    def __call__(self, *args, **kwargs):
        return self

    def as_dict(self, exclude=None):
        EXCLUDE = exclude or Lst()
        def mergeKeyValue(qdict):
            dgen = {}
            for (key, value) in qdict.items():
                if key in ("left", "right"):
                    if isinstance(value, NOTable):
                        dgen[key] = mergeKeyValue(self.__class__)
                    else:
                        dgen[key] = value
                elif (type(value).__name__ == 'NOField'):
                    dgen[key] = value.as_dict()
                elif (isinstance(value, serializable)):
                    if (key not in EXCLUDE):
                        dgen[key] = mergeKeyValue(value) \
                            if (isinstance(value, dict)) \
                            else value
            return dgen
        return objectify(mergeKeyValue(self.__dict__))

    def __getattr__(self, key):
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
            return

        if (
                ('tablename' in self.__dict__.keys()) &
                (not 'fieldname' in self.__dict__.keys())
        ):
            if (self.tablename is None):
                return NOField()


    def insert(self, *args, **kwargs):
        kwargs = Storage(kwargs)
        records = self.iterQuery(*args, **kwargs)
        records = Records(records=records, cols=self.oQuery.cols, objp4=self.objp4)
        records = self.modify_records(records, **kwargs)
        self.oQuery.query = Lst()
        return records

    def update_record(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        ''' TODO: Implement '''

    def delete_record(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        ''' TODO: Implement '''

    def update(self, *args, **kwargs):
        kwargs = Storage(kwargs)
        records = self.oQuery.iterQuery(*args, **kwargs)
        records = Records(records=records, cols=self.oQuery.cols, objP4=self.objP4)
        records = self.filter_records(records, **kwargs)
        self.oQuery.query = Lst()
        return records

    def delete(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        records = self.oQuery.iterQuery(*args, **kwargs)
        records = Records(records=records, cols=self.oQuery.cols, objP4=self.objP4)
        records = self.filter_records(records, **kwargs)
        self.oQuery.query = Lst()
        return records

    def insert(self, *args, **kwargs):
        kwargs = Storage(kwargs)
        records = self.oQuery.iterQuery(*args, **kwargs)
        records = Records(records=records, cols=self.oQuery.cols, objP4=self.objP4)
        records = self.filter_records(records, **kwargs)
        self.oQuery.query = Lst()
        return records

    def fetch(self, *fields, **kwargs):
        kwargs = Storage(kwargs)
        fields = self.define_fields(*fields, **kwargs)
        record = self.oQuery.fetch(*fields, **kwargs)
        self.oQuery.query = Lst()
        return record


class NOField(DLGExpression):
    __str__ = __repr__ = lambda self: f"<NOField {self.fieldname}>"

    def update_instance(self, op=None, left=None, right=None, **kwargs):
        '''  attributes for operators instead of DLGQuery class reference?
        '''
        for argitem in (op, left, right):
            if (argitem is not None):
                argname = argitem.__name__
                setattr(self, argname, argitem)
        [setattr(self, k, v) for (k, v) in kwargs.items() if (len(kwargs) > 0)]

    __hash__ = lambda self: hash((frozenset(self), frozenset(self.itervalues())))

    def containschars(self):
        return self.__contains__(self)

    def __len__(self):
        return lambda i: len(self.__dict__[i])

    def __init__(self,
                 fieldname,
                 tablename='notable',
                 objp4=Storage(),
                 required=False,
                 writable=True,
                 readable=True,
                 filter_in=None,
                 filter_out=None,
                 _rname=None,
                 **kwargs):

        super(NOField, self).__init__(objp4, None, self, )
        self.__dict__ = objectify(self.__dict__)
        if (
                (not isinstance(fieldname, str)) |
                (reg_valid_table_field.match(fieldname) is None)
        ):
            error = f'Invalid field name `{fieldname}`'
            objp4.LOGERROR(error)
            bail(error)
        self.fieldname = fieldname
        self._rname = _rname or fieldname
        self.tablename = self.table = tablename
        self.objp4 = objp4
        self.name = fieldname
        self.desc = ''
        self.op = None
        self.left = None
        self.right = None
        self.required = required
        self.writable = writable
        self.readable = readable
        self.filter_in = filter_in
        self.filter_out = filter_out
        [setattr(self, key, kwargs[key]) for key in kwargs]

    keys = lambda self: self.fieldnames
    attributes = lambda self: self.__dict__.keys()

    def __call__(self, *args, **kwargs):
        return self

    def as_dict(self, flat=False, sanitize=True):
        attrs = (
            'fieldname', 'tablename',
            'op', 'left', 'right',
            'writable', 'readable',
            '_rname'
        )

        def flatten(obj):
            if isinstance(obj, dict):
                return dict((flatten(k), flatten(v)) for k, v in obj.items())
            elif isinstance(obj, (tuple, list, set)):
                return [flatten(v) for v in obj]
            elif isinstance(obj, serializable):
                return obj
            elif isinstance(obj, (
                    datetime.datetime,
                    datetime.date,
                    datetime.time
            )
                            ):
                return str(obj)
            else:
                return None

        fielddict = Storage()
        if not (sanitize and not (self.readable or self.writable)):
            for attr in attrs:
                if (flat is True):
                    fielddict.update(**{attr: flatten(getattr(self, attr))})
                else:
                    fielddict.update(**{attr: getattr(self, attr)})
        return fielddict