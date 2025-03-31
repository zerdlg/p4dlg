import decimal
from datetime import date, datetime, time

from libdlg.dlgStore import ZDict, Lst
from libdlg.dlgUtilities import (
    reg_objdict,
    bail,
    Flatten,
    ALLLOWER,
    annoying_ipython_attributes
)
from libdlg.dlgDateTime import DLGDateTime
from libdlg.dlgTables import DataTable

'''  [$File: //dev/p4dlg/libsql/sqlRecord.py $] [$Change: 652 $] [$Revision: #3 $]
     [$DateTime: 2025/03/23 04:15:28 $]
     [$Author: zerdlg $]
'''

__all__ = ['Record']

from pprint import pformat

class Record(ZDict):

    __str__ = __repr__ = lambda self: f'<Record {pformat(self.as_dict())}>'

    def _fieldnames(self):
        return self.getkeys()

    def _fieldsmap(self):
        return ZDict(zip(ALLLOWER(self._fieldnames()), self._fieldnames()))

    def __setattr__(self, item, value):
        if (str(item).lower() in self._fieldsmap()):
            item = self._fieldsmap()[str(item).lower()]
        self[item] = value

    def __getitem__(self, item, other=None):
        if (str(item).lower() in self._fieldsmap()):
            item = self._fieldsmap()[str(item).lower()]
        record = self.as_dict()
        if (
                (isinstance(item, dict))
                and (isinstance(other, str))
        ):
            try:
                return getattr(self, item[other]) \
                    if (other is not None) \
                    else item.other \
                    if (hasattr(item, 'other')) \
                    else None
            except (
                    KeyError,
                    AttributeError,
                    TypeError
            ) as err:
                pass
        if (item in record.getkeys()):
            try:
                return record[item]
            except (
                    KeyError,
                    AttributeError,
                    TypeError
            ) as err:
                pass
            oMatch = reg_objdict.match(item)
            if (oMatch is not None):
                try:
                    return getattr(self, oMatch.group(1))[oMatch.group(2)]
                except (
                        KeyError,
                        AttributeError,
                        TypeError
                ):
                    raise KeyError
    __getattr__ = __getitem__
    has_key = has_field = lambda self, key: key in self.__dict__
    __nonzero__ = lambda self: len(self.__dict__) > 0
    __copy__ = lambda self: Record(self)
    fieldnames = lambda self: self._fieldnames()

    def as_dict(self, datetime_tostr=False):
        oDate = DLGDateTime()
        serializabletypes = [
            str,
            int,
            float,
            bool,
            list,
            dict
        ]
        dttypes = (
            date,
            datetime,
            time
        )
        record = ZDict(self.copy())
        for field in record.getkeys():
            try:
                fieldvalue = record[field]
                if record[field] is None:
                    continue
                elif (isinstance(fieldvalue, Record) is True):
                    record[field] = fieldvalue.as_dict()
                elif (isinstance(fieldvalue, decimal.Decimal) is True):
                    record[field] = float(fieldvalue)
                elif (isinstance(fieldvalue, dttypes) is True):
                    if datetime_tostr:
                        record[field] = oDate.to_string(fieldvalue)
                elif not isinstance(fieldvalue, tuple(serializabletypes)):
                    del record[field]
            except TypeError as err:
                bail(err, self.criticallogger)
        return record

    def as_table(self, *args, **kwargs):
        table = DataTable(self)
        dtable = table(*args, **kwargs)
        print(dtable)

    def reduce(self):
        return Flatten(**self.as_dict()).reduce()

    def expand(self):
        return Flatten(**self.as_dict()).expand()

    ''' rename fieldname to new fieldname
    '''
    def rename(self, oldfield, newfield, record=None):
        if (record is None):
            record = self
        if (record[oldfield] is not None):
            record[newfield] = record.pop(oldfield)
        return record

    ''' delete fields
    '''
    def delete(self, *fields):
        for field in fields:
            field = field.fieldname \
                if (type(field) in ('JNLField', 'Py4Field')) \
                else str(field)
            if (str(field).lower() in self._fieldsmap()):
                field = self._fieldsmap()[str(field).lower()]
            if (field in self.getkeys()):
                try:
                    self.__delitem__(field)
                except KeyError as err:
                    print(f'no such field... {field}')

    def update(self, *args, **kwargs):
        kwargkeys = kwargs.copy().keys()
        for key in kwargkeys:
            nkey = key
            if (str(key).lower() in self._fieldsmap()):
                nkey = self._fieldsmap()[str(key).lower()]
            if (key != nkey):
                kwargs = self.rename(key, nkey, kwargs)
        super(Record, self).update(*args, **kwargs)

    ''' keys, values, items
    '''
    def getkeys(self):
        return Lst(self.keys())

    def getfields(self):
        return self.getkeys()

    def getvalues(self):
        return Lst(self.values())

    def getitems(self):
        return Lst(self.items())

    ''' iterkeys, itervalues, iteritems
    '''

    def iterkeys(self):
        return iter(self.keys())

    def iterfields(self):
        return self.iterkeys()

    def itervalues(self):
        return iter(self.values())

    def iteritems(self):
        return iter(self.items())

    ''' 
                a safe & non-destructive record.update()

                overwrite - add_missing - allow_nonevalue  - extend_listvalues
    '''
    def merge(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), ZDict(kwargs))
        any = args.storageindex(reversed=True) \
            if (len(args) > 0) \
            else Lst([kwargs]).storageindex(reversed=True) \
            if (len(kwargs) > 0) \
            else Lst().storageindex(reversed=True)

        overwrite = kwargs.overwrite or True
        add_missing = kwargs.add_missing or True
        allow_nonevalue = kwargs.allow_nonevalue or True
        extend_listvalues = kwargs.extend_listvalues or True

        def mergedict(anydict):
            def updater(key, value):
                gkeys = self.getkeys()
                if (key in gkeys):
                    if (
                            (value is None)
                            and (allow_nonevalue is False)
                    ):
                        self.delete(key)
                    elif (
                            (overwrite is True) \
                            and (
                                    (value is not None)
                                    or (allow_nonevalue is True)
                            )
                    ):
                        self[key] = value
                elif (add_missing is True):
                    if (
                            (value is not None) \
                            or (allow_nonevalue is True)
                    ):
                        self[key] = value

            for key in anydict.getkeys():
                value = anydict[key]
                if (isinstance(value, list)):
                    if (
                            (isinstance(self[key], list)) \
                            and (extend_listvalues is True)
                    ):
                        value = Lst(value + self[key])
                if (
                        (value is not None) \
                        or (allow_nonevalue is False)
                ):
                    updater(key, value)

        for anyitem in any:
            mergedict(self.objectify(any[anyitem]) \
                          if (isinstance(any[anyitem], ZDict) is False) \
                          else any[anyitem])
        return self