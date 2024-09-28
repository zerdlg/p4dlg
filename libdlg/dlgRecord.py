import decimal
from datetime import date, datetime, time

from libdlg.dlgStore import Storage, Lst
from libdlg.dlgUtilities import reg_objdict, bail, Flatten, ALLLOWER
from libdlg.dlgDateTime import DLGDateTime
from libdlg.dlgTables import DataTable

'''  [$File: //dev/p4dlg/libdlg/dlgRecord.py $] [$Change: 474 $] [$Revision: #11 $]
     [$DateTime: 2024/09/09 06:39:06 $]
     [$Author: mart $]
'''

__all__ = ['DLGRecord']

from pprint import pformat

class DLGRecord(Storage):

    __str__ = __repr__ = lambda self: f'<DLGRecord {pformat(self.as_dict())}>'

    def _fieldnames(self):
        return self.getkeys()

    def _fieldsmap(self):
        return Storage(zip(ALLLOWER(self._fieldnames()), self._fieldnames()))

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
    __copy__ = lambda self: DLGRecord(self)
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
        record = Storage(self.copy())
        for field in record.getkeys():
            try:
                fieldvalue = record[field]
                if record[field] is None:
                    continue
                elif (isinstance(fieldvalue, DLGRecord) is True):
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

    def get(self, key, default=None):
        dmap = Storage({
            str(key).lower(): str(key) for key in self.getkeys()
        })
        key = dmap[str(key).lower()] \
            if (str(key).lower() in dmap.getkeys()) \
            else None
        try:
            return self.__getitem__(key)
        except(
                KeyError,
                AttributeError,
                TypeError
        ):
            return default

    def reduce(self):
        return Flatten(**self.as_dict()).reduce()

    def expand(self):
        return Flatten(**self.as_dict()).expand()

    ''' rename fieldname to new fieldname
    '''
    def rename(self, oldfield, newfield):
        if (self[oldfield] is not None):
            self[newfield] = self.pop(oldfield)
        return self

    ''' delete fields
    '''
    def delete(self, *fields):
        for field in fields:
            if (field in self.getfields()):
                try:
                    self.__delitem__(field)
                except KeyError as err:
                    print(f'no such field... {field}')

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
        (args, kwargs) = (Lst(args), Storage(kwargs))
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
                          if (isinstance(any[anyitem], Storage) is False) \
                          else any[anyitem])
        return self