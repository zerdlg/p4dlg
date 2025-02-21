import re
from datetime import datetime, date, time
from time import mktime, strptime

from libdlg.dlgStore import Lst, ZDict
from libdlg.dlgUtilities import (
    noneempty,
    reg_datetime_fieldname
)

'''  [$File: //dev/p4dlg/libdlg/dlgDateTime.py $] [$Change: 609 $] [$Revision: #13 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
'''

''' USAGE:

    >>> oDateTime = DLGDateTime()
    
    Guess
    >>> oDateTime.guess('2019/07/26') 
    datetime.date(2019, 7, 26)
    >>> oDateTime.guess('16:30.45')
    datetime.time(16, 30, 45)
    >>> oDateTime.guess('2019/07/26 16:30:45')
    datetime.datetime(2019, 7, 26, 16, 30, 45)
    
    Epoch
    >>> oDateTime.to_epoch('2019/09/03')
    1567468800.0
    >>> d = oDateTime.guess('2019/09/03')
    >>> oDateTime.to_epoch(d)
    1567468800.0
    >>> oDateTime.to_epoch(2019, 9, 3)
    1567468800.0
    >>> oDateTime.to_epoch([2019, 9, 3])
    1567468800.0
    >>> oDateTime.to_epoch(*[2019, 9, 3])
    1567468800.0
'''

__all__ = ['DLGDateTime', 'DLGDateTimeConvert']

reg_time = re.compile('([^0-9 ]+(?P<m>[0-9 ]+))?([^0-9ap ]+(?P<s>[0-9]*))?')
reg_epochtime = re.compile(r'^\d*(\.\d+)?$')
reg_split_datetime = re.compile(r'[/-]|:|\\|\s|,')
reg_separator = re.compile(r'[/-]|:|\\|\s|,')
reg_p4sep = re.compile('/')
reg_isosep = re.compile('-')

class DLGDateTime(object):
    def __init__(
             self,
             sep='/',
             utc=False,
             **kwargs
    ):
        kwargs = ZDict(kwargs)
        (self.sep, self.utc) = (sep, utc)
        ''' date/time formats
        '''
        self.separator = '/'
        self.separators = Lst([
            '/',
            '-',
            ':',
            '.'
        ])
        self.years = Lst(['%y', '%Y'])
        self.months = Lst([
            '%m',
            '%b',
            '%B'
        ])
        self.day = '%d'
        self.hour = '%H:%M:%S'
        self.minute = '%H:%M'
        self.second = '%I:%M:%S%p'
        self.hour_min_sec = Lst([
            '%H:%M:%S',
            '%H:%M',
            '%I:%M:%S%p',
            '%I:%M%p'
        ])
        (
            self.datetimeFormat,
            self.dateFormat
        ) = \
                (
                    f'%Y{sep}%m{sep}%d %H:%M:%S',
                    f'%Y{sep}%m{sep}%d'
                )
        self.timeFormat = '%H:%M:%S'
        self.typeformat_mappings = {
                            datetime: self.datetimeFormat,
                            date: self.dateFormat,
                            time: self.timeFormat}

    def __call__(self, *args, **kwargs):
        return self

    def validate(self, value, func=None):
        try:
            fvalue = func(value)
            if ((noneempty(value), noneempty(fvalue)) == (False, True)):
                return False
        except Exception as err:
            return False
        return True

    def is_string(self, value):
        return len(value) in (0, len([i for i in value if (isinstance(i, str))]))

    def is_integer(self, value):
        return self.validate(value, lambda v: int(v) == float(v))

    def is_double(self, value):
        return self.validate(value, lambda v: float(v))

    def is_epoch(self, value):
        if (isinstance(value, (int, float))):
            return True
        elif (isinstance(value, str) is True):
            if (reg_epochtime.match(value) is not None):
                return True
        return False

    def is_date(self, value, fmt=None):
        if (isinstance(value, str)):
            dformat = fmt or self.dateFormat
            return self.validate(value, lambda v, f=dformat: strptime(v, f))
        elif (isinstance(value, date) is True):
            return True
        return False

    def is_time(self, value, fmt=None):
        if (isinstance(value, str)):
            tformat = fmt or self.timeFormat
            return self.validate(value, lambda v, f=tformat: strptime(v, f))
        elif (isinstance(value, time) is True):
            return True
        return False

    def is_datetime(self, value, fmt=None):
        if (isinstance(value, str)):
            dtformat = fmt or self.datetimeFormat
            return self.validate(value, lambda v, f=dtformat: strptime(v, f))
        elif (isinstance(value, datetime) is True):
            return True
        return False

    def guesstime(self, gvalue, typed=True):
        if (re.match(':', gvalue) is not None):
            for hms in self.hour_min_sec:
                if (self.is_time(gvalue, hms) is True):
                    try:
                        return f'time {hms}' \
                            if (typed is False) \
                            else time(*[int(i) for i in reg_separator.split(gvalue)])
                    except Exception as err:
                        print(err)

    def guessepoch(self, gvalue, typed=True):
        if (self.is_epoch(gvalue) is True):
            try:
                return f'float {gvalue}' \
                    if (typed is False) \
                    else float(gvalue)
            except Exception as err:
                print(err)

    def guessdate(self, *args, typed=True):
        args = Lst(args)
        (
            gvalue,
            sep,
            dtformat
        ) \
            = (
                args(0),
                args(1),
                args(2)
        )
        ''' is it date?
        '''
        if (self.is_date(gvalue, dtformat)):
            try:
                return f'date {dtformat}' \
                    if (typed is False) \
                    else date(*[int(i) for i in gvalue.split(sep)])
            except Exception as err:
                print(err)

    def guessdatetime(self, *args, typed=True):
        args = Lst(args)
        (
            gvalue,
            sep,
            datestamp
        ) \
            = (
                args(0),
                args(1),
                args(2)
        )
        ''' is it datetime?
        '''
        for hms in self.hour_min_sec:
            dtformat = sep.join(datestamp)
            dtstamp = f'{dtformat} {hms}'
            if (self.is_datetime(gvalue, dtstamp) is True):
                try:
                    return f'datetime {dtstamp}' \
                        if (typed is False) \
                        else datetime(*[int(i) for i in reg_separator.split(gvalue)])
                except Exception as err:
                    print(err)

    def guesstype(self, *args):
        result = self.guess(*args)
        if (result is not None):
            return type(result)

    def guess(self, *args, typed=True):
        ''' given a str, guess its type
        '''
        args = Lst(args)
        if (len(args) == 1):
            gvalue = args(0)
            if (
                    (isinstance(gvalue, list))
                    and (len(gvalue) >= 3)
            ):
                return self.guess_dt_from_ints(*gvalue)
            if (isinstance(gvalue, str)):
                gtime = self.guesstime(gvalue, typed=typed)
                etime = self.guessepoch(gvalue, typed=typed)
                if (etime is not None):
                    if (int(etime) < 1000):
                        etime = None
                for item in [
                    gtime,
                    etime
                ]:
                    if (item is not None):
                        return item
                for sep in self.separators:
                    if (sep in gvalue):
                        for month in self.months:
                            for year in self.years:
                                datestructures = (
                                    Lst(
                                        ['%d', month, year]
                                    ),
                                    Lst(
                                        [year, month, '%d']
                                    )
                                )
                                for datestamp in datestructures:
                                    for item in [
                                        self.guessdate(gvalue, sep, sep.join(datestamp), typed=typed),
                                        self.guessdatetime(gvalue, sep, datestamp, typed=typed),
                                        self.guessdate(gvalue, sep, f'{month}{sep}%d{sep}{year}', typed=typed),
                                        self.guessdatetime(gvalue, sep, datestamp, typed=typed)
                                    ]:
                                        if (item is not None):
                                            return item
            elif ((isinstance(gvalue, (int, float)) is True) \
                    and (self.is_epoch(gvalue) is True)):
                    return self.guessepoch(gvalue, typed=typed)
        if (len(args) >= 3):
            return self.guess_dt_from_ints(*args, typed=typed)

    def guess_dt_from_ints(self, *gargs, typed=True):
        ''' given a list of ints, guess it type
        '''
        gargs = Lst(gargs)
        if (len(gargs) == 1):
            if (isinstance(gargs(0), list)):
                return self.guess_dt_from_ints(*gargs(0))
        if (len(gargs) == 3):
            for dtype in (date, time):
                try:
                    dt = dtype(*gargs)
                    if (isinstance(dt, dtype)):
                        return dt \
                            if (typed is True) \
                            else self.to_string(dt)
                except ValueError:
                    pass
        elif (len(gargs) > 3):
            try:
                dt = datetime(*gargs)
                if (isinstance(dt, datetime)):
                    return dt \
                        if (typed is True) \
                        else self.to_string(dt)
            except ValueError:
                pass
    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    '''     convert datetime stuff to other datetime stuff                                       '''
    '''                                                                                          '''
    ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    def to_string(self, *args):
        ''' Takes a supported DT type & tries to convert it to a
            well formatted p4 str repsesentation

            USAGE:
                >>> oP4QDT = DLGDateTime
                >>> dt = oP4QDT.guess('2024/04/03 23:36:12')
                >>> oP4QDT.to_string('2024/04/03 23:36:12')
                '2024/04/03 23:36:12'
                >>> dt = ('23:36:12')
        '''
        def guess_dt(*gargs):
            dt = self.guess_dt_from_ints(*gargs)
            try:
                dtype = type(dt)
                return dt.strftime(self.typeformat_mappings[dtype])
            except ValueError as err:
                print(err)

        args = Lst(args)
        if (len(args) == 0):
            print('can not resolve date/time type :( ')
        if (len(args) == 1):
            arg = args(0)
            if (isinstance(arg, str) is True):
                return arg
            elif ((isinstance(arg, (int, float)) is True) \
                    and (self.is_epoch(arg) is True)):
                    return str(arg)
            elif (isinstance(arg, date) is True):
                return arg.strftime(self.dateFormat)
            elif (isinstance(arg, time) is True):
                return arg.strftime(self.timeFormat)
            elif (isinstance(arg, datetime) is True):
                return arg.strftime(self.datetimeFormat)
            elif (isinstance(arg, (list, tuple))):
                return guess_dt(*arg)
        elif (len(args) >= 3):
            return guess_dt(*args)

    def string_to_datetime(self, arg):
        ''' >>> oDT= DLGDateTime()
            >>> oDT.string_to_datetime('2019/08/29')
            datetime.datetime(2019, 8, 29, 0, 0)
            >>> oDT.string_to_datetime('2019 08 29')
            datetime.datetime(2019, 8, 29, 0, 0)
            >>> oDT.string_to_datetime('2019-08-29')
            datetime.datetime(2019, 8, 29, 0, 0)
            >>> oDT.string_to_datetime('2019:08:29')
            datetime.datetime(2019, 8, 29, 0, 0)
        '''
        if (reg_epochtime.match(arg) is not None):
            return self.to_datetime(int(arg))
        ints = [int(dateitem) for dateitem in \
                reg_split_datetime.split(arg)]
        return self.guess_dt_from_ints(*ints)

    def to_epoch(self, *args):
        '''  convert a p4 date ('2019/08/29')/datetime/tuple (y,m,d)/ list [y,m,d] to epoch

                >>> oP4QDT = DLGDateTime()
                >>> oConvert.to_epoch(datetime(2019, 8, 29, 0, 0))
                1567123200.0
                >>> oP4QDT.to_epoch('2019/08/29')
                1567123200.0
        '''
        def convert(arg):
            if (isinstance(arg, str) is True):
                arg = self.string_to_datetime(arg)
            if (self.utc is False):
                if (isinstance(arg, date) is True):
                    arg = datetime.combine(arg, datetime.min.time())
                if (isinstance(arg, datetime) is True):
                    return (arg - datetime(1970, 1, 1)).total_seconds()
            if (hasattr(arg, 'timetuple')):
                return mktime(arg.timetuple())

        if (len(args) == 1):
            arg = args[0]
            if (reg_epochtime.match(str(arg)) is not None):
                return arg
            if (isinstance(arg, (date)) is True):
                return convert(arg)
            elif (isinstance(arg, str) is True):
                return convert(datetime(*[int(dateitem) for \
                    dateitem in reg_split_datetime.split(arg)]))
            elif (isinstance(arg, (tuple, list)) is True):
                return convert(datetime(*arg).strftime(format=self.dateFormat))
        elif (len(args) >= 3):
            return convert(datetime(*args).strftime(format=self.dateFormat))

    def date_time_to_datetime(self, d, t):
        if ((self.is_date(d) is True) and (self.is_time(t) is True)):
            return datetime.combine(d, t)

    def date_to_datetime(self, *args):
        args = Lst(args)
        if (len(args) == 1):
            if (self.is_date(args(0)) is True):
                datetime.combine(args(0), time(0))
        elif (len(args) == 2):
            if ((self.is_date(args(0)) is True) & (self.is_time(args(1)) is True)):
                return datetime.combine(args(0), args(1))
        elif (len(args) > 2):
            dtargs = [int(dateitem) for dateitem in args[1:]]
            if (isinstance(args(0), date) is True):
                return datetime.combine(args(0), time(*dtargs))
            else:
                dtargs.insert(0, args(0))
                return datetime(*dtargs)

    def to_datetime(self, *args, **kwargs):
        ''' >>> oP4QDT = DLGDateTime()
            >>> oP4QDT.to_datetime(2019,8,19)
            datetime(2019,8,19,0,0)
            >>> oP4QDT.to_datetime(*[2019,8,19])
            datetime(2019,8,19,0,0)
            >>> oP4QDT.to_datetime("2019/8/19")
            datetime(2019,8,19,0,0)
            >>> oP4QDT.to_datetime("2019-8-19")
            datetime(2019,8,19,0,0)
            >>> oP4QDT.to_datetime("1547856000.0")
            datetime(2019,8,19,0,0)
            >>> oP4QDT.to_datetime(1547856000.0)
            datetime(2019,8,19,0,0)
            >>> oP4QDT.to_datetime(*[2019,6,9])
            datetime(2019,8,19,0,0)
        '''
        (args, kwargs) = (Lst(args), ZDict(kwargs))
        if (len(args) == 1):
            dt = args(0)
            if (isinstance(args(0), str) is True):
                dt = self.guess(args(0))
            if (self.is_date(dt) is True):
                return datetime.combine(dt, time(0))
            elif (self.is_datetime(dt) is True):
                return dt
            elif (self.is_time(dt) is True):
                print('can not convert datetime.time to datetime.datetime.')
            elif ((isinstance(dt, (tuple, list)) is True) and (len(dt) >= 3)):
                return datetime(*args)
            elif (isinstance(dt, (int, float)) is True):
                return datetime.fromtimestamp(float(dt))
        elif (len(args) > 2):
            return self.guess_dt_from_ints(*args)

    def to_p4date(self, *args, datetype='datetime' ,**kwargs):
        '''  The method's singular purpose is input of datetime
             data then output a p4 formatted date/time stamp (str)

             USAGE:

                 >>> oP4QDT = DLGDateTime()
                 >>> oP4QDT.to_p4date(2019,8,19)
                 '2019/8/1'
                 >>> oP4QDT.to_p4date('2019','8','19')
                 '2019/8/1'
                 >>> oP4QDT.to_p4date('2019,8,19')
                 '2019/8/1'
                 >>> oP4QDT.to_p4date('2019-8-19')
                 '2019/8/1'
                 >>> oP4QDT.to_p4date('2019 8 19')
                 '2019/8/1'
                 >>> oP4QDT.to_p4date('2019:8:19')
                 '2019/8/1'
                 >>> oP4QDT.to_p4date('2019:8,19')
                 '2019/8/1'
                 >>> oP4QDT.to_p4date(2019,8,19)
                 '2019/8/1'
                 >>> oP4QDT.to_p4date(*[2019,8,19])
                 '2019/8/1'
                 >>> oP4QDT.to_p4date('1547856000.0')
                 '2019/8/1'
                 >>> oP4QDT.to_p4date(1547856000.0)
                 '2019/8/1'
        '''
        args = Lst(args).clean()
        if (len(args) == 1):
            arg = args[0]
            if (type(arg) in (date, datetime, time)):
                if (datetype == 'date'):
                    return arg.strftime(format=self.dateFormat)
                elif (datetype == 'datetime'):
                    return arg.strftime(format=self.datetimeFormat)
                elif (datetype == 'time'):
                    return arg.strftime(format=self.timeFormat)
            elif (isinstance(arg, list) is True):
                return self.to_p4date(*arg, datetype=datetype)
            elif ((isinstance(arg, (int, float))) or
                  (reg_epochtime.match(arg) is not None)):
                dtime = datetime.fromtimestamp(float(arg))
                return self.to_p4date(dtime, datetype=datetype)
            elif (isinstance(arg, str)):
                dateitems = [int(dateitem) for dateitem in reg_split_datetime.split(arg)]
                return self.to_p4date(*dateitems, datetype=datetype)
        if (len(args) >= 3):
            for arg in args:
                if (isinstance(arg, str) is True):
                    iarg = int(arg)
                    idx = args.index(arg)
                    args.pop(idx)
                    args.insert(idx, iarg)
            dtime = datetime(*args)
            return self.to_p4date(dtime, datetype=datetype)

class DLGDateTimeConvert(object):
    def __init__(self, objp4):
        self.objp4 = objp4
        self.oDateTime = DLGDateTime()

    def __call__(self, record=None, tablename=None, datetype='datetime', **kwargs):
        fieldlist = record.getkeys()
        fieldnames = fieldlist.storageindex(reversed=True)
        fieldlist = Lst(fieldnames[fidx] \
                            if (isinstance(fvalue, str) is True) \
                            else fieldnames[fidx].fieldname for (fidx, fvalue)
                        in fieldnames.items())
        ''' convert epoch datetime stamps to an ISO standard (like p4' flavour).

            eg. '2024/09/17 00:00:00'
        '''
        datetime_fields = self.get_datetime_fields(tablename)
        if (len(datetime_fields) > 0):
            ''' some datetime fields are missing from the full fields list
                (eg. user has passed in a custom list fields list - let's 
                check those we have (if any)
            '''
            updateable_fields = datetime_fields \
                if (len(fieldlist.intersect(datetime_fields)) == len(datetime_fields)) \
                else [dtitem for dtitem in datetime_fields if (dtitem in fieldlist)]
            ''' re-define the record, yet again.

                the `datetype` kwarg value is 'datetime' which, you guessed it,
                has the effect of forcing p4dlg to output the full date/time 
                stamp. `datetype` accepted values are:
                    datetime    ->  '2024/09/17 00:00:00'
                    date        ->  '2024/09/17'
                    time        ->  '00:00:00'
            '''
            if (len(updateable_fields) > 0):
                record = self.update_datefields(record, updateable_fields, datetype=datetype)
        return record

    def update_datefields(self, record, fieldnames, datetype='datetime'):
        for name in fieldnames:
            try:
                if (not record[name] in ('', '0')):
                    dtstamp = self.oDateTime.to_p4date(
                        record[name],
                        datetype=datetype
                    )
                    record.merge({name: dtstamp})
            except Exception as err:
                print(f'Failed to convert Date/Time field ({name}) from epoch to datestamp.\n{err}')
        return record

    def get_datetime_fields(self, tablename):
        ''' promote fields that looks like date/time fields &
            them to p4 formatted date/time stamps (yyyy/mm/dd)
        '''
        datetime_fields = []
        table = getattr(self.objp4, tablename)
        for fld in table.fields:
            (
                fieldname,
                fieldtype
            ) \
                = (
                table.fields[fld].fieldname,
                table.fields[fld].type
            ) \
                if (isinstance(fld, str)) \
                else (
                fld.name,
                fld.type
            ) \
                if (type(fld).__name__ in ('Py4Field', 'JNLField')) \
                else (
                fld.fieldname,
                fld.type
            ) \
                if (type(fld).__name__ == 'ZDict') \
                else (
                None,
                None
            )
            if (reg_datetime_fieldname.search(fieldname) is not None):
                datetime_fields.append(fieldname)
        return datetime_fields