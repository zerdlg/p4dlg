import os, re
from pprint import pformat
from typing import *
from ast import literal_eval
import datetime

from libdlg.dlgUtilities import *
from libdlg.dlgFileIO import isanyfile
from libdlg.dlgStore import Storage, objectify, Lst, StorageIndex
from libdlg.dlgDateTime import DLGDateTime
from libdlg.dlgQuery_and_operators import *
from libdlg.contrib.prettytable.prettytable import PrettyTable as PT

''' [$File: //dev/p4dlg/libdlg/dlgTables.py $] [$Change: 472 $] [$Revision: #13 $]
    [$DateTime: 2024/09/03 03:46:02 $]
    [$Author: mart $]
'''

__all__ = [
    'DataGrid',
    'DataTable',
    'msgbox',
]

if (noneempty(os.getenv('ANSI_COLORS_DISABLED')) is False):
    os.getenv('ANSI_COLORS_DISABLED', None)

re_formatted = re.compile('\\x1b[\[0-9;]*m')
(
    STARTSEQUENCE,
    ENDSEQUENCE
) = \
    (
        '\033[',
        '\033[0m'
    )

table_stryle = Storage(
            {
                'default': 10,
                'msword_friendly': 11,
                'plain_columns': 12,
                'markdown': 13,
                'orgmode': 14,
                'double_border': 15,
                'single_border': 16,
                'random': 20
            }
)

tableoption = objectify(
    {
        'hrules': {
                    'default': 0,
                    'frame': 0,
                    'all': 1,
                    'none': 2,
                    'header': 3
                },
        'vrules': {
                    'default': 0,
                    'frame': 0,
                    'all': 2,
                    'none': 3
                },
        'align': {
                    'default': 'l',
                    'center': 'c',
                    'left': 'l',
                    'right': 'r'
                },
        'valign': {
                    'default': 't',
                    'top': 't',
                    'middle': 'm',
                    'bottom': 'b'
                },
        'chars': (
                    '~', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '_', '+', '|', '-', '=',
                    '\\', '{', '}', '[', ']', ';', "'", ':', '"', ',', '.', '/', ';', '<', '>', '?',
                ),
        'vertical_char': '|',
        'horizontal_char': '-',
        'junction_char': '+',
        'top_junction_char': '+',
        'bottom_junction_char': '+',
        'right_junction_char': '+',
        'left_junction_char': '+',
        'top_right_junction_char': '+',
        'top_left_junction_char': '+',
        'bottom_right_junction_char': '+',
        'bottom_left_junction_char': '+',
        'includeid': False,
        'includetype': False,
        'includetypeid': False,
        'header': True,
        'border': True,
        'print_empty': True,
        'maxrows': None,
        'truncate_width': True,
        'max_width': 80
    }
)

class Ascii(object):
    def __init__(self,**options):
        options = Storage(options)
        self.datatable = PT()
        self.datagrid = PT()
        self.dataoptions = Storage()
        self.oDateTime = DLGDateTime()

        for opt in (
                'hrules',
                'vrules',
                'align',
                'valign'
        ):
            self.dataoptions[opt] = options[opt] \
                if (options[opt] is not None) \
                else tableoption[opt].default

        for opt in (
                'vertical_char',
                'horizontal_char',
                'junction_char',
                'top_junction_char',
                'bottom_junction_char',
                'right_junction_char',
                'left_junction_char',
                'top_right_junction_char',
                'top_left_junction_char',
                'bottom_right_junction_char',
                'bottom_left_junction_char'
        ):
            if AND(
                    (options[opt] is not None),
                    (options[opt] in tableoption.chars)
            ):
                self.dataoptions[opt] = options[opt] or tableoption[opt]

        for opt in (
                'min_with',
                'min_table_width',
                'padding_width',
                'left_padding_width',
                'right_padding_width'
        ):
            if (options[opt] is not None):
                self.dataoptions[opt] = options[opt]

        for opt in (
                    'header',
                    'border',
                    'print_empty',
                    'maxrows',
                    'includeid',
                    'includetype',
                    'includetypeid',
                    'truncate_width',
                    'max_width'
        ):
            self.dataoptions[opt] = options[opt] \
                if (options[opt] is not None) \
                else tableoption[opt]

    def formatfieldvalue(self, value):
        try:
            realtype = type(literal_eval(value))
        except Exception as err:
            try:
                realtype = type(value)
            except Exception as err:
                realtype = str(value)
        if (isnum(value) is True):
            try:
                value = (int(value))
            except:
                value = float(value)
            return value
        elif (isinstance(value, str) is True):
            value = value.rstrip('\n')
        elif (realtype is bytes):
            value = value.decode('utf8')
        elif (realtype in (
                dict,
                Storage,
                StorageIndex
            )
        ):
            value = pformat(value)
        if AND(
                (self.dataoptions.truncate_width is True),
                (isinstance(value, str)),
            ):
            if (len(value) > self.dataoptions.max_width):
                return f'{value[0:self.dataoptions.max_width]}...'
        return value

class msgbox(object):
    ''' USAGE:
        >>> header = "Submitted configurables status"
        >>> msg = "All required configurables are valid!"
        >>> msgbox(header)(msg)
        +----------------------------------------+
        |  [__SUBMITTED_CONFIGURABLES_STATUS__]  |
        +----------------------------------------+
        |                                        |
        | All required configurables are valid!  |
        |                                        |
        +----------------------------------------+
        '''

    def __init__(self, title='MESSAGE_BOX', *args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        self.stylize = kwargs.stylize or False
        title = re.sub('\s', '_', title).upper()
        self.msgboxtable = PT([title,])
        self.msgboxtable.align = kwargs.align or 'c'
        self.msgboxtable._hrule = kwargs.hrule or 1

    def __call__(self, msg):
        self.msgboxtable.add_row([msg])
        return self.msgboxtable

class DataGrid(Ascii):
    def __init__(
            self,
            rows,
            **options
    ):
        options = Storage(options)
        super(DataGrid, self).__init__(**options)
        self.exclude_fields = ()
        if (isinstance(rows, list)):
            rows = objectify(rows)
        elif (isinstance(rows, Storage)):
            for key in rows.keys():
                (
                    groupname,
                    value
                ) = \
                    (
                        key,
                        rows[key]
                    )
                if (isinstance(value, StorageIndex) is True):
                    rows.mergeright({key: rows[key]})
        elif (type(rows) in (enumerate, Generator)):
            rows = objectify([row for row in rows])
        elif (type(rows).__name__ == 'DLGRecords'):
            rows = Lst(rows)
        elif (isinstance(rows, StorageIndex) is True):
            rows = storageIndexToList(rows)
        self.rows = rows

    def __call__(self, *fields, **kwargs):
        self.build_datagrid(*fields, **kwargs)
        return self.datagrid

    def define_fields(self, *fields, **kwargs):
        (
            fields,
            kwargs
        ) = \
            (
                Lst(fields),
                Storage(kwargs)
            )
        ''' field definition:

            - if none are specified, then default
                * default
                    1) if rows(0) is a list,then those items are the
                       fieldnames (rows(0) will be popped)
                    2) if rows(0) is a row (Storage,dict) then rows(0).getkeys()
                       are the fieldnames (rows(0) is not popped)
            - can be a list of args
            - can be kwargs.fields (key 'fields' will be popped)
            - if specified, then ony those specified fieldnames will make up the returned rows (best
              effort to respect the order in which they were provided)

            * once defined, cast fields list to StorageIndex()
        '''
        if (noneempty(fields) is True):
            if (noneempty(kwargs.fields) is False):
                fields = Lst(kwargs.pop('fields'))
            if (type(self.rows).__name__ == 'DLGRecords'):
                fields = self.rows.cols
            elif (isinstance(self.rows(0), dict)):
                fields = Storage(self.rows(0)).getkeys()
            elif AND(
                    (isinstance(self.rows(0), list)),
                    (isinstance(self.rows(1), dict))
            ):
                fields = Lst(self.rows.pop(0))
            elif (isinstance(self.rows(0), dict)):
                fields = self.rows(0).getkeys()

        self.fieldsmap = Storage(
            zip(
                [field.lower() for field in fields], fields
            )
        )
        self.fields = fields.storageindex(reversed=True)
        self.cols = [self.fields[fld]for fld in self.fields]

    def defineDataGrid(self):
        fieldnames = self.fields.getvalues() \
            if (type(self.fields) is StorageIndex) \
            else self.fields

        requires_rowidx = False
        if (self.dataoptions.includeid is True):
            if (not 'idx' in fieldnames):
                requires_rowidx = True
                fieldnames.insert(0, 'idx')
        ''' the datagrid
        '''
        self.datagrid = PT(fieldnames, **self.dataoptions)
        rowcounter = 0
        for (nidx, row) in enumerate(self.rows, start=1):
            if (isinstance(row, dict) is True):
                row = Storage(row).getvalues()
            if (requires_rowidx is True):
                row.insert(0, nidx)
            if (self.dataoptions.maxrows is not None):
                if (nidx < self.dataoptions.maxrows):
                    self.addrow(row)
                    rowcounter += 1
            else:
                self.addrow(row)

    def build_datagrid(self, *fields, **kwargs):
        (fields, kwargs) = (Lst(fields), Storage(kwargs))
        if (type(self.rows) is Generator):
            self.rows = Lst([row for row in self.rows])
        if (len(self.rows) == 0):
            return self
        self.dataoptions.merge(kwargs)
        self.define_fields(*fields, **kwargs)
        self.defineDataGrid()

    def addrow(self, row):
        for (idx, fieldname) in self.fields.items():
            value1 = row(idx)
            if (value1 is not None):
                value2 = self.formatfieldvalue(value1)
            if (str(value1) != str(value2)):
                row.pop(idx)
                row.insert(idx, value2)
        self.datagrid.add_row(row)

    def addrows(self, rows):
        for row in rows:
            self.addrow(row)

    def addcolum(
            self,
            fieldname,
            column,
            align=None,
            valign=None
    ):
        self.datagrid.add_column(
            fieldname,
            column,
            align or self.dataoptions.align,
            valign or self.dataoptions.valign
        )
        column = self.datagrid.column

    def addcolums(
            self,
            cols,
            align=None,
            valign=None
    ):
        '''    cols is assumed to be an embedded dict:

                    {
                        <fieldname1>: [row, row, row, ...],
                        <fieldname2>: [row, row, row, ...],
                        ...
                    }
        '''
        align = align or self.dataoptions.align
        valign = valign or self.dataoptions.valign
        for (fldname, fields) in cols.items():
            self.addcolum(
                fldname,
                fields,
                align,
                valign
            )

    def deleterow(self, func):
        for row in self.rows:
            if func(row):
                idx = self.rows.index(row)
                self.datagrid.del_row(idx)
                self.rows.pop(idx)
                break

    def clearrows(self):
        self.datagrid.clear_rows()
        self.rows = Lst()

    def get_string(self):
        if (len(self.rows) > 0):
            return self.datatable.get_string()

    def printer(self):
        if (len(self.rows) > 0):
            print(f"\n[___{self.dataoptions.title.upper()}___]")
            print(self.get_string())

class DataTable(Ascii):
    def __init__(self, record, **options):
        options = Storage(options)
        reservedatts = ['get', 'set']
        self.__dict__ = Storage(self.__dict__)
        self.oDateTime = DLGDateTime()
        self.record = objectify(record) \
            if (
            isinstance(
                record,
                (
                    dict,
                    Storage,
                    Lst,
                    list
                )
            )
        ) \
            else record

        [
            self.record.rename(reservedkey, f'{reservedkey}') \
                for reservedkey in reservedatts \
                    if (reservedkey in self.record.keys())
        ]
        super(DataTable, self).__init__(**options)
        self.title = options.title or 'Record'
        self.excludekeys = options.exclude or ['update_record', 'delete_record']

    def __call__(self, *fields, **kwargs):
        self.build_datatable(*fields, **kwargs)
        return self.datatable

    def build_datatable(self, *fields, **kwargs):
        (
            fields,
            kwargs
        ) = \
            (
                Lst(fields) or Lst(['key', 'value']),
                Storage(kwargs)
            )
        self.dataoptions_copy = self.dataoptions.copy()
        self.dataoptions.merge(kwargs)
        ''' forcing respect of field order
        '''
        if (type(self.record) is not StorageIndex):
            self.record = Lst(
                Storage(
                    {i: j}
                ) for (i, j) in self.record.items()
            ).storageindex(reversed=True, startindex=1)
        ''' do we need to add columns 'idx' and 'type'?
        '''
        if AND(
                (self.dataoptions.includeid is True),
                (not 'idx' in fields)
        ):
            fields.insert(0, 'idx')

        if AND(
                (self.dataoptions.includetype is True),
                (not 'type' in fields)
        ):
            typeidx = 1 \
                if (fields(0) == 'idx') \
                else 0
            fields.insert(typeidx, 'type')
        ''' datatable --> the table object
        '''
        self.datatable = PT(field_names=fields, **self.dataoptions)
        self.datatable.title = self.dataoptions.title or 'TABLE'
        ''' insert a record to datatable
        '''
        if (noneempty(self.record) is False):
            self.orderedtable(self.record)
        self.dataoptions = self.dataoptions_copy

    def getfieldtype(self, value, fieldtype=None):
        ''' returns the value's type name
        '''
        typeindex = 0 \
            if (not 'idx' in self.datatable.field_names) \
            else 1

        if (not 'type' in self.datatable.field_names):
            self.datatable.field_names.insert(typeindex, 'type')

        if (fieldtype is None):
            if (isanyfile(value) is True):
                fieldtype = 'str'
            elif (re.search(r'[/\s]',  value) is not None):
                strlist = re.split(r'[/\s]', value)
                if (len(strlist) > 1):
                    fieldtype = 'str'
            else:
                try:
                    fieldtype = type(literal_eval(value)).__name__
                except ValueError:
                    fieldtype = type(value).__name__
                except Exception as err:
                    fieldtype = 'str'
                    #bail(err)
        elif (isinstance(fieldtype, str) is False):
            if (type(value).__name__ != fieldtype.__name__):
                ''' should we log an error... or just correct it?
                '''
                fieldtype = type(value)
        elif (type(value) != fieldtype):
            fieldtype = type(value).__name__
        return (fieldtype, typeindex)

    def getfieldid(self, key, value):
        if (None in (key, value)):
            return
        idindex = 0
        if (not 'idx' in self.datatable.field_names):
            self.datatable.field_names.insert(idindex, 'idx')
        elif (idindex != 0):
            self.datatable.field_names.remove(idindex)
            self.datatable.field_names.insert(0, 'idx')
        return idindex

    def orderedtable(self, datarecord=None):
        ''' ordered table --> respect field order is True
        '''
        if (noneempty(datarecord) is True):
            datarecord = self.record

        if (type(datarecord) is not StorageIndex):
            datatemp = StorageIndex()
            if (isinstance(datarecord, dict) is True):
                [
                    datatemp.merge({i: {j: datarecord[j]}}) for (i, j) in enumerate(datarecord)
                ]
            datarecord = datatemp
        (
            key,
            value
        ) = \
            (
                None,
                None
            )
        for (idx, data) in datarecord.items():
            if (isinstance(data, dict)):
                (key, value) = Lst(data.items())(0)
            if (noneempty(value) is True):
                value = 'None'
            try:
                self.rowinsert(
                                idx,
                                key,
                                value
                )
            except Exception as err:
                bail(err)


    def rowinsert(
            self,
            idx,
            key,
            value,
            fieldtype=None
    ):
        ''' embed a datatable in another datatable?

        like this:
        >>> PT = PrettyTable
        >>> opt2 = PT(['key', 'value'])
        >>> opt2.add_row(['charlotte', 2.0])
        >>> opt2.add_row(['gc', 1])
        >>> opt2.title = 'TEST'
        
        >>> rows = [{'cat': 'gareth', 'dog': 'charlotte', 'kangaroo': 'jerry', 'test': opt2},
                    {'cat': 'minou', 'dog': 'normand', 'kangaroo': 'albert', 'test': opt2}]
                
        >>> oGrid = DataGrid(rows)
        >>> oGrid()    
        +-------------------------------------------------------+
        |                          GRID                         |
        +--------+-----------+----------+-----------------------+
        | cat    | dog       | kangaroo | test                  |
        +--------+-----------+----------+-----------------------+
        | gareth | charlotte | jerry    | +-------------------+ |
        |        |           |          | |        TEST       | |
        |        |           |          | +-----------+-------+ |
        |        |           |          | |    key    | value | |
        |        |           |          | +-----------+-------+ |
        |        |           |          | | charlotte |  2.0  | |
        |        |           |          | |    gc     |   1   | |
        |        |           |          | +-----------+-------+ |
        | minou  | normand   | albert   | +-------------------+ |
        |        |           |          | |        TEST       | |
        |        |           |          | +-----------+-------+ |
        |        |           |          | |    key    | value | |
        |        |           |          | +-----------+-------+ |
        |        |           |          | | charlotte |  2.0  | |
        |        |           |          | |    gc     |   1   | |
        |        |           |          | +-----------+-------+ |
        +--------+-----------+----------+-----------------------+
        '''
        value = str(value)
        keyidx = 0
        row = Lst()

        ''' Include idx?
        '''
        if AND(
                (self.dataoptions.includeid is True),
                (idx is not None)
        ):
            keyidx += 1
            idindex = self.getfieldid(key, value)
            if (not 'idx' in self.datatable.field_names):
                self.datatable.field_names.insert(idindex, 'idx')
            else:
                if (idindex != 0):
                    self.datatable.field_names.remove(idindex)
                    self.datatable.field_names.insert(0, 'idx')
            row.insert(idindex, idx)

        ''' Include type?
        '''
        if AND(
                (self.dataoptions.includetype is True),
                (noneempty(value) is False)
        ):
            (
                junction,
                horizontal
            ) = \
                (
                    tableoption.horizontal_char,
                    tableoption.junction_char
                )
            reg_datatable = re.compile(f'^\{junction}\{horizontal}.\+$')
            typeindex = 1 \
                if (self.dataoptions.includeid is True) \
                else 0
            if (reg_datatable.search(value) is not None):
                fieldtype = '<DLGTable.DLGDataTable>'
            elif (fieldtype is None):
                ''' check if type is datetime..... make this work!
                '''
                datetime_type = self.oDateTime.guesstype(value)
                if (datetime_type is not None):
                    fieldtype = datetime_type.__name__
            if (fieldtype is None):
                (fieldtype, typeindex) = self.getfieldtype(value, fieldtype)
            keyidx += 1
            row.insert(typeindex, fieldtype)
        value = self.formatfieldvalue(value)
        row.insert(keyidx, key)
        row.insert((keyidx + 1), value)

        # Why is this here ? Can't remember! :(
        if ('set' in row):
            keyindex = row.index('set')
            valueindex = (keyindex + 1)
            if AND(
                    (row(valueindex) == ''),
                    ('unset' in self.record.keys())
            ):
                return

        self.datatable.add_row(row)

    def notable(self, msg="no table for you!"):
        msgbox('no-table')(msg)

    def get_string(self):
        return self.datatable.get_string()

    def printer(self):
        asstr = self.get_string()
        title = self.dataoptions.title
        if (title is not None):
            print(f"\n[___ {str(title).upper()} ___]")
        print(asstr)