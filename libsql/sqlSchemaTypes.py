import re
from libdlg.dlgUtilities import (
    bail,
    isnum,
    IsMatch,
    ALLLOWER
)
from libdlg.dlgStore import Lst, Storage
from libsql.sqlQuery import BELONGS
from libsql.sqlSchema import SchemaXML
from libsql.sqlValidate import *

__all__ = [
    'SchemaType'
]

class SchemaType(object):
    flagnames = lambda self: Lst(dtype.name for dtype in self.datatype_flags())
    bitmasknames = lambda self: Lst(dtype.name for dtype in self.datatype_bitmasks())

    def __init__(self, objJnl=None, oSchema=None, version='latest'):
        oSchema = objJnl.oSchema \
            if (objJnl is not None) \
            else oSchema \
            if (oSchema is not None) \
            else SchemaXML(version=version)
        oP4Schema = oSchema.p4schema
        (
            self.oSchema,
            self.oP4schema
        ) = \
            (
                oSchema,
                oP4Schema
            )
        ''' shortcuts
        '''
        self.version = oP4Schema.version
        self.datatypes = oP4Schema.datatypes.datatype
        self.recordtypes = oP4Schema.recordtypes.record
        self.tables = oP4Schema.tables.table

    def datatype_flags(self):
        ''' datatypes where type is 'flag' (the whole elem)
        '''
        return Lst(
            filter(
                lambda dtype: (dtype.type == 'flag'),
                self.datatypes
            )
        )

    def datatype_bitmasks(self):
        ''' datatypes where type is 'bitmask' (the whole elem)
        '''
        return Lst(
            filter(
                lambda dtype: (dtype.type == 'bitmask'),
                self.datatypes
            )
        )

    def is_flag(self, fieldtype):
        ''' fieldtype is either str or Field object
        '''
        if (is_fieldType(fieldtype) is True):
            fieldtype = fieldtype._type
        if (fieldtype in self.flagnames()):
            dtype = self.datatype_byname(fieldtype)
            return True \
                if (dtype.type == 'flag') \
                else False
        return False

    def is_bitmask(self, fieldtype):
        ''' fieldtype is either str or Field object
        '''
        if (is_fieldType(fieldtype) is True):
            ''' user may have passed in a field instance, get its type & carry on
            '''
            fieldtype = fieldtype._type
        if (fieldtype in self.bitmasknames()):
            dtype = self.datatype_byname(fieldtype)
            return True \
                if (dtype.type == 'bitmask') \
                else False
        return False

    def tablenames(self):
        (
            tables,
            eot
        ) = \
            (
                Lst(),
                False
            )
        qry = (lambda tbl: tbl.name is not None)
        tablesfilter = filter(qry, self.tables)
        while eot is False:
            try:
                tablename = next(tablesfilter).name
                tables.append(tablename)
            except StopIteration:
                eot = True
            except Exception as err:
                bail(err)
        return tables

    def fix_tablename(self, name):
        '''

            >>> oSchemaType.fix_tablename('domain')
           'db.domain'
        '''
        name = name.lower()
        try:
            if (re.match(r'^db\..*$', name) is None):
                name = f'db.{name}'
            if BELONGS(name, (self.tablenames())):
                return name
        except Exception as err:
            bail(err)

    def recordtype_names(self):
        ''' returns a list of all recordtype names
        '''
        (
            recordnames,
            eod
        ) = \
            (
                set(),
                False
            )
        qry = (lambda record: record.name is not None)
        records = filter(qry, self.recordtypes)
        while eod is False:
            try:
                record = next(records).name
                recordnames.add(record)
            except StopIteration:
                eod = True
            except Exception as err:
                bail(err)
        return Lst(recordnames)

    def recordtype_types(self):
        ''' returns a list of all recordtype types
        '''
        (
            recordtypes,
            eod
        ) \
            = (
            set(),
            False
        )
        qry = (lambda rec: rec.column is not None)
        recordsfilter = filter(qry, self.recordtypes)
        while eod is False:
            eoc = False
            try:
                fields = next(recordsfilter).column
                qry2 = (lambda field: field.type is not None)
                fieldsfilter = filter(qry2, fields)
                while eoc is False:
                    try:
                        fieldtype = next(fieldsfilter).type
                        recordtypes.add(fieldtype)
                    except StopIteration:
                        eoc = True
            except StopIteration:
                eod = True
        return Lst(recordtypes)

    def datatype_names(self):
        ''' returns a list of all datatype names

            >>> datatype_names()
            {'Action',
             'Change',
             'ChangeStatus',
             'Counter',
             'Date',
             'DepotType',
             'DescShort',
             'Digest',
             'Domain',
             'DomainOpts',
             'DomainType',
             ... }
        '''
        (
            names,
            eod
        ) = \
            (
                set(),
                False
            )
        qry = (lambda dtype: dtype.name is not None)
        dtypesfilter = filter(qry, self.datatypes)
        while eod is False:
            try:
                dtypename = next(dtypesfilter)
                names.add(dtypename.name)
            except StopIteration:
                eod = True
            except Exception as err:
                bail(err)
        return Lst(names)

    def datatype_types(self):
        ''' returns a list of all datatype types (yes, all 4 of them)

            >>> datatype_types()
            {'bitmask', 'flag', 'integer', 'string'}]
        '''
        (
            types,
            eod
        ) = \
            (
                set(),
                False
            )
        qry = (lambda dtype: dtype.type is not None)
        dtypesfilter = filter(qry, self.datatypes)
        while eod is False:
            try:
                dtypetypenamne = next(dtypesfilter)
                types.add(dtypetypenamne.type)
            except StopIteration:
                eod = True
            except Exception as err:
                bail(err)
        return Lst(types)

    def recordtype_byname(self, recordtypename):
        ''' recordtypename is either str or Table object

        >>> oSchemaType.recordtype_byname('Domain')
        Out[7]:
        [{'name': 'name', 'type': 'Domain', 'desc': 'Domain name'},
         {'name': 'type', 'type': 'DomainType', 'desc': 'Type of domain'},
         {'name': 'extra',
          'type': 'Text',
          'desc': 'Formerly "host". Associated host or, for labels, revision \n\t\t    number.'},
         {'name': 'mount', 'type': 'Text', 'desc': 'The client root'},
         {'name': 'mount2', 'type': 'Text', 'desc': 'Alternate client root'},
         {'name': 'mount3', 'type': 'Text', 'desc': 'Alternate client root'},
         {'name': 'owner',
          'type': 'User',
          'desc': 'Name of user who owns the domain.'},
         {'name': 'updateDate',
          'type': 'Date',
          'desc': 'Date of last update to domain specification.'},
         {'name': 'accessDate',
          'type': 'Date',
          'desc': 'Date of last access to domain specification.'},
         {'name': 'options',
          'type': 'DomainOpts',
          'desc': 'Options for client, label, and branch domains.'},
         {'name': 'description', 'type': 'Text', 'desc': 'Description of domain.'},
         {'name': 'stream',
          'type': 'Text',
          'desc': 'Associated stream for client records'},
         {'name': 'serverid',
          'type': 'Text',
          'desc': 'Associated server ID for client records'},
         {'name': 'partition',
          'type': 'Int',
          'desc': 'Currently unused. Reserved for future use'}]
        '''
        if (is_tableType(recordtypename) is True):
            recordtypename = recordtypename._type
        error = f'{recordtypename} does not belong to this schema version ({self.version}) or {recordtypename} is not a valid recordtype name.\n'
        recordtypename = self.validate_recordtype_name(recordtypename)
        if (recordtypename is not None):
            record = None
            qry = (lambda rec: rec.name == recordtypename)
            try:
                recsfilter = filter(qry, self.recordtypes)
                record = next(recsfilter).column
                if (record is None):
                    bail(f'can not define record for recordtype name `{recordtypename}`')
            except StopIteration:
                pass
            except Exception as err:
                bail(f'recordtype name `{recordtypename}` is invalid.\n')
            return record
        else:
            bail(error)

    def table_byname(self, tablename):
        ''' tablename is either str or Table object

            >>> oSchemaType.get_table_byname('db.domain')
            {'name': 'db.domain',
             'type': 'Domain',
             'version': '6',
             'classic_lockseq': '17',
             'peek_lockseq': '17',
             'keying': 'name',
             'desc': 'Domains: depots, clients, labels, branches, streams, and typemap'}
        '''
        if (is_tableType(tablename) is True):
            tablename = tablename.tablename
        table = None
        tablename = self.fix_tablename(tablename.lower())
        if (tablename is not None):
            qry = (lambda tbl: tbl.name == tablename)
            try:
                tablesfilter = filter(qry, self.tables)
                table = next(tablesfilter)
            except StopIteration:
                pass
            except Exception as err:
                bail(f'{tablename} does not belong to schema version {self.version}')
        return table

    def datatype_byname(self, datatypename):
        ''' datatypename is either str or Field object or Table object.

            But, ..., Field & Table objects have, necessarily, different types...

            >>> oSchemaType.datatype_byname('Domain')
            {'name': 'Domain',
             'type': 'string',
             'summary': 'A domain name',
             'desc': 'A string representing the name of a depot, label, client,\n\t\tbranch, typemap, or stream.'}
        '''
        datatypename = self.datatype_namemap(datatypename)
        if (is_fieldType_or_tableType(datatypename) is True):
            datatypename = datatypename._type

        datatypename = self.validate_datatype_name(datatypename)
        if (datatypename is not None):
            try:
                return next(filter(lambda dtype: (dtype.name == datatypename), self.datatypes))
            except StopIteration:
                pass

    def datatypes_bytype(self, datatypetype):
        ''' returns a list of all datatype elems of type `datatypename`
            (* one of ['string', 'integer', 'flag', 'bitmask']

            >>> datatypes_bytype('string')
            [{'name': 'Counter',
            'type': 'string',
            'summary': 'A counter name',
            'desc': 'A string representing the counter name. Counter ...'},
           {'name': 'DescShort',
            'type': 'string',
            'summary': 'A short string value',
            'desc': 'The first 31 characters of a Text string',
            'seeAlso': 'Text'},
            ...
            }]
        '''
        error = f'{datatypetype} does not belong to this schema version ({self.version}) or {datatypetype} is not a valid datatype type.\n'
        datatypetype = self.validate_datatype_type(datatypetype)
        if (datatypetype is not None):
            (
                datatypes,
                eod
            ) = \
                (
                    Lst(),
                    False
                )
            qry = (lambda dtype: dtype.type == datatypetype)
            dtypesfilter = filter(qry, self.datatypes)
            while eod is False:
                try:
                    dtypetype = next(dtypesfilter)
                    datatypes.append(dtypetype)
                except StopIteration:
                    eod = True
                except Exception as err:
                    bail(err)
            return datatypes
        else:
            bail(error)

    def values_bydatatype(self, datatype):
        ''' Only datatypes of type `flag` or `bitmask` hyave a `values` elem.
            returns the list of values linked to referenced datatypename's `values` key.

            eg.
            >>> jnl.rev.action._type
            'Action'

            >>> oSchemaType.values_bydatatype('Action')
            [{'value': '0', 'desc': 'add; user adds a file'},
             {'value': '1', 'desc': 'edit; user edits a file'},
             {'value': '2', 'desc': 'delete; user deletes a file'},
             {'value': '3', 'desc': 'branch; add via integration'},
             {'value': '4', 'desc': 'integ; edit via integration'},
             {'value': '5', 'desc': 'import; add via remote depot'},
             {'value': '6', 'desc': 'purge; purged revision, no longer available'},
             {'value': '7', 'desc': 'movefrom; move from another filename'},
             {'value': '8', 'desc': 'moveto; move to another filename'},
             {'value': '9', 'desc': 'archive; stored in archive depot'}]

        '''
        if (is_fieldType(datatype) is True):
            datatype = datatype._type
        error = f'{datatype} does not belong to this schema version ({self.version}) or {datatype} is not a valid datatype name\n'
        datatypename = self.validate_datatype_name(datatype)
        if (datatypename is not None):
            values = None
            qry = (lambda datatype: datatype.name == datatypename)
            try:
                flagsfilter = filter(qry, self.datatypes)
                values = next(flagsfilter)['values']
            except StopIteration:
                pass
            except Exception as err:
                bail(err)
            return values
        else:
            bail(error)

    def valuesnames_bydatatype(self, datatypename):
        ''' from the list of values (returned by self.values_bydatatype()),

                eg.
                 {'value': '0', 'desc': 'add; user adds a file'},
                 {'value': '1', 'desc': 'edit; user edits a file'},
                 {'value': '2', 'desc': 'delete; user deletes a file'},
                 {'value': '3', 'desc': 'branch; add via integration'},
                 {'value': '4', 'desc': 'integ; edit via integration'},
                 {'value': '5', 'desc': 'import; add via remote depot'},
                 {'value': '6', 'desc': 'purge; purged revision, no longer available'},
                 {'value': '7', 'desc': 'movefrom; move from another filename'},
                 {'value': '8', 'desc': 'moveto; move to another filename'},
                 {'value': '9', 'desc': 'archive; stored in archive depot'}

            the `value` key represents the value used by p4d (internally). We, as
            users, will likely not remember all flag and bitmask values associated
            each to their own fields (at least I certainly can't). Instead, the
            `name` might be easier to remember.

            Therefore, a SchemaType class reference parses the `desc` key to figure
            out the user's query is looking for. Once parsed, a list of names is
            returned.

            eg, consider this query:

                >>> qry = (jnl.rev.action == 2)

            Though, valid, it may be a challenge for us to remember which flag
            stands for a deleted rev. 'delete' is much more intuitive.

                >>> qry = jnl.rev.action == 'delete'

            >>> obj.values_names_bydatatype('DomainType')
            ['unloaded client',
             'unloaded label',
             'unloaded task stream',
             'branch',
             'client',
             'depot',
             'label',
             'stream',
             'typemap']
        '''
        error = f'{datatypename} does not belong to this schema version ({self.version}) or {datatypename} is not a valid datatype name.\n'
        datatypename = self.validate_datatype_name(datatypename)
        if (datatypename is not None):
            names = None
            try:
                values = self.values_bydatatype(datatypename)
                names = Lst(value['desc'] for value in values)
            except Exception as err:
                pass
            return names
        else:
            bail(error)

    def trimvalue_bydatatype(self, datatype, dtvalue=None):
        ''' datatype is either str or DLGQuery object.

            If dtvalue is None, returns a list of all trimmed values,
            otherwise, returns a single trimmed value.

            * A candidate for this method would be a number value
              followed by its ASCII representation (eg. 'DomainType')

        '''
        if (is_queryType(datatype) is True):
            dtvalue = datatype.right
            datatype = datatype.left._type
        error = f'{datatype} does not belong to this schema version ({self.version}) or {datatype} is not a valid datatype name.\n'
        datatypename = self.validate_datatype_name(datatype)
        if (datatypename is not None):
            values = self.values_bydatatype(datatypename)
            if (
                    (isnum(dtvalue) is False) &
                    (dtvalue is not None)
            ):
                trimmedvalue = None
                try:
                    bits = Lst(re.split(dtvalue, '\s')).clean()
                    row = next(filter(lambda val: val.desc.startswith(dtvalue), values)) \
                        if (len(bits) == 1) \
                        else next(filter(lambda val: val.desc.contains(dtvalue), values))
                    if (re.search(r'\(ASCII', row['value']) is not None):
                        trimmedvalue = Lst(re.split('\s', row['value'])).clean()(0)
                except Exception as err:
                    bail(err)
                return trimmedvalue
            trimmedvalues = set()
            for row in values:
                trimmedvalues = None
                try:
                    if (re.search(r'\(ASCII', row['value']) is not None):
                        trimmedvalue = Lst(re.split('\s', row['value'])).clean()(0)
                    trimmedvalues.add(trimmedvalue)
                except Exception as err:
                    bail(err)
            return Lst(trimmedvalues) \
                if (len(trimmedvalues) > 0) \
                else None
        else:
            bail(error)

    def maskmatch(self, maskname, dtype):
        ''' match a flag name to its internal number.
            If matched, return it, otherwise None.

            TODO: list of checks may need to grow...
        '''
        if (
                (dtype.name is not None) &
                (maskname == dtype.name)
        ):
            return dtype.name
        elif (re.match(f'^{maskname}', dtype.desc) is not None):
            return dtype['value']
        else:
            word = Lst(re.split("[:;,/(']", dtype.desc)).clean()(0)
            if (re.match(maskname, word) is not None):
                return dtype['value']

    def flagmatch(self, flagname, dtype):
        ''' match a flag name to its internal number.
            If matched, return it, otherwise None.

            TODO: list of checks may need to grow...
        '''
        if (
                (dtype.name is not None) &
                (flagname == dtype.name)
        ):
            return dtype.name
        elif (re.match(f'^{flagname}', dtype.desc) is not None):
            return dtype['value']
        else:
            word = Lst(re.split("[:;,/(']", dtype.desc)).clean()(0)
            if (re.match(flagname, word) is not None):
                return dtype['value']

    def resolve_datatype_flag(self, *args):
        (args, flag, datatypename) = (Lst(args), None, None)
        if (len(args) == 1):
            if (not is_queryType(args(0))):
                bail('Single field argument MUST be a Field object.')
            else:
                flag = args(0).right
                datatypename = args(0).left.type
        elif (len(args) == 2):
            if (isinstance(args(0), (int, str)) is True):
                flag = args(0)
            elif (is_fieldType(args(0)) is True):
                flag = args(0).type
            if (
                    (flag is not None) &
                    (isinstance(args(1), str) is True)
            ):
                datatypename = args(1)
        '''
            Usage:
                automatically resolves the right side of the query 
                to its associated flag number (or simply makes sure 
                that the datatype comes back as a str.
    
            Parameters:
                args[0]: flag name or number 
                args[1]: the field's p4type
            
             or args[0]: a single query as a single parameter.        
        eg.
            datatype = jnl.domain.type.type
            qry1 = (jnl.domain.type == 'client')
            qry2 = (jnl.domain.type == '99')
            qry3 = (jnl.domain.type == 99)

            >>> oSchemaType.convert_flag('client', datatype)
            '99'

            >>> oSchemaType.convert_flagname_to_flagvalue(99, datatype)
            '99'
            
            >>> oSchemaType.convert_flagname_to_flagvalue('99', datatype)
            '99'
            
            >>> oSchemaType.convert_flagname_to_flagvalue(qry1)
            '99'
            
            >>> oSchemaType.convert_flagname_to_flagvalue(qry2)
            '99'
            
            >>> oSchemaType.convert_flag(qry3)
            '99'
        
        '''
        error = f'{datatypename} does not belong to this schema version ({self.version}) or {datatypename} is not a valid datatype name.\n'
        ''' See that the datatypename is valid.
            If it is, make sure the user has the correct
            case and correct as needed.
        '''
        datatypename = self.validate_datatype_name(datatypename)
        if (datatypename is not None):
            ''' build a query-lke statement to figure out the correct numbered flag.
            '''
            dtqry = (lambda datatype: self.flagmatch(flag, datatype))
            if (isnum(flag) is False):
                ''' get a list of field flags from the schema 
                '''
                values = self.values_bydatatype(datatypename)
                if (values is not None):
                    flagsfilter = filter(dtqry, values)
                    try:
                        flagvalue = next(flagsfilter)
                        flagedvalue = flagvalue['value']
                        if (isnum(flagedvalue) is True):
                            flag = str(flagedvalue)
                        else:
                            ''' strip everything out of the flagvalue's 
                                description that isn't the number
                            '''
                            flag = Lst(re.split('["\s]', flagedvalue)).clean()(0)
                    except StopIteration:
                        pass
                    except Exception as err:
                        print(err)
            if (isinstance(flag, int)):
                flag = str(flag)
            return flag
        else:
            bail(error)

    def fieldnames_byrecordtype(self, recordtypename):
        error = f"{recordtypename} does not belong to this schema version's ({self.version}) or {recordtypename} is not a record tyoe name.\n"
        recordtypename = self.validate_recordtype_name(recordtypename)
        if (recordtypename is not None):
            eof = False
            fieldnames = Lst()
            column = self.recordtype_byname(recordtypename)
            if (column is None):
                bail(f'No such recordtype {recordtypename}.\n')
            else:
                qry = (lambda col: col.name is not None)
                fieldsfilter = filter(qry, column)
                while eof is False:
                    try:
                        field = next(fieldsfilter).name
                        fieldnames.append(field)
                    except StopIteration:
                        eof = True
                    except Exception as err:
                        bail(err)
                return fieldnames
        else:
            bail(error)

    ''' name & type maps
    '''
    def recordtype_namemap(self, name=None):
        names = self.recordtype_names()
        namemap = Storage(zip(ALLLOWER(names), names))
        return namemap(name.lower()) \
            if (name is not None) \
            else namemap

    def recordtype_typemap(self, typename=None):
        types = self.recordtype_types()
        typemap = Storage(zip(ALLLOWER(types), types))
        return typemap(typename.lower()) \
            if (typename is not None) \
            else typemap

    def datatype_namemap(self, name=None):
        names = self.datatype_names()
        namemap = Storage(zip(ALLLOWER(names), names))
        return namemap(name.lower()) \
            if (name is not None) \
            else namemap

    def datatype_typemap(self, typename=None):
        types = self.datatype_types()
        typemap = Storage(zip(ALLLOWER(types), types))
        return typemap(typename.lower()) \
            if (typename is not None) \
            else typemap

    def validate_datatype_name(self, datatypename):
        namemap = self.datatype_namemap()
        datatypename = datatypename.lower()
        try:
            if BELONGS(datatypename, (namemap.keys())):
                return namemap[datatypename]
        except Exception as err:
            bail(err)

    def validate_datatype_type(self, datatypetype):
        typemap = self.datatype_typemap()
        datatypetype = datatypetype.lower()
        try:
            if BELONGS(datatypetype, (typemap.keys())):
                return typemap[datatypetype]
        except Exception as err:
            bail(err)

    def validate_recordtype_name(self, recordtypename):
        namemap = self.recordtype_namemap()
        recordtypename = recordtypename.lower()
        try:
            if BELONGS(recordtypename, (namemap.keys())):
                return namemap[recordtypename]
        except Exception as err:
            bail(err)

    def validate_recordtype_type(self, recordtypetype):
        typemap = self.recordtype_namemap()
        recordtypetype = recordtypetype.lower()
        try:
            if BELONGS(recordtypetype, (typemap.keys())):
                return typemap[recordtypetype]
        except Exception as err:
            bail(err)

    def cleanup(self, s):
        return re.sub('[\"\']', '', Lst(re.split('[,;\s]', s))(0))

    def convert_maskname_to_maskvalue(self, dtype, maskname):
        ''' USAGE:

            >>> data_type = jnl.protect.perm.type    --> ('Perm')
            >>> oSchemaType.convert_maskname_to_maskvalue(data_type, '0x0040')
            'Super'
        '''
        maskvalues = self.datatype_byname(dtype)('values')
        try:
            maskvalue = next(filter(lambda maskrec: maskrec('name').lower() == maskname.lower(), maskvalues))
            return maskvalue('value')
        except StopIteration:
            pass
        except Exception as err:
            bail(err)

    def convert_maskvalue_to_maskname(self, dtype, maskvalue):
        ''' USAGE:

            >>> data_type = jnl.protect.perm.type    --> ('Perm')
            >>> oSchemaType.convert_maskvalue_to_maskname(data_type, '0x0040')
            'Super'
        '''
        maskvalues = self.datatype_byname(dtype)('values')
        try:
            bitmaskvalue = next(filter(lambda maskrec: maskrec('value') == maskvalue, maskvalues))
            return bitmaskvalue('name')
        except StopIteration:
            pass
        except Exception as err:
            bail(err)

    def flagname_byvalue(self, oField, value):
        ''' Eg.

            from jnl.rev.action, we have these flag values.

            [{'value': '0', 'desc': 'add; user adds a file'},
             {'value': '1', 'desc': 'edit; user edits a file'},
             {'value': '2', 'desc': 'delete; user deletes a file'},
             {'value': '3', 'desc': 'branch; add via integration'},
             {'value': '4', 'desc': 'integ; edit via integration'},
             {'value': '5', 'desc': 'import; add via remote depot'},
             {'value': '6', 'desc': 'purge; purged revision, no longer available'},
             {'value': '7', 'desc': 'movefrom; move from another filename'},
             {'value': '8', 'desc': 'moveto; move to another filename'},
             {'value': '9', 'desc': 'archive; stored in archive depot'}]

             >>> jnl.oSchemaType.flagname_byvalue(jnl.rev.action, '8')

        '''
        value = str(value)
        values = self.values_bydatatype(oField.type)
        desc = next(filter(lambda rec: rec.value == value, values)).desc
        name = Lst(desc.split(';'))(0)
        return name

    def flagvalue_byname(self, oField, flag):
        ''' Eg.

            >>> jnl.oSchemaType.flagvalue_byname(jnl.rev.action, 'movefrom')
            '7'
        '''
        values = self.values_bydatatype(oField.type)
        value = next(filter(lambda rec: rec.desc.startswith(flag), values)).value
        return value


"""
def get_schema_history(objSchema):
    for item in ('server_versions', 'releases'):
        if (objSchema(item) is not None):
            return objSchema[item].release

def iter_schema_history(histrecord, local_releases):
    recversion = histrecord.version or histrecord.release_id
    history_record = Storage()
    if (recversion is not None):
        release = to_releasename(recversion)
        if (release in local_releases):
            history_record.merge(
                {
                    'version': recversion,
                    'release': release,
                }
            )
            # sometimes tables don't change between schema versions...
            # if need, we can expand each table and their respective
            # RecordType to compare fields between schema versions.
            oSchema = SchemaXML(version=release)#define_schema(release)
            tables = oSchema.p4schema.tables.table
            tables_count = len(tables)
            history_record.merge(
                {
                    'tables_count': tables_count,
                    'p4schema': oSchema.p4schema
                }
            )
            del oSchema
            for key in (
                    'added',
                    'changed',
                    'removed'
            ):
                actions = histrecord(key) or Lst(())
                if (isinstance(actions, str) is True):
                    actions = Lst(re.split('\s', actions)).clean()
                history_record.merge({key: actions})
        return history_record

def generate_release_history(objSchema=None, version='latest'):
    ''' the idea is to get the latest a history from the latest schema available,
        which should cntains all the elements needed to guess the target release...
    '''
    schemaxml = SchemaXML(version=version)#define_schema(version=version)
    try:
        objSchema = objSchema.p4schema
    except:
        objSchema = schemaxml.p4schema
    local_releases = schemaxml.list_localreleases()
    history = Lst()
    if (objSchema is not None):
        schema_history = get_schema_history(objSchema)
        EOH = False
        ''' staring with r19.1, a new top level element has been added to the schema
                `server_versions`, then later renamed to `releases`
            (admittedly, a good idea - even for those bozos at Perforce)
            
            TODO: calculate comparable data from previous schema versions
        '''
        filtered_history = filter(lambda hrec: hrec.version or hrec.release_id, schema_history)
        while (EOH is False):
            try:
                histrecord = next(filtered_history)
                history_record = iter_schema_history(histrecord, local_releases)
                if (len(history_record) > 0):
                    history.append(history_record)
            except StopIteration:
                EOH = True
        return history
"""
