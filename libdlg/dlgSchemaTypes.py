import re
from libdlg.dlgUtilities import (bail, isnum, ALLLOWER)
from libdlg.dlgStore import Lst, Storage
from libdlg.dlgQuery_and_operators import BELONGS, AND, OR

__all__ = ['SchemaType']

class SchemaType(object):

    def __init__(self, objJnl):
        self.objJnl = objJnl
        self.oP4Schema = objJnl.oSchema.p4schema
        ''' shortcuts
        '''
        self.version = self.oP4Schema.version
        self.datatypes = self.oP4Schema.datatypes.datatype
        self.recordtypes = self.oP4Schema.recordtypes.record
        self.tables = self.oP4Schema.tables.table

    ''' RECORDTYPES
    '''
    def recordtype_names(self):
        qry = (lambda record: record.name is not None)
        recordnames = set()
        eod = False
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
        eod = False
        recordtypes = set()
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

    def recordtype_byname(self, recordtypename):
        ''' >>> oSchemaType.recordtype_byname('Domain')
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
        error = f'{recordtypename} does not belong to this schema version ({self.version}).\n'
        datatypename = self.validate_recordtype_name(recordtypename)
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

    ''' TABLES
    '''
    def tablenames(self):
        eot = False
        tables = Lst()
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

    def table_byname(self, name):
        ''' >>> oSchemaType.get_table_byname('db.domain')
            {'name': 'db.domain',
             'type': 'Domain',
             'version': '6',
             'classic_lockseq': '17',
             'peek_lockseq': '17',
             'keying': 'name',
             'desc': 'Domains: depots, clients, labels, branches, streams, and typemap'}
        '''
        table = None
        name = self.fix_tablename(name.lower())
        if (name is not None):
            qry = (lambda tbl: tbl.name == name)
            try:
                tablesfilter = filter(qry, self.tables)
                table = next(tablesfilter)
            except StopIteration:
                pass
            except Exception as err:
                bail(f'{name} does not belong to schema version {self.version}')
        return table

    ''' DATATYPES
    '''
    def datatype_byname(self, datatypename):
        ''' >>> oSchemaType.get_datatype_byname('Domain')
            {'name': 'Domain',
             'type': 'string',
             'summary': 'A domain name',
             'desc': 'A string representing the name of a depot, label, client,\n\t\tbranch, typemap, or stream.'}
        '''
        error = f'{datatypename} does not belong to this schema version ({self.version}).\n'
        datatypename = self.validate_datatype_name(datatypename)
        if (datatypename is not None):
            datatype = None
            qry = (lambda dtype: dtype.name == datatypename)
            try:
                dtypesfilter = filter(qry, self.datatypes)
                datatype = next(dtypesfilter)
            except StopIteration:
                pass
            except Exception as err:
                bail(err)
            return datatype
        else:
            bail(error)

    def datatypes_bytype(self, datatypetype):
        ''' retrive all datatype records of type `typename`

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
        error = f'{datatypetype} does not belong to this schema version ({self.version}).\n'
        datatypetype = self.validate_datatype_type(datatypetype)
        if (datatypetype is not None):
            eod = False
            datatypes = Lst()
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

    def values_bydatatype(self, datatypename):
        error = f'{datatypename} does not belong to this schema version ({self.version}).\n'
        datatypename = self.validate_datatype_name(datatypename)
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

    def values_names_bydatatype(self, datatypename):
        ''' >>> obj.values_names_bydatatype('DomainType')
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
        error = f'{datatypename} does not belong to this schema version ({self.version}).\n'
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

    def values_codes_bydatatype(self, datatypename):
        ''' codes datatype.values values (codes sounds a little more discriminate)
        '''
        error = f'{datatypename} does not belong to this schema version ({self.version}).\n'
        datatypename = self.validate_datatype_name(datatypename)
        if (datatypename is not None):
            codes = set()
            values = self.values_bydatatype(datatypename)
            for value in values:
                try:
                    code = value
                    if (re.search(r'\(ASCII', value['value']) is not None):
                        code = Lst(re.split('\s', value['value'])).clean()(0)
                    codes.add(code)
                except Exception as err:
                    bail(err)
            return Lst(codes) \
                if (len(codes) > 0) \
                else None
        else:
            bail(error)

    def datatype_flag(self, flag, datatypename):
        '''

            match the flag agaisnt the datatype's value or name

            if flag is a number, then do nothing & return initial flag
            if string, then get the datatype's flag value (the number),
            then return it

            I.e.
            >>> obj.datatype_flag('client')
            '99'
            >>> obj.datatype_flag(99)
            '99'
            >>> obj.datatype_flag('99')
            '99'

            Usage:
                automatically resolves a flag name to its associated flag number in a query.
            eg.
            from:
                >>> qry = (oJnl.domain.type == 'client')
            to:
                >>> qry = (oJnl.domain.type == '99')
        '''
        error = f'{datatypename} does not belong to this schema version ({self.version}).\n'
        datatypename = self.validate_datatype_name(datatypename)
        if (datatypename is not None):
            qry = (lambda datatype: datatype.desc == flag)
            if (isnum(flag) is False):
                values = self.values_bydatatype(datatypename)
                if (values is not None):
                    flagsfilter = filter(qry, values)
                    try:
                        flagvalue = next(flagsfilter)['value']
                        flag = Lst(re.split('["\s]', flagvalue)).clean()(0)
                    except StopIteration:
                        pass
                    except Exception as err:
                        print(err)
            if (isinstance(flag, int)):
                flag = str(flag)
            return flag
        else:
            # bail(f'`{flag}` does not belong to datatype {datatypename}.\n')
            bail(error)

    def datatype_types(self):
        ''' retrieve a list of all datatype types form this schema's version
            >>> datatype_types()
            {'bitmask', 'flag', 'integer', 'string'}]
        '''
        eod = False
        types = set()
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

    def datatype_names(self):
        ''' retrieve a list of all datatype names form this schema's version

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
        eod = False
        names = set()
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

    def fieldnames_byrecordtype(self, recordtypename):
        error = f"{recordtypename} does not belong to this schema version's ({self.version}).\n"
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
    def recordtype_typemap(self):
        types = self.recordtype_types()
        try:
            return Storage(zip(ALLLOWER(types), types))
        except Exception as err:
            bail(err)

    def recordtype_namemap(self):
        names = self.recordtype_names()
        try:
            return Storage(zip(ALLLOWER(names), names))
        except Exception as err:
            bail(err)

    def datatype_namemap(self):
        names = self.datatype_names()
        try:
            return Storage(zip(ALLLOWER(names), names))
        except Exception as err:
            bail(err)

    def datatype_typemap(self):
        types = self.datatype_types()
        try:
            return Storage(zip(ALLLOWER(types), types))
        except Exception as err:
            bail(err)

    ''' name & type validation table name fixing
        &
        tries to catch case problems
    '''
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

    def datatdatatypes_flagsypes_flags(self):
        ''' datatypes where type is 'flag'
        '''
        return Lst(
            filter(
                lambda dtype: dtype.type == 'flag',
                self.datatypes
            )
        )

    def datatypes_bitmasks(self):
        ''' datatypes where type is 'bitmask'
        '''
        return Lst(
            filter(
                lambda dtype: dtype.type == 'bitmask',
                self.datatypes
            )
        )

    def flagnames(self):
        return Lst(dtype.name for dtype in self.datatypes_flags())

    def bitmasknames(self):
        return Lst(dtype.name for dtype in self.datatypes_bitmasks())

    def is_flag(self, field):
        ''' TODO: test this.
        '''
        datatypenames = self.datatype_names()
        datatypenames_lower = ALLLOWER(datatypenames)
        fieldname = field.fieldname
        fieldtype = field.type
        fieldname_lower = fieldname.lower()

        return True if AND((fieldname_lower in datatypenames_lower), (fieldtype == 'flag')) else False

        #fieldname = field if (isinstance(field, str) is True) else field.fieldname
        #return True if (fieldname in self.flagnames) else False

    def is_bitmask(self, field):
        ''' TODO: complete this
        '''
        fieldname = field if (isinstance(field, str) is True) else field.fieldname
        return True if (fieldname in self.bitmasknames) else False

    def fix_tablename(self, name):
        ''' >>> oSchemaType.fix_tablename('domain')
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

    def is_domaintype(self, flagname):
        dflag = self.datatype_flag(flagname, 'DomainType')
        return True \
            if (dflag is not None) \
            else False

    def mask_value2name(self, oField, value):
        ''' TODO: this '''

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

             >>> jnl.oSchemaType.flag_value2name(jnl.rev.action, '8')

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

    '''
    datatypenames = jnl.oSchemaType.datatype_names())
    datatypenames_lower = ALLLOWER(datatypenames)
    
    def is
    
    def get_datatype_name(field):
        fieldname = field.fieldname
        fieldname_lower = fieldname.lower() 
        if (fieldname_lower in datatypenames_lower):
            return re.sub(fieldname[0], fieldname[0].upper(), fieldname)

    '''
