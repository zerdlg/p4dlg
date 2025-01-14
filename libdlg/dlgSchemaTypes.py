import re
from libdlg.dlgUtilities import (
    bail,
    isnum,
    ALLLOWER
)
from libdlg.dlgStore import Lst, Storage
from libdlg.dlgQuery_and_operators import BELONGS, AND, OR
from libdlg.dlgSchema import SchemaXML

__all__ = [
    'SchemaType'
]

class SchemaType(object):

    flagnames = lambda self: Lst(dtype.name for dtype in self.datatypes_flags())
    bitmasknames = lambda self: Lst(dtype.name for dtype in self.datatypes_bitmasks())

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
        datatypename = self.validate_datatype_name(datatypename)
        if (datatypename is not None):
            try:
                return next(filter(lambda dtype: (dtype.name == datatypename), self.datatypes))
            except StopIteration:
                pass

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

    def datatypes_flags(self):
        ''' datatypes where type is 'flag'
        '''
        return Lst(
            filter(
                lambda dtype: (dtype.type == 'flag'),
                self.datatypes
            )
        )

    def datatypes_bitmasks(self):
        ''' datatypes where type is 'bitmask'
        '''
        return Lst(
            filter(
                lambda dtype: (dtype.type == 'bitmask'),
                self.datatypes
            )
        )

    def is_flag(self, fieldtype):
        if (not isinstance(fieldtype, str)):
            ''' use may have passed in a field instance, get its type & carry on
            '''
            try:
                fieldtype = fieldtype.type
            except Exception as err:
                bail(err)
        if (fieldtype in self.flagnames()):
            dtype = self.datatype_byname(fieldtype)
            return True \
                if (dtype.type == 'flag') \
                else False
        return False

    def is_bitmask(self, fieldtype):
        if (not isinstance(fieldtype, str)):
            ''' use may have passed in a field instance, get its type & carry on
            '''
            try:
                fieldtype = fieldtype.type
            except:
                bail(f'`{fieldtype}` is not a valid field type')
        if (fieldtype in self.bitmasknames()):

            dtype = self.datatype_byname(fieldtype)
            return True \
                if (dtype.type == 'bitmask') \
                else False
        return False

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

    def convert_flagname_to_flagvalue(self, data_type, flagname):
        ''' USAGE:

            >>> data_type = jnl.domain.type.type    --> ('DomainType')
            >>> oSchemaType.convert_flagname_to_flagvalue(data_type, 'client')
            '99'
        '''
        flagvalues = self.datatype_byname(data_type)('values')
        for flagvalue in flagvalues:
            initvalue = flagvalue('value')
            if (re.search('\(ASCII', initvalue) is not None):
                value = Lst(re.split('\s', initvalue))(0)
            elif (flagvalue('name') is not None):
                value = flagvalue('name')
            if OR(
                    (flagvalue('desc') == flagname),
                    (re.sub("[:;']", '', Lst(re.split('\s', flagvalue('desc')))(0)) == flagname)
            ):
                return value

    def convert_flagvalue_to_flagname(self, data_type, value):
        ''' USAGE:

            >>> data_type = jnl.domain.type.type    -> (DomainType)
            >>> oSchemaType.convert_flagvalue_to_flagname(data_type, '99')
            'client'
        '''
        value = str(value)
        flagvalues = self.datatype_byname(data_type)('values')
        for flagvalue in flagvalues:
            initvalue = flagvalue('value')
            if OR((value == initvalue), (value in initvalue)):
                flagname = None
                if (flagvalue('name') is not None):
                    flagname = flagvalue('name')
                elif (flagvalue('desc') is not None):
                    flagname = re.sub("[:;']", '', Lst(re.split('\s', flagvalue('desc')))(0))
                return flagname

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
    local_releases = schemaxml.listreleases_local()
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