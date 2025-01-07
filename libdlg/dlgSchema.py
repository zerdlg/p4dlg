import os
import re
from xml.etree.ElementTree import ElementTree
from libdlg.dlgModel import Py4Model
from libdlg.dlgQuery_and_operators import AND, OR
from libdlg.dlgStore import (
    Storage,
    objectify,
    Lst
)
from urllib import request, parse
from libdlg.dlgUtilities import (
    noneempty,
    decode_bytes,
    bail
)
from libdlg.dlgFileIO import (
    make_writable,
    is_writable,
    fileopen
)

'''  [$File: //dev/p4dlg/libdlg/dlgSchema.py $] [$Change: 467 $] [$Revision: #14 $]
     [$DateTime: 2024/08/24 08:15:42 $]
     [$Author: mart $]
'''

__all__ = [
    'SchemaMatch',
    'SchemaXML',
    'is_releasename',
    'is_versionname',
    'is_xmlfilename',
    'to_releasename',
    'to_versionname',
    'schemaxmlversion',
    'version_to_xmlfilename',
    'fullxmlpath',
    'SchemaMatch'
    ]

''' an re to match against a version number ('2014.2')
        ** not specifying start/end
'''
version_re = '^\d{2,4}\.\d+'

''' an re to match against the release number (r'14.2')
'''
release_re = 'r(\d){2,4}.\d+'

''' an re to match against a version number ('2014.2')
        ** specifying start/end (strict)
'''
versionstrict_re = f'^{version_re}$'

''' an re to match against a schema xml filename ('schema_2014.2.xml')
        ** startswith 'schema_', followed by version_re & endswith '.xml'
'''
versionxmlpath_re = f'^.*{version_re}\.xml$'

releasexml_filename = '^schema_r(\d){0,2}\.\d+\.xml$'

def versionxml_filename_re(version):
    regex = get_versionregex(version)
    return f'^schema_{regex}\.xml$'

def get_schemadir():
    ''' For now, this is their local location
    '''
    from os.path import dirname
    import schemaxml
    return dirname(schemaxml.__file__)

def get_versionregex(version):
    regex = versionstrict_re \
        if (is_versionname(version) is True) \
        else release_re \
        if (is_releasename(version) is True) \
        else version
    return regex

''' verify and rename version/release names
        XXXX.X to rXX.X
'''
def is_releasename(value):
    (ver, matches) = SchemaMatch(release_re)(value)
    return matches

def is_versionname(value):
    (ver, matches) = SchemaMatch(version_re)(value)
    return matches

def is_xmlfilename(value):
    (item, matches) = SchemaMatch('^schema_r(\d){0,2}\.\d+\.xml$')(value)
    return matches

def to_releasename(ver):
    ''' quick versionname to releasename function
        * however, both 'r16.2' & '2016.2' result in 'r16.2'
    '''
    if (re.match('^r', ver) is not None):
        ver = re.sub('^r', '', ver)

    if (re.match('^\d{4}\.\d*$', ver) is not None):
        return re.sub('^(\d){0,2}', 'r', ver) \
            if (is_versionname(ver) is True) \
            else ver
    elif (re.match('^\d{2}\.\d*$', ver) is not None):
        return f'r{ver}'
    else:
        return ver

def to_versionname(ver):
    ''' quick releasename to versionname function
        * however, both '2016.2' & 'r16.2' result in '2016.2'
    '''
    if (ver.startswith('r')):
        ver = re.sub('^r', '' , ver)
    return re.sub(r'^r', '20', ver) \
        if (is_releasename(ver) is True) \
        else ver

def version_to_xmlfilename(version):
    ''' quick version to & xmlfilename function
        * both '2016.2' & 'r16.2' result in 'schema_2014.2.xml'

            >>> version='2014.2'
            >>> version_to_xmlfilename(version)
            'schema_2014.2.xml'
    '''
    version = to_releasename(version)
    regex = get_versionregex(version)
    (res, matches) = SchemaMatch(regex)(version)
    if (matches is True):
        return f'schema_{version}.xml'

''' check if the target version *is* actually valid
'''
def schemaxmlversion(version):
    ''' check if the target version *is* actually a valid verion number

            >>> name='schema_2014.2.xml'
            >>> schemadir(name)
            ['2014.2', True]
    '''
    regex = get_versionregex(version)
    (res, matches) = SchemaMatch(regex)(version)
    if (matches is True):
        return version

def getversion_from_filename(name):
    return re.sub(
        '.xml',
        '',
        re.sub(
            'schema_',
            '',
            name
        )
    )

def fullxmlpath(schemadir, version):
    ''' something to retrieve the full path to a schema xml file

            >>> version='2014.2'
            >>> fullxmlpath(version)
            >>> './schemas/schemaxml/schema_2014.2.xml'

            does the version match something that looks like "schema_r16.2.xml" ?
    '''
    (res, matches) = SchemaMatch(versionxml_filename_re(version))(version)
    if (matches is True):
        return os.path.join(schemadir, version)
    ''' does the version match something that looks like either "r16.2" or "2016.2" ?
    '''
    regex = get_versionregex(version)
    (res, matches) = SchemaMatch(regex)(version)
    if (matches is True):
        return os.path.join(schemadir, version_to_xmlfilename(version))
    ''' does the version match something that looks like a path to a file that ends with ".xml" ?
    '''
    (res, matches) = SchemaMatch(versionxmlpath_re)(version)
    if (matches is True):
        return version

class SchemaMatch(object):
    ''' something to match an expression to a xml schema related string...

            usage:

                >>> x,y=SchemaMatch(version_re)('2014.1.xml')
                >>> print(x,y)
                ['2014.1', True]

                >>> x,y=SchemaMatch(versionxmlname_re)('/s/b/c/f/blabla_2014.1.xml')
                >>> print(x,y)
                ['/s/b/c/f/blabla_2014.1.xml', False]

                >>> x=SchemaMatch(version_re)('_schema_2014.1.xml')
                >>> print(x)
                ['2014.1', True]

                >>> x=SchemaMatch(versionstrict_re)('_schema_2014.1.xml')
                >>> print(x)
                >>> ['_schema_2014.1.xml', False]

                >>> x,y=SchemaMatch(versionxmlname_re)('/s/b/c/f/schema_2014.2.xml')
                >>> print(x,y)
                ['/s/b/c/f/schema_2014.2.xml', False]

                >>> x,y=SchemaMatch(versionxmlname_re)('schema_2014.2.xml')
                >>> print(x,y)
                ['schema_2014.2.xml', True]

                >>> x,y=SchemaMatch(versionxmlpath_re)('/s/b/c/f/blabla_2141.xml')
                >>> print(x,y)
                ['/s/b/c/f/blabla_2141.xml', False]

                >>> x,y=SchemaMatch(versionxmlpath_re)('/s/b/c/f/schema_20141.xml')
                >>> print(x,y)
                ['/s/b/c/f/schema_20141.xml', False]

                >>> x,y=SchemaMatch(versionxmlpath_re)('/s/b/c/f/schema_2014.xml')
                >>> print(x,y)
                ['/s/b/c/f/schema_2141.xml', False]

                >>> x,y=SchemaMatch(versionxmlpath_re)('/s/b/c/f/schema_2014.2.xml')
                >>> print(x,y)
                ['/s/b/c/f/schema_2014.2.xml', True]

                >>> x,y=SchemaMatch(versionxmlpath_re)('/s/b/c/f/schema_2014.2xml')
                >>> print(x,y)
                ['/s/b/c/f/schema_2014.2xml', False]
                >>> a,b=SchemaMatch('^.*{}'.format(version_re))(x)
                >>> print(a,b)
                ['/s/b/c/f/schema_2014.2', True]
    '''

    def __init__(
            self,
            expression,
            strict=False,
            search=True,
            extract=False,
            groupidx=None
    ):
        if (not hasattr(expression, 'search')):
            if AND(
                    OR(
                        (strict is True),
                        (search is True)
                    ),
                    (not expression.startswith('^'))
            ):
                expression = f'^{expression}'
            if AND(
                    OR(
                        (strict is True),
                        (search is True)
                    ),
                    (not expression.endswith('$'))
            ):
                expression = f'{expression}$'
        self.regex = re.compile(expression)
        (
            self.groupidx,
            self.extract,
            self.search
        ) = \
            (
                groupidx,
                extract,
                search
            )

    def __call__(self, value):
        if AND(
                (self.regex is not None),
                (hasattr(self.regex, 'search'))
        ):
            objReg = self.regex.search \
                if (self.search is True) \
                else self.regex.match
            match = objReg(value)
            if (match is not None):
                match_group = match.group() \
                    if (self.groupidx is None) \
                    else match.group(self.groupidx)
                return (self.extract and match_group or value, True)
        return (value, False)

''' parse, download, load & store xml schemas
'''
class SchemaXML(object):
    def __init__(
            self,
            schemadir=None,
            version=None
    ):
        ''' local
        '''
        self.schemadir = schemadir or get_schemadir()
        self.version = self.latestrelease_local() \
            if (version in ('latest', None)) \
            else version
        ''' remote
        '''
        self.currentSchemaURL = 'https://www.perforce.com/perforce/doc.current/schema/index.xml'
        self.leftURL = 'https://ftp.perforce.com/perforce'
        self.rightURL = 'doc/schema/index.xml'
        ''' schema & model
        '''
        (self.p4schema, self.p4model) = (None, None)
        localschemacontent = self.loadxmlschema_local(self.version)
        if (localschemacontent is not None):
            self.p4schema = localschemacontent.p4schema
            oModel = Py4Model(self.p4schema)
            self.p4model = oModel.modelize_schema()

    def __call__(self):
        return self

    ''' list schema files, both local and remote from 'ftp://ftp.perforce.com/perforce/'
    '''
    def listreleases_remote(self):
        releases = []
        try:
            remotereleases = self.readxmlfile_remote(url=self.leftURL).split()
        except Exception as err:
            errmsg = f"Check your internet connection, or the URL is invalid ({self.leftURL})\n"
            bail(f'URLError: {errmsg}.\n{err}\n')
        if AND(
                (len(remotereleases) > 0),
                (isinstance(remotereleases, list))
        ):
            for item in remotereleases:
                found = re.search(r"href=\"(.+)\">", item)
                if (found is not None):
                    p4version = re.sub('\/">.*$', '', re.sub('href="', '', found.group()))
                    if (re.match(r'^r[0-9]+(\.).*$', p4version) is not None):
                        releases.append(p4version)
        return sorted(releases)

    def listreleases_local(self):
        try:
            releases = [
                    getversion_from_filename(schema) for schema \
                        in self.listxmlfiles_local()
                    ]
        except Exception as err:
            releases = Lst()
        return sorted(releases)

    def listxmlfiles_local(self):
        try:
            return sorted(
                [
                    schema for schema in os.listdir(self.schemadir) \
                        if (is_xmlfilename(schema) is True)
                    ]
            )
        except:
            return []

    def versionexists_local(self, version):
        versions = self.listreleases_local()
        return True \
            if (version in versions) \
            else False

    def versionexists_remote(self, version):
        versions = self.listreleases_remote()
        return True \
            if (version in versions) \
            else False

    def latestrelease_local(self):
        localschemas = self.listreleases_local()
        latestschema = max(localschemas)
        return getversion_from_filename(latestschema)

    def latestrelease_remote(self):
        remoteschemas = self.listreleases_remote()
        latestschema = max(remoteschemas)
        return latestschema

    ''' write local (schema_<>.xml)
    '''
    def writexmlfile_local(
            self,
            version,
            localfile=None,
            oFile=None,
            overwrite=False,
            preview=False
    ):
        (
            status,
            exists,
            error,
            schema
        ) = \
            (
                None,
                False,
                None,
                None
            )
        if (oFile is not None):
            try:
                schema = oFile.read()
            finally:
                if (hasattr(oFile, 'close')):
                    oFile.close()
        else:
            try:
                schema = self.readxmlfile_remote(version)
            except Exception as err:
                status = 'skipped'
                return (
                    status,
                    f'{err.url} {err.msg}'
                )

        try:
            if (preview is False):
                os.mkdir(self.schemadir)
        except OSError:pass

        localxmlfilepath = fullxmlpath(self.schemadir, version) \
            if (localfile is None) \
            else localfile
        status = 'pending'

        if (os.path.exists(localxmlfilepath)):
            if (overwrite is True):
                if (is_writable(localxmlfilepath) is False):
                    if (preview is False):
                        make_writable(localxmlfilepath)
                if (preview is False):
                    os.remove(localxmlfilepath)
                status = 'removed'
            else:
                status = 'skipped'
                error = f'{localxmlfilepath} already exists.'

        if AND(
                (status != 'skipped'),
                (preview is False)
        ):
            oFile = fileopen(localxmlfilepath, 'w')
            try:
                oFile.write(schema)
                status = 'written' \
                    if (status != 'removed') \
                    else 'overwritten'
            except Exception as err:
                error = err.msg
                status = f'{status}-failed'
            finally:
                oFile.close()

        if (preview is True):
            status = f'{status} - previewed'

        return (
            status,
            error
        )

    def _schema_update(
            self,
            ver,
            overwrite,
            preview,
    ):
        status = 'pending'
        error = None
        try:
            (
                status,
                error
            ) = \
                (
                    self.writexmlfile_local(
                        ver,
                        overwrite=overwrite,
                        preview=preview,
                    )
                )
        except Exception as err:
            if (type(err).__name__ == 'HTTPError'):
                if AND(
                        (err.msg == 'Not Found'),
                        (err.status == '404')
                ):
                    error = f"No remote schema available for p4 release `{ver}`"

        return (
            ver,
            status,
            error
        )

    def update_xmlschemas(
            self,
            *versions,
            overwrite=False,
            preview=False,
            newonly=True
    ):
        ''' eg.
            SchemaXML().update_xmlschemas(preview=True, newonly=False)
        '''

        def either(arg1, arg2):
            ''' Force mutual exclusivity between newonly & overwrite,
                favouring newonly - they can't both be True.
            '''
            match arg1:
                case True:
                    arg2 = False
                case False:
                    arg2 = True
            return (arg1, arg2)

        (newonly, overwrite) = either(newonly, overwrite)
        localversions = self.listreleases_local()
        remoteversions = versions or self.listreleases_remote()
        max_version = max(localversions)
        (results, skip, EOV) = ([], True, False)
        result_headers = [
            'RELEASE',
            'STATUS',
            'ERROR'
        ]

        verfilter = filter(lambda rver: rver, remoteversions)
        while EOV is False:
            try:
                ver = next(verfilter)
                is_new = (ver > max_version)
                if OR(
                        OR(
                            AND(
                                (is_new is False),
                                (overwrite is True)
                            ),
                            AND(
                                (newonly is True),
                                (is_new is True)
                            )
                        ),
                        AND(
                            (newonly is False),
                            (is_new is False)
                        )
                ):
                    skip = False

                if (skip is True):
                    status = f'skipped - {preview}' \
                        if (preview is True) \
                        else 'Skipped'
                    result = [ver, status, None]
                    result = Storage(
                        zip(
                            result_headers,
                            list(result)
                        )
                    )
                    results.append(result)
                else:
                    result = self._schema_update(
                        ver,
                        overwrite,
                        preview,
                    )
                    result = Storage(
                        zip(
                            result_headers,
                            list(result)
                        )
                    )
                    results.append(result)
            except StopIteration:
                EOV = True
        return results

    ''' read remote (index.xml)
    '''
    def readxmlfile_local(self, ver):
        if (is_versionname(ver) is True):
            ver = to_releasename(ver)
        filename = version_to_xmlfilename(ver)
        localfile = os.path.join(self.schemadir, filename)
        oFile = fileopen(localfile, 'r')
        try:
            return oFile.read()
        finally:
            oFile.close()

    def readxmlfile_remote(self, ver=None, url=None):
        urlpath = f'{self.leftURL}/{ver}/{self.rightURL}' \
            if (ver is not None) \
            else url
        oFile = request.urlopen(urlpath)
        try:
            out = oFile.read()
            return decode_bytes(out) \
                if (type(out) is bytes) \
                else out
        except Exception as err:
            bail(err)
        finally:
            oFile.close()

    def xmlfilename_local(self, ver):
        if (is_versionname(ver) is True):
            ver = to_releasename(ver)
        localxmlfile = os.path.join(self.schemadir, version_to_xmlfilename(ver))
        return localxmlfile

    def loadxmlschema_local(self, ver):
        xmlfile = fullxmlpath(self.schemadir, ver)
        if (os.stat(xmlfile).st_size > 0):
            oFile = open(xmlfile, 'rb')
            oTree = ElementTree()
            try:
                oTree.parse(oFile)
                root = oTree.getroot()
                schema = self.xmlschema2Storage(root)
                return objectify(
                    {
                        root.tag: schema
                    }
                )
            except Exception as err:
                return Storage()
            finally:
                oFile.close()

    def xmlschema2Storage(self, elem, text_as_atrributes=True):
        elemdict = Storage()
        items = elem.attrib
        if (len(items) > 0):
            elemdict.merge(items)

        for child in elem:
            newitem = self.xmlschema2Storage(child)
            if (hasattr(child, 'tag')):
                if (child.tag in elemdict.keys()):
                    if (isinstance(elemdict[child.tag], list)):
                        elemdict[child.tag].append(newitem)
                    else:
                        elemdict[child.tag] = [elemdict[child.tag], newitem]
                else:
                    elemdict[child.tag] = newitem
        text = '' \
            if (noneempty(elem.text) is True) \
            else elem.text.strip()
        if (text_as_atrributes is False):
            elemdict._text = text \
                if AND(
                        (len(elemdict) > 0),
                        (len(text) > 0)
            ) \
                else text \
                if (len(elemdict) == 0) \
                else ''
        else:
            if (len(elemdict) > 0):
                if (len(text) > 0):
                    elemdict._text = text
            else:
                elemdict = text
        return elemdict