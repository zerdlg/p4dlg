from os.path import dirname
from pprint import pprint
import schemaxml

from libsql.sqlSchema import *
from libdlg.dlgTables import DataGrid

schemadir = dirname(schemaxml.__file__)

''' a few ut & samples on xmlschemas
'''
def testmisc(schemaObj):
    version = schemaObj.version

    #schemaObj = SchemaXML()

    ''' remote p4 schemas & locally stored schemas
    '''
    remote_releases = schemaObj.list_remotereleases()
    pprint(remote_releases)
    latestlocalversion = schemaObj.local_latestrelease()
    print(f'\nlatest local release version: {latestlocalversion}\n')
    local_versions = schemaObj.list_localreleases()
    pprint(local_versions)
    latest_local = schemaObj.local_latestrelease()

    local_xmlfiles = schemaObj.list_localxmlfiles()

    ''' xmlfile readers
    '''
    localcontent = schemaObj.read_localxmlfile(version)
    remotecontent = schemaObj.read_remotexmlfile(version)

    ''' figure out xmlfile names (for storing on this local system)
    '''
    xmlfilename = schemaObj.local_xmlfile(version)
    loadedschema = schemaObj.load_localxmlschema(version)



    ''' version names: yyyy.xx -> 2015.2
        release name:  ryy.xx  -> r15.2
    '''
    versionnames = set()
    filenames = set()
    for name in ('r15.2', '2015.2'):
        ver = to_releasename(name)
        versionnames.add(ver)
        filename = versionname_to_xmlfilename(ver)
        filenames.add(filename)
    if (len(versionnames) == 1):
        print(f'PASSED version names: {versionnames}')
    else:
        print(f'FAILED version names (expected 1 name, got  {len(versionnames)}: {versionnames}')

    if (len(filenames) ==1):
        print(f'PASSED filenames: {filenames}')
    else:
        print(f'FAILED filenames (expected 1 name, got  {len(filenames)}: {filenames}')


def update_xmlschema_files(schemaObj):
    ''' populate this local system with xml schemas posted on https://ftp.perforce.com/perforce/

        return a list of tuples:
        [(version, status, error),]

        version -> eg. 'r16.2'

        +--------------------------------------------------------------------------------------------------+
        |                                  status & explanation                                            |
        +-------------------------------------------+------------------------------------------------------+
        |                   code                    |                      explained                       |
        +-------------------------------------------+------------------------------------------------------+
        | pending                                   | started but incomplete                               |
        | skipped                                   | skipped this release version                         |
        | removed                                   | xmlschem file was removed from the local storage     |
        | failed                                    | something went wrong / error                         |
        | written                                   | xmlschema file was written to local storage          |
        | overwritten                               | xmlschema file existed localy and was overwritten    |
        | removed/pending/skipped-failed            | StatusBeforeFailure-failed                           |
        | overwritten/written/skipped-previewed     | Status in preview                                    |
        +-------------------------------------------+------------------------------------------------------+
        |                               results sample & explanation                                       |
        +----------+----------------------+---------+------------------------------------------------------+
        | Version  |       Status         |  Error  |                    Explained                         |
        +----------+----------------------+---------+------------------------------------------------------+
        | 'r16.2'  | StatusCode           | None    | means xmlfile has been created/overwritten           |
        |          |                      |         | or skipped for version r16.2                         |
        +----------+----------------------+---------+------------------------------------------------------+
        | 'r16.2'  | StatusCode-failed    | ErrMsg  | means a release posted on ftp.perforce.com does      |
        |          |                      |         | not contain a new (or targetted) p4 schema. Skip.    |
        +----------+----------------------+---------+------------------------------------------------------+
        | 'r16.2'  | StatusCode-previewed | None    | means it ran in preview                              |
        +----------+----------------------+---------+------------------------------------------------------+

        where:
        xmlfiles are stored in /p4dlg/xmlschemas/

        *args = release version(s) or None
        overwrite=False,
        preview=False,
        newonly=True

        *args
        we can provide either:
            a single release        -> oSchema,update_xmlschemas('r16.2')
            an array of release     -> oSchema.update_xmlschemas('r16.2', 'r18.2', 'r24.1')
                                                     or paqued *['r16.2', 'r18.2', 'r24.1']
            None                    -> this will instruct the method to target all available
                                       schema releases posted on perforce servers.
        **kwargs
            overwrite = [False]/True    --> if True, existing xmlschema files will be overwritten if targeted.
            preview = [False]/True      --> if True, it will update the schema data without
                                            adding/modifying/deleting local xml files.
            newonly = [True]/False      --> if True, it will skip any release that is lesser or equal
                                            to the latest locally stored release.

        Caveat, `overwrite` & `newonly` are mutually exclusive. They cannot both be True.
    '''
    xmlschema_results = schemaObj.update_xmlschemas(
        'r20.1',
        overwrite=True,
        preview=True,
        newonly=True
    )
    print(DataGrid(xmlschema_results, align='c')())

def test():
    version = 'r20.1'
    oSchema = SchemaXML(version)

    results = oSchema.update_xmlschemas(
        version,
        overwrite=True,
        preview=True,
        newonly=True
    )
    print(DataGrid(results, align='c')())


if (__name__ == '__main__'):
    ''' USAGE:
            >>> schemaObj = SchemaXML('r16.2') 
    '''
    schemaObj = SchemaXML('r15.2')
    testmisc(schemaObj)
    update_xmlschema_files(schemaObj)

