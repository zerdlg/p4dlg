import os
from libdlg.dlgStore import Storage, Lst
from libfs.fsFileIO import loadpickle, dumppickle
from libsql.sqlSchema import SchemaXML
from libdlg.dlgUtilities import remove

from os.path import dirname
import schemaxml
schemadir = dirname(schemaxml.__file__)
default_xmlschema_version = 'r16.2'

'''  [$File: //dev/p4dlg/libjnl/jnlInit.py $] [$Change: 689 $] [$Revision: #10 $]
     [$DateTime: 2025/04/15 05:30:50 $]
     [$Author: mart $]
'''

___all__ = ['JnlInitialize']

class JnlInitialize(object):
    ''' What if we don't know the server's version? It is entirely
        reasonable. So, let's try to guess to which version a journal
        file belongs.
    '''

    def __init__(self,
                 schemaxmldocs=schemadir,
                 oSchema=None,
                 updateschemas=False,
                 overwrite=False):

        self.schemaxmldocs = schemaxmldocs
        self.oSchema = oSchema

        if (self.oSchema is None):
            self.oSchema = SchemaXML(schemadir=schemaxmldocs)
        if (updateschemas is True):
            self.oSchema.update_localschema(release='all', overwrite=overwrite)
        self.xmlPath = self.oSchema.localpath
        self.picklespath = os.path.join(self.xmlPath, 'pickles')
        self.objfilespath = os.path.join(self.picklespath, 'objfiles')
        self.modelfilespath = os.path.join(self.picklespath, 'modelfiles')
        self.p4tablesdatapath = os.path.join(self.picklespath, 'tablesdata')
        self.p4tablesdatafile = os.path.join(self.p4tablesdatapath, 'tablesdata')
        self.versionFiles = self.oSchema.list_localxmlfiles()
        self.releases = [filename.lstrip('schema_').rstrip('.xml') \
                            for filename in self.versionFiles]
        self.makepaths()
        self.cleanupdstores()
        self.objFiles = Lst(objFile for objFile in os.listdir(self.objfilespath))
        self.modelFiles = Lst(modelFile for modelFile in os.listdir(self.modelfilespath))
        self.objects = Storage()
        self.models = Storage()
        self.ALLTABLES = set()
        self.records = Lst()

    def loadTables(self):
        for modelFile in self.modelFiles:
            modelXml = self.models[modelFile]
            [self.ALLTABLES.add(tablename) for tablename in modelXml.keys()]

    def loadAllObjects(self):
        self.cleanupdstores()
        [self.loadObject(objFile) for objFile in self.objFiles]

    def loadObject(self, objFile):
        filename = os.path.join(self.objfilespath, objFile)
        try:
            obj = loadpickle(filename)
            self.objects.merge({objFile: obj})
        except:
            print(f'No such pickle: {objFile}')

    def loadALLmodels(self):
        self.cleanupdstores()
        [self.loadModel(modelFile) for modelFile in self.modelFiles]

    def loadModel(self, modelFile):
        filename = os.path.join(self.modelfilespath, modelFile)
        try:
            obj = loadpickle(filename)
            self.models.merge({modelFile: obj})
        except:
            print(f'No such pickle: {modelFile}')

    def cleanupdstores(self):
        paths = [
             self.xmlPath,
             self.picklespath,
             self.objfilespath,
             self.modelfilespath,
             self.p4tablesdatapath
        ]

        for path in paths:
            dstore = os.path.join(path, '.DS_Store')
            if (os.path.exists(dstore)):
                try:
                    remove(dstore)
                except:pass

        if (
                (hasattr(self, 'objFiles')) &
                (hasattr(self, 'modelFiles'))
        ):
            objs = [self.objFiles, self.modelFiles]
            [obj.clean('.DS_Store') for obj in objs if ('.DS_Store' in obj)]

    def makepaths(self):
        dirs = [
            self.picklespath,
            self.modelfilespath,
            self.objfilespath,
            self.p4tablesdatapath
        ]
        for folder in dirs:
            try:
                os.mkdir(folder)
            except:pass

    def buildSchemaData(self):
        ''' [___            RECORDS FOR GUESSING             ___]
            +-----+---------+--------+--------------+-----------+
            | id  | release | table  | tableVersion | numFields |
            +-----+---------+--------+--------------+-----------+
            | 496 | r13.3   | domain | 6            | 18        |
            | 500 | r16.2   | domain | 6            | 18        |
            | 501 | r18.2   | domain | 7            | 18        |
            +-----+---------+--------+--------------+-----------+
        '''
        idx = 0
        for table in self.ALLTABLES:
            for model in self.models:
                release = model.lstrip('p4Model')
                modelXml = self.models[model]
                modelTables = modelXml.keys()
                idx += 1
                tblrecord = Storage(
                    {
                        'id': idx,
                        'table': table,
                        'release': release
                    }
                )
                if (table in modelTables):
                    tableXml = modelXml[table]
                    tblrecord.merge(
                        {
                            'numFields': len(tableXml.fields),
                            'tableVersion': tableXml.version
                        }
                    )
                    self.records.append(tblrecord)
        dumppickle(self.records, self.p4tablesdatafile)

    def __call__(self, overwrite_objects=False, overwrite_models=False):
        for release in self.releases:
            print(f'initializing.........\t{release}')
            oXml = self.oSchema(release)

            objFile = f'oXml{release}'
            if ((not objFile in self.objFiles) | (overwrite_objects is True)):
                print('creating p4schema....\t')
                xmlSchema = oXml.p4schema
                filename = os.path.join(self.objfilespath, objFile)
                if (os.path.exists(filename)):
                    try:
                        remove(filename)
                    except Exception as err:
                        print(f"ERROR: {err}")
                print('saving to file.......\t')
                dumppickle(xmlSchema, filename)

            modelFile = f'p4Model{release}'
            if ((not modelFile in self.modelFiles) | (overwrite_models is True)):
                print('creating p4 model....\t')
                p4model = oXml.p4model
                filename = os.path.join(self.modelfilespath, modelFile)
                if (
                        (os.path.exists(filename)) &
                        (overwrite_models is True)
                ):
                    try:
                        remove(filename)
                    except Exception as err:
                        print('ERROR: {}'.format(err))
                dumppickle(p4model, filename)
                print('saving to file.......\t')

            print(f'loading object file..\t{objFile}')
            self.loadObject(objFile)
            self.loadModel(modelFile)
            print(f'loading model file...\t{modelFile}')

        self.objFiles = [objFile for objFile in os.listdir(self.objfilespath)]
        self.modelFiles = [modFile for modFile in os.listdir(self.modelfilespath)]
        print('loading tables........\t')
        self.loadTables()
        print('saving schema data....\t')
        self.buildSchemaData()
        return self


def main():
    schemaxmldocs = os.path.abspath('../../schemaxml')
    JnlInitialize(schemaxmldocs)(overwrite_objects=True, overwrite_models=True)

# if __name__=='__main__':main()
