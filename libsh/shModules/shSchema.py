import os
from libdlg.dlgSchema import SchemaXML, to_releasename

'''  [$File: //dev/p4dlg/libsh/shModules/shSchema.py $] [$Change: 463 $] [$Revision: #10 $]
     [$DateTime: 2024/08/19 18:03:01 $]
     [$Author: mart $]
'''

__all__=['P4Schema']


class P4Schema(object):
    def __init__(self, schemaxmldocs='../schemaxml'):
        xmlpath = os.path.abspath(schemaxmldocs)
        self.objSchema=SchemaXML(schemadir=xmlpath)
    def __call__(self, schemaversion='r16.2'):
        schemaversion=to_releasename(str(schemaversion))
        oSchema=self.objSchema(version=schemaversion)
        return oSchema

if (__name__=='__main__'):
    oSchema=P4Schema(schemaxmldocs='../schemaxml')('r15.2')#('2016.2'))


