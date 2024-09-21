from os.path import dirname
from libdlg.dlgQuery_and_operators import AND, NOT
from libpy4.py4IO import Py4
from libdlg import SchemaXML, to_releasename
from libdlg.dlgStore import Storage, Lst
import schemaxml

'''  [$File: //dev/p4dlg/libsample/smpRetype.py $] [$Change: 466 $] [$Revision: #11 $]
     [$DateTime: 2024/08/23 04:23:28 $]
     [$Author: mart $]
'''

__all__ = ['Retype']

class Retype(object):
    ''' USAGE:

        >>> qry1 = AND(
                        (oP4.files.action.belongs(('add', 'edit'))),
                        (oP4.files.type != 'ktext')
                        )
        >>> targetfiles = oP4(qry1)._select(oP4.files.depotFile)
        >>> filelist = oP4(oP4.files.depotFile.belongs(targetfiles)).select('depotFile')
        >>> result = oP4().edit(('--type', 'ktext', *filelist))

    '''

    def __init__(self, *args, version='16.2', **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        schemadir = dirname(schemaxml.__file__)
        schemaversion = to_releasename(version)
        objSchema = SchemaXML(schemadir)
        oSchema = objSchema(schemaversion)
        p4args = {
                    'user': kwargs.user or 'mart',
                    'port': kwargs.port or 'anastasia.local:1777',
                    'client': kwargs.client or 'computer_p4q',
                    'oSchema': oSchema
        }
        ''' Create a reference to class Py4
        '''
        self.oP4 = Py4(**p4args)

    def __call__(self, preview=False):
        ''' Retype all added | edited python files that have not been previously typed to ktext.

            There are many ways to achieve this, but I thought that
            this could serve as good example to demonstrate the use
            of the `belongs` aggregator (like `SQL IN`).

            We will need 2 queries.
        '''
        oP4 = self.oP4

        ''' 1. A query to find all python files where the filetype is not 'ktext` 
               and where field `action` has a value of either an `add` or an `edit`. 

               The first query demonstrates the use of a straight up `belongs` (AKA
               a `SQL IN`. It takes a tuple as its only parameter which contains the
               the field values mentioned above. 
        '''
        qry1 = AND(
            AND(
                (oP4.files.action.belongs(('add', 'edit'))),
                (oP4.files.type != 'ktext')
            ),
            (oP4.files.depotFile.endswith('.py'))
        )

        ''' 2. Query p4d for files that defined in qry1, select the target records. 

            The difference between select() and _select() is that _select() 
            doesn't cast the p4 records as DLGRecords objects.

            Note that _select's first arg must be the field object that the
            target files belong to.

            The query should look like this:

                {'objp4': <Py4 anastasia.local:1777 >,
                 'op': <function p4qOp_and_Query.AND(*args, **kwargs)>,
                 'left': <DLGExpression {'left': <Py4Field action>,
                                         'objp4': <Py4 anastasia.local:1777 >,
                                         'op': <function BELONGS at 0x110e36020>,
                                         'right': ('add', 'edit')}>,
                 'right': <DLGQuery {'left': <Py4Field type>,
                                    'objp4': <Py4 anastasia.local:1777 >,
                                    'op': <function NE at 0x110e34f40>,
                                    'right': 'ktext'}>}            

        '''
        targetfiles = oP4(qry1)._select(oP4.files.depotFile)
        #targetfiles2 = oP4(qry1).select('depotFile')
        ''' Note: Records are selected with the help `_select' as opposed to 
                  `select` as we would normally do. The difference is in 
                  the values it returns. `select` will return records (DLGRecords
                  class references) and `sellect' will give us the value of the
                  field specified as `_select`'s single paramater (a Py4Field object).
                  In this case, we get a list of `depotFile`s (`targetfiels`) which 
                  the next query will use to look for `depotFile`s that `belongs` 
                  to that list.
                  
                  Moving on... 
        '''

        ''' 3. A second query, the actual nested `belongs`. This is where `targetfiles` 
               come into play as we pass in those results to the following `select' 
               statement.
        '''
        #qry2 = (oP4.files.depotFile.belongs(targetfiles))

        ''' 4. Select records against qry2. `select()` takes packed fieldname arguments 
               that belong to the table 'files`. 
               
                    eg. Don't know what the table's fields are? No worry. Type (or run)
                        the following:
                        
                        >>> oP4.files.fieldnames
                        ['idx', 'code', 'depotFile', 'rev', 'change', 'action', 'type', 'time']
                    
                        * field `idx` is not a valid field for any of the tables, therefore
                          it can be ignored.
        '''
        #filelist = oP4(qry2).select('depotFile')

        ''' 5. We now have the complete list of files that need to be retyped as 'ktext'. 
               All that remains is to open up the files for `edit` while specifying 
               all we need to do, is open those files for edit while specifying a file 
               type &/| modifier. In this case, and since the purpose os this effort, is
               to enable Keyword Expansion, so we we want -t ktext 

                eg. p4 edit -t ktext filename...  
        '''
        cmdargs = [
                    '-t',
                    'ktext',
                    *targetfiles
                ]
        if (preview is True):
            cmdargs.insert(0, '--preview', )
        try:

            return oP4.edit(*cmdargs)
            #return oP4.edit(*cmdargs)
        except Exception as err:
            print(err)


if (__name__ == '__main__'):
    result = Retype(version='r16.2')(preview=True)
    print(result)