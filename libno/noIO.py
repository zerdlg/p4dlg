from types import LambdaType
from libdlg.dlgStore import Storage, objectify, Lst
from libdlg.dlgQuery_and_operators import AND, OR
from libdlg.dlgControl import DLGControl
from libdlg.dlgRecordset import DLGRecordSet
from libdlg.dlgUtilities import queryStringToStorage, bail, noneempty, ALLLOWER
from libno.noSqltypes import NOTable

'''  [$File: //dev/p4dlg/libno/noIO.py $] [$Change: 452 $] [$Revision: #7 $]
     [$DateTime: 2024/07/30 12:39:25 $]
     [$Author: mart $]
'''

__all__ = ['PyNO']

class PyNO(DLGControl):
    def __init__(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), objectify(kwargs))
        loglevel = kwargs.loglevel or 'DEBUG'
        loggername = Lst(__name__.split('.'))(-1)
        super(PyNO, self).__init__(loggername, loglevel)
        self.tablememo = {}
        #self.queryformat = kwargs.queryformat or "strQuery"  # funcQuery, recQuery


    def __call__(self, *queries, **tabledata):

        oRecordSet = DLGRecordSet(self, self.records, **tabledata)
        return oRecordSet() if (len(queries) == 0) else oRecordSet(*queries)


    def __call__(self, query=None, *options, **kwargs):
        noQueries = Lst()
        kwargs = Storage(kwargs)
        (tablename, tabledata) = (kwargs.tablename or 'notable', Storage())
        queries = Lst()
        self.maxrows = kwargs.maxrows or 1000
        self.loginfo(f"maxrows = f{self.maxrows}")
        #self.queryformat = kwargs.queryformat or 'strQuery'
        self.compute = kwargs.compute or Lst()
        if (noneempty(self.compute) is False):
            self.loginfo(f"compute = f{self.compute}")
        if (isinstance(query, NOTable)):
            tablename = query.tablename
            tabledata = self.memoizetable(tablename)
        elif ((noneempty(query) is False) | (type(query).__name__ in \
                ('DLGExpression', 'DLGQuery', 'Py4Table', 'Py4Field'))):
            qries = Lst()
            if (isinstance(query, str)):
                qries = objectify(Lst(queryStringToStorage(q) for q in query.split()))
            elif (isinstance(query, (list, Lst, tuple, NOTable))):
                qries = objectify(Lst(query))
            elif OR((isinstance(query, dict)),(type(query) is LambdaType)):
                qries =  objectify(Lst([query]))
            elif (type(query).__name__ in ('DLGQuery', 'DLGExpression')):
                qries =  Lst([query])
            for qry in qries:
                if (tablename is None):
                    ''' grab the tablename and go!
                    '''
                    (
                        q,
                        left,
                        right,
                        op,
                        tablename,
                        tabledata
                    ) = self.breakdown_query(qry)
                queries.append(qry)
            self.loginfo(f'defined {len(queries)} query(ies)')
        for qry in queries:
            if (isinstance(qry, str)):
                qry = objectify([queryStringToStorage(q) for q in qry.split()])
                for qryitem in Lst(qry.split()).clean():
                    item = queryStringToStorage(qryitem)
                    if (tablename is None):
                        try:
                            (
                                q,
                                left,
                                right,
                                op,
                                tablename,
                                tabledata
                            ) = self.buildQuery(item, tabledata)
                        except Exception as err:
                            bail(
                                f"bad query: {item}\n{err}"
                            )
                    noQueries.append(item)
            else:
                if (tablename is None):
                    try:
                        (
                            q,
                            left,
                            right,
                            op,
                            tablename,
                            tabledata
                        ) = self.buildQuery(qry, tabledata)
                    except Exception as err:
                        bail(
                            f"bad query: {qry}\n{err}"
                        )
                noQueries.append(qry)
        tabledata \
            .merge(kwargs) \
            .merge({item: getattr(self, item) for item in \
                  [
                      'recordchunks',
                      #'queryformat',
                      'tableformat',
                  ]})\
            .merge({
                'maxrows': kwargs.maxrows or 1000,
                'compute': kwargs.compute or '',
                'tablename': tablename,
                'tabletype': NOFile
                })
        oSource = enumerate(self.records)
        if AND((len(queries) == 0), (isinstance(query, NOTable))):
            return DLGRecordSet(self, oSource, **tabledata)
        oRecordSet = DLGRecordSet(self, oSource, **tabledata)
        return oRecordSet() if (len(noQueries) == 0) else oRecordSet(*noQueries)

    def getfieldmaps(self, tablename='notable'):
        ''' mapping of all table names  -> {lower_case_fieldname: actual_fieldname}
        '''
        fieldnames = self.rec0.getkeys()
        fieldsmap = Storage(zip(ALLLOWER(fieldnames), fieldnames))
        fieldtypesmap = Storage()
        return (fieldnames, fieldsmap, fieldtypesmap)

    def memoizetable(self, tablename='notable'):
        try:
            tabledata = self.tablememo[tablename]
        except KeyError:
            (fieldnames, fieldsmap, fieldtypesmap) = self.getfieldmaps(tablename)
            tabledata = self.tablememo[tablename] = Storage({
                'tablename': self.tablename,
                'fieldsmap': fieldsmap,
                'fieldtypesmap': fieldtypesmap,
                'fieldnames': fieldnames
            })
            self.loginfo(f'notable defined: {tablename}')
        return tabledata