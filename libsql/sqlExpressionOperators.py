from libsql.sqlControl import DLGSql
from libdlg.dlgStore import Lst, Storage, StorageIndex
from libdlg.dlgUtilities import (
    bail,
    noneempty,
)
from libsql.sqlValidate import *

__all__ = [
    'Count', 'Sum', 'Avg', 'Min', 'Max'
]

'''  [$File: //dev/p4dlg/libsql/sqlExpressionOperators.py $] 
     [$Change: 717 $] 
     [$Revision: #9 $]
     [$DateTime: 2025/05/15 11:21:30 $]
     [$Author: zerdlg $]
'''

class Count(DLGSql):
    def count(
            self,
            *fieldnames,
            distinct=False,
            close_session=True,
            **kwargs
    ):
        (fieldnames, kwargs) = (Lst(fieldnames), Storage(kwargs))
        cols = self.cols or Lst()
        records = self.records
        query = self.query
        field = None

        try:
            if (fieldnames(0) is not None):
                field = fieldnames(0)
            elif (noneempty(query) is False):
                qry = query[0] \
                    if (isinstance(query, list) is True) \
                    else query
                field = qry.fieldname \
                    if (is_fieldType(qry) is True) \
                    else qry.left.fieldname

        except Exception as err:
            bail('Can not define fieldname for record counting')

        if (isinstance(query, list) is False):
            query = Lst([query]).clean()
        if (type(records) != enumerate):
            if (len(query) > 0):
                records = self.objp4(query).select(distinct=distinct)
            elif (field is not None):
                oTable = getattr(self.objp4, self.tablename)
                records = self.objp4(oTable).select(field, distinct=distinct)
        (
            eor,
            recordcounter,
            distinctvalues
        ) = \
            (
                False,
                0,
                set()
            )
        recordsiter = self.get_recordsIterator(records)
        while (eor is False):
            skip_record = False
            try:
                (
                    idx,
                    record
                ) = (
                    next(recordsiter)
                )
                if (isinstance(record, list) is True):
                    (record, skip_record) = self.ziprecord(record, cols, idx)
                QResults = Lst()
                if (len(query) == 0):
                    QResults.append(0)
                for qry in query:
                    recresult = self.build_results(qry, record)
                    QResults.append(recresult)
                if (sum(QResults) == len(query)):
                    #skip_record = self.skiprecord(record, self.tablename)
                    #if (skip_record is False):
                    if (distinct is False):
                        distinct = None
                    if (
                            (noneempty(distinct) is False) |
                            (hasattr(distinct, 'objp4') is True)
                    ):
                        fieldname = None
                        if (isinstance(distinct, bool) is True):
                            if (distinct is True):
                                if (isinstance(query, list) is True):
                                    qry = query(0) \
                                        if (len(query) > 0) \
                                        else query
                                if (noneempty(qry) is False):
                                    fieldname = qry.fieldname
                                elif (fieldnames(0) is not None):
                                    fieldname = fieldnames(0).fieldname \
                                        if (is_fieldType(fieldnames(0)) is True) \
                                        else fieldnames(0).left
                        else:
                            fieldname = self.validate_distinct(distinct)
                        if (fieldname is not None):
                            distinctvalue = record(fieldname)
                            if (distinctvalue is not None):
                                distinctvalues.add(distinctvalue)
                    recordcounter += 1
            except (StopIteration, EOFError):
                eor = True
                if (hasattr(records, 'read')):
                    records.close()
            except Exception as err:
                bail(err)
        if (close_session is True):
            self.close()
        distinctcounter = len(distinctvalues)
        return distinctcounter or recordcounter

class Sum(DLGSql):
    def sum(
            self,
            *fieldnames,
            close_session=True,
    ):
        fieldnames = Lst(fieldnames)
        cols = self.cols or Lst()
        records = self.records

        query = self.query
        if (query is None):
            bail("Counting records requires a query, try again.")
        fieldname = None
        try:
            if (fieldnames(0) is not None):
                fieldname = fieldnames(0)
                if (is_fieldType(fieldname) is True):
                    fieldname = fieldname.fieldname
            elif (query is not None):
                qry = query[0] \
                    if (isinstance(query, list) is True) \
                    else query
                fieldname = qry.fieldname \
                    if (is_fieldType(qry) is True) \
                    else qry.left.fieldname
        except Exception as err:
            bail('Can not define fieldname for record counting')

        if (isinstance(query, list) is False):
            query = Lst([query])

        if (type(records) != enumerate):
            records = self.guess_records(query, self.tablename)

        (
            eor,
            recordcounter,
            fsum
        ) = \
            (
                False,
                0,
                0
            )

        recordsiter = self.get_recordsIterator(records)
        while (eor is False):
            skip_record = False
            try:
                (
                    idx,
                    record
                ) = (
                    next(recordsiter)
                )
                if (isinstance(record, list) is True):
                    (record, skip_record) = self.ziprecord(record, cols, idx)
                QResults = Lst()
                if (len(query) == 0):
                    QResults.append(0)
                for qry in query:
                    recresult = self.build_results(qry, record)
                    QResults.append(recresult)
                if (sum(QResults) == len(query)):
                    # skip_record = self.skiprecord(record, self.tablename)
                    # if (skip_record is False):
                    try:
                        fsum += int(record(fieldname))
                    except Exception as err:
                        bail("field value must be an number (str/int/float)")
                    recordcounter += 1
            except (StopIteration, EOFError):
                eor = True
                if (hasattr(records, 'read')):
                    records.close()
            except Exception as err:
                bail(err)
        if (close_session is True):
            self.close()
        return fsum

class Avg(DLGSql):
    def avg(
            self,
            *fieldnames,
            close_session=True,
    ):
        fieldnames = Lst(fieldnames)
        cols = self.cols or Lst()
        records = self.records

        query = self.query
        if (query is None):
            bail("Counting records requires a query, try again.")
        fieldname = None
        try:
            if (fieldnames(0) is not None):
                fieldname = fieldnames(0)
                if (is_fieldType(fieldname) is True):
                    fieldname = fieldname.fieldname
            elif (query is not None):
                qry = query[0] \
                    if (isinstance(query, list) is True) \
                    else query
                fieldname = qry.fieldname \
                    if (is_fieldType(qry) is True) \
                    else qry.left.fieldname
        except Exception as err:
            bail('Can not define fieldname for record counting')

        if (isinstance(query, list) is False):
            query = Lst([query])

        if (type(records) != enumerate):
            records = self.guess_records(query, self.tablename)

        (
            eor,
            recordcounter,
            fsum,
            favg
        ) = \
            (
                False,
                0,
                0,
                0
            )

        recordsiter = self.get_recordsIterator(records)
        while (eor is False):
            skip_record = False
            try:
                (
                    idx,
                    record
                ) = (
                    next(recordsiter)
                )
                if (isinstance(record, list) is True):
                    (record, skip_record) = self.ziprecord(record, cols, idx)
                QResults = Lst()
                if (len(query) == 0):
                    QResults.append(0)
                for qry in query:
                    recresult = self.build_results(qry, record)
                    QResults.append(recresult)
                if (sum(QResults) == len(query)):
                    # skip_record = self.skiprecord(record, self.tablename)
                    # if (skip_record is False):
                    try:
                        fsum += int(record(fieldname))
                    except Exception as err:
                        bail("field value must be an number (str/int/float)")
                    recordcounter += 1
            except (StopIteration, EOFError):
                eor = True
                if (hasattr(records, 'read')):
                    records.close()
            except Exception as err:
                bail(err)
        if (close_session is True):
            self.close()
        return fsum

class Min(DLGSql):
    def sum(
            self,
            *fieldnames,
            close_session=True,
    ):
        fieldnames = Lst(fieldnames)
        cols = self.cols or Lst()
        records = self.records

        query = self.query
        if (query is None):
            bail("Counting records requires a query, try again.")
        fieldname = None
        try:
            if (fieldnames(0) is not None):
                fieldname = fieldnames(0)
                if (is_fieldType(fieldname) is True):
                    fieldname = fieldname.fieldname
            elif (query is not None):
                qry = query[0] \
                    if (isinstance(query, list) is True) \
                    else query
                fieldname = qry.fieldname \
                    if (is_fieldType(qry) is True) \
                    else qry.left.fieldname
        except Exception as err:
            bail('Can not define fieldname for record counting')

        if (isinstance(query, list) is False):
            query = Lst([query])

        if (type(records) != enumerate):
            records = self.guess_records(query, self.tablename)

        (
            eor,
            recordcounter,
            fsum
        ) = \
            (
                False,
                0,
                0
            )

        recordsiter = self.get_recordsIterator(records)
        while (eor is False):
            skip_record = False
            try:
                (
                    idx,
                    record
                ) = (
                    next(recordsiter)
                )
                if (isinstance(record, list) is True):
                    (record, skip_record) = self.ziprecord(record, cols, idx)
                QResults = Lst()
                if (len(query) == 0):
                    QResults.append(0)
                for qry in query:
                    recresult = self.build_results(qry, record)
                    QResults.append(recresult)
                if (sum(QResults) == len(query)):
                    # skip_record = self.skiprecord(record, self.tablename)
                    # if (skip_record is False):
                    try:
                        fsum += int(record(fieldname))
                    except Exception as err:
                        bail("field value must be an number (str/int/float)")
                    recordcounter += 1
            except (StopIteration, EOFError):
                eor = True
                if (hasattr(records, 'read')):
                    records.close()
            except Exception as err:
                bail(err)
        if (close_session is True):
            self.close()
        return fsum

class Max(DLGSql):
    def sum(
            self,
            *fieldnames,
            close_session=True,
    ):
        fieldnames = Lst(fieldnames)
        cols = self.cols or Lst()
        records = self.records

        query = self.query
        if (query is None):
            bail("Counting records requires a query, try again.")
        fieldname = None
        try:
            if (fieldnames(0) is not None):
                fieldname = fieldnames(0)
                if (is_fieldType(fieldname) is True):
                    fieldname = fieldname.fieldname
            elif (query is not None):
                qry = query[0] if (isinstance(query, list) is True) else query
                fieldname = qry.fieldname \
                    if (is_fieldType(qry) is True) \
                    else qry.left.fieldname
        except Exception as err:
            bail('Can not define fieldname for record counting')

        if (isinstance(query, list) is False):
            query = Lst([query])

        if (type(records) != enumerate):
            records = self.guess_records(query, self.tablename)

        (
            eor,
            recordcounter,
            fsum
        ) = \
            (
                False,
                0,
                0
            )

        recordsiter = self.get_recordsIterator(records)
        while (eor is False):
            skip_record = False
            try:
                (
                    idx,
                    record
                ) = (
                    next(recordsiter)
                )
                if (isinstance(record, list) is True):
                    (record, skip_record) = self.ziprecord(record, cols, idx)
                QResults = Lst()
                if (len(query) == 0):
                    QResults.append(0)
                for qry in query:
                    recresult = self.build_results(qry, record)
                    QResults.append(recresult)
                if (sum(QResults) == len(query)):
                    # skip_record = self.skiprecord(record, self.tablename)
                    # if (skip_record is False):
                    try:
                        fsum += int(record(fieldname))
                    except Exception as err:
                        bail("field value must be an number (str/int/float)")
                    recordcounter += 1
            except (StopIteration, EOFError):
                eor = True
                if (hasattr(records, 'read')):
                    records.close()
            except Exception as err:
                bail(err)
        if (close_session is True):
            self.close()
        return fsum

class Len(DLGSql):
    def sum(
            self,
            *fieldnames,
            close_session=True,
    ):
        fieldnames = Lst(fieldnames)
        cols = self.cols or Lst()
        records = self.records

        query = self.query
        if (query is None):
            bail("Counting records requires a query, try again.")
        fieldname = None
        try:
            if (fieldnames(0) is not None):
                fieldname = fieldnames(0)
                if (is_fieldType(fieldname) is True):
                    fieldname = fieldname.fieldname
            elif (query is not None):
                qry = query[0] if (isinstance(query, list) is True) else query
                fieldname = qry.left.fieldname
        except Exception as err:
            bail('Can not define fieldname for record counting')

        if (isinstance(query, list) is False):
            query = Lst([query])

        if (type(records) != enumerate):
            records = self.guess_records(query, self.tablename)

        (
            eor,
            recordcounter,
            fsum
        ) = \
            (
                False,
                0,
                0
            )

        recordsiter = self.get_recordsIterator(records)
        while (eor is False):
            skip_record = False
            try:
                (
                    idx,
                    record
                ) = (
                    next(recordsiter)
                )
                if (isinstance(record, list) is True):
                    (record, skip_record) = self.ziprecord(record, cols, idx)
                QResults = Lst()
                if (len(query) == 0):
                    QResults.append(0)
                for qry in query:
                    recresult = self.build_results(qry, record)
                    QResults.append(recresult)
                if (sum(QResults) == len(query)):
                    # skip_record = self.skiprecord(record, self.tablename)
                    # if (skip_record is False):
                    try:
                        fsum += int(record(fieldname))
                    except Exception as err:
                        bail("field value must be an number (str/int/float)")
                    recordcounter += 1
            except (StopIteration, EOFError):
                eor = True
                if (hasattr(records, 'read')):
                    records.close()
            except Exception as err:
                bail(err)
        if (close_session is True):
            self.close()
        return fsum