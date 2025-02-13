from libsql import DLGSql, is_fieldType
from libdlg.dlgStore import ZDict, Lst
from libdlg.dlgUtilities import (
    bail
)

class Count(DLGSql):
    '''
    *fieldnames,
    records=None,
    cols=None,
    query=None,
    distinct=None,
    close_session=True,
    leave_field_values_untouched=False,
    datetype='datetime',
    **kwargs

    '''
    def count(
            self,
            distinct=None,
            close_session=True,
    ):
        if (distinct is not None):
            distinct = self.validate_distinct(distinct)
        cols = self.cols or Lst()
        records = self.records
        query = self.query or []
        if (isinstance(query, list) is False):
            query = Lst([query])
        if (type(records) != enumerate):
            records = self.guess_records(query, self.tablename)

        (
            eor,
            recordcounter,
            distinctrecords
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
                    if (distinct is not None):
                        distinctvalue = record(distinct)
                        if (distinctvalue is not None):
                            distinctrecords.add(distinctvalue)
                    recordcounter += 1
            except (StopIteration, EOFError):
                eor = True
                if (hasattr(records, 'read')):
                    records.close()
            except Exception as err:
                bail(err)
        if (close_session is True):
            self.close()
        distinctcounter = len(distinctrecords)
        return distinctcounter or recordcounter