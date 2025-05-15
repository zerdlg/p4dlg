import re

from libdlg.dlgStore import Lst, Storage, objectify
from libdlg.dlgUtilities import serializable

'''  [$File: //dev/p4dlg/libsql/sqlValidate.py $] 
     [$Change: 717 $] 
     [$Revision: #14 $]
     [$DateTime: 2025/05/15 11:21:30 $]
     [$Author: zerdlg $]
'''

__all__ = [
    'is_expressionType',
    'is_strType',
    'is_recordType',
    'is_dictType',
    'is_sqlObjectType',
    'is_fieldType_or_queryType',
    'is_fieldType_or_expressionType',
    'is_fieldType_or_tableType',
    'is_tableType',
    'is_list_of_fields',
    'is_fieldType',
    'is_q_or_dicttype',
    'is_queryType',
    'is_list_of_queries',
    'is_recordsType',
    'is_Py4',
    'is_Py4Exception',
    'is_P4JnlException',
    'is_P4Jnl',
    'is_query_or_expressionType',
    'is_qType_or_field',
    'is_NOSource',
    'is_job',
    'is_serializable',
    'query_is_reference',
    'fieldType',
    'qtypes',
    'objecttypes',
    'tabletypes',
    'fieldtypes',
    'SQLType',
    'is_sliceType',
    'is_substrType',
]


qtypes = ('DLGQuery', 'DLGExpression')

def is_fieldNumerical(field):
    '''TO DO'''

def is_serializable(stype):
    return True \
        if (stype in serializable) \
        else False

def is_job(right):
    return True \
        if (re.match(r'^job[0-9]+$', right) is not None) \
        else False

def is_recordType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (type(left).__name__ == 'Record') &
                    (type(right).__name__ == 'Record')
        ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ == 'Record') \
            else False
    return ret


def is_recordsType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (type(left).__name__ == 'Records') &
                    (type(right).__name__ == 'Records')
            ) \
                else False
    else:
        ret = True \
            if (type(left).__name__ == 'Records') \
            else False
    return ret


def is_strType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (isinstance(left, str) is True) &
                    (isinstance(right, str) is True)
        ) \
            else False
    else:
        ret = True \
            if (isinstance(left, str) is True) \
            else False
    return ret

def is_fieldType_or_queryType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (
                        (is_queryType(left) is True) |
                        (is_fieldType(left) is True)
                    ) &
                    (
                        (is_queryType(right) is True) |
                        (is_fieldType(right) is True)
                    )
                ) \
            else False
    else:
        ret = True \
            if (
                    (is_queryType(left) is True) |
                    (is_fieldType(left) is True)
                ) \
            else False
    return ret

def is_qType_or_field(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (
                        (is_query_or_expressionType(left) is True) |
                        (is_fieldType(left) is True)
                    ) &
                    (
                        (is_query_or_expressionType(right) is True) |
                        (is_fieldType(right) is True)
                    )
                ) \
            else False
    else:
        ret = True \
            if (
                    (is_query_or_expressionType(left) is True) |
                    (is_fieldType(left) is True)
                ) \
            else False
    return ret


def is_q_or_dicttype(left, right=None):
    def istype(ttype):
        return True \
            if (
                (is_query_or_expressionType(ttype) is True)
                | (is_dictType(ttype) is True)
        ) \
            else False

    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    isleft = True \
        if (istype(left) is True) \
        else False
    if (right is not None):
        isright = True \
            if (istype(right) is True) \
            else False
        ret = True \
            if (
                    (isleft is True) |
                    (isright is True)
        ) \
            else False
    else:
        ret = True \
            if (isleft is True) \
            else False
    return ret


def is_sqlObjectType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (
                        (is_query_or_expressionType(left) is True) |
                        (is_fieldType(left) is True) |
                        (is_recordType(left) is True) |
                        (is_tableType(left) is True)
                    ) &
                    (
                        (is_query_or_expressionType(right) is True) |
                        (is_fieldType(right) is True)
                        (is_recordType(right) is True) |
                        (is_tableType(right) is True)
                    )
                ) \
            else False
    else:
        ret = True \
            if (
                    (is_query_or_expressionType(left) is True) |
                    (is_fieldType(left) is True)
                    (is_recordType(left) is True) |
                    (is_tableType(left) is True)
                ) \
            else False
    return ret


def query_is_reference(query):
    if (is_queryType(query) is True):
        if (
                (is_fieldType(query.left) is True) &
                (is_fieldType(query.right) is True)
        ):
            return True
    return False


def is_list_of_fields(iterable):
    try:
        return True \
            if (sum([int(is_fieldType(litem)) for litem in iterable]) == len(iterable)) \
            else False
    except Exception as err:
        print(err)
    return False


def is_list_of_queries(iterable):
    try:
        return True \
            if (sum([int(is_queryType(litem)) for litem in iterable]) == len(iterable)) \
            else False
    except Exception as err:
        print(err)
    return False


def is_dictType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (isinstance(left, dict) is True) &
                    (isinstance(right, dict) is True)
        ) \
            else False
    else:
        ret = True \
            if (isinstance(left, dict) is True) \
            else False
    return ret

def is_query_or_expressionType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (type(left).__name__ in qtypes) |
                    (type(right).__name__ in qtypes)
        ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ in qtypes) \
            else False
    return ret

def is_queryType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False

    if (right is not None):
        ret = True \
            if (
                    (type(left).__name__ == 'DLGQuery') &
                    (type(right).__name__ == 'DLGQuery')
                ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ == 'DLGQuery') \
            else False
    return ret

def is_expressionType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False

    if (right is not None):
        ret = True \
            if (
                    (type(left).__name__ == 'DLGExpression') &
                    (type(right).__name__ == 'DLGExpression')
                ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ == 'DLGExpression') \
            else False
    return ret

def is_fieldType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (type(left).__name__ in ('Py4Field', 'JNLField')) &
                    (type(right).__name__ in ('Py4Field', 'JNLField'))
        ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ in ('Py4Field', 'JNLField')) \
            else False
    return ret

def is_fieldType_or_expressionType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                    (type(left).__name__ in ('Py4Field', 'JNLField', 'DLGExpression')) &
                    (type(right).__name__ in ('Py4Field', 'JNLField', 'DLGExpression'))
        ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ in ('Py4Field', 'JNLField', 'DLGExpression')) \
            else False
    return ret

def is_tableType(left, right=None):
    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if (
                   (type(left).__name__ in ('Py4Table', 'JNLTable')) &
                   (type(right).__name__ in ('Py4Table', 'JNLTable'))
        ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ in ('Py4Table', 'JNLTable')) \
            else False
    return ret


def is_fieldType_or_tableType(left, right=None):
    def istype(ttype):
        return True \
            if (
                    (is_tableType(ttype) is True) |
                    (is_fieldType(ttype) is True)
        ) \
            else False

    if (
            (isinstance(left, (int, bool)) is True) |
            (isinstance(right, (int, bool)) is True)
    ):
        return False

    isleft = True \
        if (istype(left) is True) \
        else False
    if (right is not None):
        isright = True \
            if (istype(right) is True) \
            else False
        ret = True \
            if (
                    (isleft is True) &
                    (isright is True)
        ) \
            else False
    else:
        ret = True \
            if (isleft is True) \
            else False
    return ret

def is_sliceType(value):
    return True \
        if (type(value) is slice) \
        else False

''' expression type validation
'''
def is_substrType(value):
    op = value.op.__name__ \
        if (is_expressionType(value) is True) \
        else value
    return True \
        if (op == 'SUBSTR') \
        else False


SQLType = objectify(
    {
        'objects': (
            'Py4',
            'P4Jnl',
            'PyNO',
            'PyRCS',
            'PyD',
            'P4DB',
        ),
        'tables': (
            'Py4Table',
            'JNLTable',
            'NOTable',
            'RCSTable',
            'PyDTable',
            'P4DTable'
        ),
        'fields': (
            'Py4Field',
            'JNLField',
            'NOField',
            'RCSField',
            'PyDField',
            'P4DBField'
        )
    }
)
objecttypes = SQLType.objects
tabletypes = SQLType.tables
fieldtypes = SQLType.fields


def is_Py4Exception(*args, **kwargs):
    (args, record) = (Lst(args), Storage(kwargs))
    if (
            (len(record) == 0)
            & (isinstance(args(0), list) is True)
    ):
        record = Storage(args(0))

    exceptkeys = ['code', 'data', 'generic', 'severity']
    intersect = record.getkeys().intersect(exceptkeys)

    if (
            (len(intersect) == len(exceptkeys))
            & (kwargs.code == 'error')
    ):
        return True
    return False


def is_P4JnlException(*args, **kwargs):
    pass


def is_Py4(p4obj):
    return True \
        if (type(p4obj).__name__ == 'Py4') \
        else False


def is_P4Jnl(p4obj):
    return True \
        if (type(p4obj).__name__ == 'P4Jnl') \
        else False


def is_NOSource(p4obj):
    return True \
        if (type(p4obj).__name__ == 'NOFile') \
        else False


def fieldType(p4obj):
    return 'Py4Field' \
        if (is_Py4(p4obj) is True) \
        else 'JNLField' \
        if (is_P4Jnl(p4obj) is True) \
        else 'NOField' \
        if (is_NOSource(p4obj) is True) \
        else None