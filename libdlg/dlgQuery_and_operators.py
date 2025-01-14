import sys
import re
from functools import reduce
from types import FunctionType
from datetime import date, time, datetime
from pprint import pformat

from libdlg.dlgDateTime import DLGDateTime
from libdlg.dlgStore import Storage, Lst, StorageIndex
from libdlg.dlgUtilities import (
    bail,
    serializable,
    isnum,
    getTableOpKeyValue,
    is_array,
    fieldType,
    reg_type,
    basestring,
)

'''  [$File: //dev/p4dlg/libdlg/dlgQuery_and_operators.py $] 
     [$Change: 452 $] 
     [$Revision: #10 $]
     [$DateTime: 2024/07/30 12:39:25 $]
     [$Author: mart $]
'''

objectify = Storage.objectify

__all__ = [
           'ops', 'andops', 'orops', 'xorops', 'notops', 'dops',
           'containlike_ops', 'optable', 'expression_table', 'all_ops_table',
 \
           'QClass',
           'NOT', 'AND', 'OR', 'XOR',
           'EQ', 'NE', 'GE', 'GT', 'LE', 'LT',
           'CONTAINS', 'ENDSWITH', 'STARTSWITH',
           'ADD', 'SUB', 'MUL', 'MOD',
           'CASE', 'CASEELSE', 'DIFF', 'MATCH', 'SEARCH',
           'LOWER', 'UPPER', 'JOIN', 'LEFT',
           'PRIMARYKEY', 'COALESCE', 'COALESCEZERO',
           'EXTRACT', 'SUBSTRING', 'LIKE', 'ILIKE',
           'SUM', 'ABS',
           'AVG', 'MIN', 'MAX', 'BELONGS', 'IN', 'TRUEDIV', 'COUNT',
           'YEAR', 'MONTH', 'DAY', \
 \
            'DLGQuery', 'DLGExpression', 'query_is_reference',
\
            'qtypes',
            'is_dictType', 'is_queryType', 'is_expressionType',
            'is_fieldType', 'is_qType_or_field', 'is_field_tableType',
            'is_tableType', 'is_recordType', 'is_recordsType', 'is_strType',
            'is_query_or_expressionType',
           ]

qtypes = ('DLGQuery', 'DLGExpression')

def is_recordType(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if AND(
                    (type(left).__name__ == 'DLGRecord'),
                    (type(right).__name__ == 'DLGRecord')
        ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ == 'DLGRecord') \
            else False
    return ret


def is_recordsType(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if AND(
                    (type(left).__name__ == 'DLGRecords'),
                    (type(right).__name__ == 'DLGRecords')
            ) \
                else False
    else:
        ret = True \
            if (type(left).__name__ == 'DLGRecords') \
            else False
    return ret


def is_strType(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if AND(
                    (isinstance(left, str) is True),
                    (isinstance(right, str) is True)
        ) \
            else False
    else:
        ret = True \
            if (isinstance(left, str) is True) \
            else False
    return ret


def is_dictType(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if AND(
                    (isinstance(left, dict) is True),
                    (isinstance(right, dict) is True)
        ) \
            else False
    else:
        ret = True \
            if (isinstance(left, dict) is True) \
            else False
    return ret


def is_query_or_expressionType(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if AND(
                    (type(left).__name__ in qtypes),
                    (type(right).__name__ in qtypes)
        ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ in qtypes) \
            else False
    return ret


def is_qType_or_field(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if AND(
                    OR(
                        (is_query_or_expressionType(left) is True),
                        (is_fieldType(left) is True)
                    ),
                    OR(
                        (is_query_or_expressionType(right) is True),
                        (is_fieldType(right) is True)
                    )
                ) \
            else False
    else:
        ret = True \
            if OR(
                    (is_query_or_expressionType(left) is True),
                    (is_fieldType(left) is True)
                ) \
            else False
    return ret


def is_expressionType(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False

    if (right is not None):
        ret = True \
            if AND(
                    (type(left).__name__ == 'DLGExpression'),
                    (type(right).__name__ == 'DLGExpression')
                ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ == 'DLGExpression') \
            else False
    return ret


def is_queryType(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if AND(
                    (type(left).__name__ == 'DLGQuery'),
                    (type(right).__name__ == 'DLGQuery')
            ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ == 'DLGQuery') \
            else False
    return ret


def is_fieldType(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if AND(
                    (type(left).__name__ in ('Py4Field', 'JNLField')),
                    (type(right).__name__ in ('Py4Field', 'JNLField'))
        ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ in ('Py4Field', 'JNLField')) \
            else False
    return ret


def is_tableType(left, right=None):
    if OR(
            (isinstance(left, (int, bool)) is True),
            (isinstance(right, (int, bool)) is True)
    ):
        return False
    if (right is not None):
        ret = True \
            if AND(
                   (type(left).__name__ in ('Py4Table', 'JNLTable')),
                   (type(right).__name__ in ('Py4Table', 'JNLTable'))
        ) \
            else False
    else:
        ret = True \
            if (type(left).__name__ in ('Py4Table', 'JNLTable')) \
            else False
    return ret


def is_field_tableType(left, right=None):
    def istype(ttype):
        return True \
            if OR(
                    (is_tableType(ttype) is True),
                    (is_fieldType(ttype) is True)
        ) \
            else False

    if OR(
            (isinstance(left, (int, bool)) is True),
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
            if AND(
                    (isleft is True),
                    (isright is True)
        ) \
            else False
    else:
        ret = True \
            if (isleft is True) \
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

    if OR(
            (isinstance(left, (int, bool)) is True),
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
            if AND(
                    (isleft is True),
                    (isright is True)
        ) \
            else False
    else:
        ret = True \
            if (isleft is True) \
            else False
    return ret


def query_is_reference(query):
    if (is_query_or_expressionType(query) is True):
        if (is_fieldType(query.left, query.right) is True):
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

''' Bitwise Operators
'''
def AND(*args, **kwargs):
    return clsAND(**kwargs)(*args)

def OR(*args, **kwargs):
    return clsOR(**kwargs)(*args)

def XOR(*args, **kwargs):
    return clsXOR(**kwargs)(*args)

def NOT(*args, **kwargs):
    return clsNOT(**kwargs)(*args)

def INVERT(*args, **kwargs):
    return clsINVERT(**kwargs)(*args)

''' Compound Operators

    TODO: when time permits
'''
def EQAND(*args, **kwargs):
    ''' a = a & b'''
def EQOR(*args, **kwargs):
    ''' a = a | b'''
def EQXOR(*args, **kwargs):
    ''' a = a ^ b'''

''' operrators
'''
def EQ(*args, **kwargs):
    return clsEQ(**kwargs)(*args)

def NE(*args, **kwargs):
    return clsNE(**kwargs)(*args)

def LT(*args, **kwargs):
    return clsLT(**kwargs)(*args)

def LE(*args, **kwargs):
    return clsLE(**kwargs)(*args)

def GT(*args, **kwargs):
    return clsGT(**kwargs)(*args)

def GE(*args, **kwargs):
    return clsGE(**kwargs)(*args)

def CONTAINS(*args, **kwargs):
    return clsCONTAINS(**kwargs)(*args)

def STARTSWITH(*args, **kwargs): return (
    clsSTARTSWITH(**kwargs)(*args))

def ENDSWITH(*args, **kwargs): return (
    clsENDSWITH(**kwargs)(*args))

def MATCH(*args, **kwargs):
    return clsMATCH(**kwargs)(*args)

def SEARCH(*args, **kwargs):
    return clsSEARCH(**kwargs)(*args)

def DIFF(*args, **kwargs):
    return bool(clsDIFF(**kwargs)(*args))

def CASE(*args, **kwargs):
    return clsCASE(**kwargs)(*args)

def CASEELSE(*args, **kwargs):
    return clsCASEELSE(**kwargs)(*args)

def SUM(*args, **kwargs):
    return clsSUM(**kwargs)(*args)

def MIN(*args, **kwargs):
    return clsMIN(**kwargs)(*args)

def MAX(*args, **kwargs):
    return clsMAX(**kwargs)(*args)

def ABS(*args, **kwargs):
    return clsABS(**kwargs)(*args)

def AVG(*args, **kwargs):
    return clsAVG(**kwargs)(*args)

def LOWER(*args, **kwargs):
    return clsLOWER(**kwargs)(*args)

def UPPER(*args, **kwargs):
    return clsUPPER(**kwargs)(*args)

def LIKE(*args, **kwargs):
    return clsLIKE(**kwargs)(*args)

def ILIKE(*args, **kwargs):
    return clsILIKE(**kwargs)(*args)

def ADD(*args, **kwargs):
    return clsADD(**kwargs)(*args)

def SUB(*args, **kwargs):
    return clsSUB(**kwargs)(*args)

def MUL(*args, **kwargs):
    return clsMUL(**kwargs)(*args)

def TRUEDIV(*args, **kwargs):
    return clsTRUEDIV(**kwargs)(*args)

def MOD(*args, **kwargs):
    return clsMOD(**kwargs)(*args)

def BELONGS(*args, **kwargs):
    return clsBELONGS(**kwargs)(*args)

def IN(*args, **kwargs):
    return clsIN(**kwargs)(*args)

def ON(*args, **kwargs):
    return clsON(**kwargs)(*args)

def COUNT(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsCOUNT(**kwargs)(*args)

def COALESCE(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsCOALESCE(**kwargs)(*args)

def COALESCEZERO(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsCOALESCEZERO(**kwargs)(*args)

def JOIN(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsJOIN(**kwargs)(*args)

def LEFT(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsLEFT(**kwargs)(*args)

def YEAR(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsYEAR(**kwargs)(*args)

def MONTH(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsMONTH(**kwargs)(*args)

def DAY(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsDAY(**kwargs)(*args)

def HOUR(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsHOUR(**kwargs)(*args)

def MINUTE(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsMINUTE(**kwargs)(*args)

def SECOND(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsSECOND(**kwargs)(*args)

def EPOCH(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsEPOCH(**kwargs)(*args)

def PRIMARYKEY(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsPRIMARYKEY(**kwargs)(*args)

def EXTRACT(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsEXTRACT(**kwargs)(*args)

def SUBSTRING(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsSUBSTRING(**kwargs)(*args)

def BETWEEN(*args, **kwargs):
    #return clsBETWEEN(**kwargs)(*args)
    return clsNOTIMPLEMENTED(**kwargs)(*args)

class QClass(object):
    expcache = {}
    __init__ = lambda self, *args, **kwargs: self.__dict__.update(*args, **kwargs)

    def cache(self, value):
        try:
            valcache = self.expcache[value]
        except KeyError:
            valcache = self.expcache[value] = re.compile(value)
        return valcache

    def is_all_bools(self, *exps):
        return all(
            [
                True \
                    if (isinstance(item, bool)) \
                    else False for item in exps
            ]
        )

    def get_boolres(self, *exps):
        exps = Lst(exps)
        allbool = all(
            [
                True \
                    if (isinstance(item, bool)) \
                    else False for item in exps
            ]
        )
        if (allbool is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left & right),
                              self.getSequence(*exps))
            else:
                def true_or_false(bitem):
                    return OR(
                        AND(
                            bitem,
                            1
                        ),
                        0
                    )

                alltrue = [
                    true_or_false(i) for i in exps
                ]
                sum_alltrue = sum(alltrue)
                qres = (len(exps) == sum_alltrue is True)
                return qres

    def build_query(self, left, right, OP, inversion=False):
        qry = Storage()
        if (OP in notops):
            inversion = True
        if (isinstance(left, bool) | isinstance(right, bool)):
            return qry
        if (hasattr(left, 'left')):
            if (left.left is not None):
                try:
                    if (not type(left.left).__name__ in ('DLGQuery', 'DLGExpression')):
                        left = self.build(left)
                except Exception as err:
                    pass
            else:
                left.left = left
        if (hasattr(left, 'right')):
            if (left.right is not None):
                try:
                    if (not type(left.right).__name__ in ('DLGQuery', 'DLGExpression')):
                        right = self.build(right)
                except Exception as err:
                    pass
            else:
                left.right = right

        return Storage({'op': OP, 'left': left, 'right': right, 'inversion': inversion})

    def build(self, qry):
        (op, left, right) = getTableOpKeyValue(qry) \
            if (isinstance(qry, str)) \
            else (qry.op, qry.left, qry.right)
        built = None
        if (op in andops + orops + xorops + notops):
            (buildleft, buildright) = (self.build(left), self.build(right))
            if (op in (AND, 'AND', '&')):
                built = {'op': AND, 'left': buildleft, 'right': buildright}
            elif (op in (OR, 'OR', '|')):
                built = {'op': OR, 'left': buildleft, 'right': buildright}
            elif (op in (XOR, 'XOR', '^')):
                built = {'op': XOR, 'left': buildleft, 'right': buildright}
            elif (op in ((NOT, 'NOT', '~'))):
                if (left is None):
                    bail('Invalid Query')
                buildleft = self.build(left)
                built = ~buildleft
        else:
            attdict = objectify({"left": left, "right": right})
            for (akey, avalue) in attdict.items():
                if (is_query_or_expressionType(avalue) is True) | (is_dictType(avalue) is True):
                    if (hasattr(avalue, 'op') is True):
                        if (avalue.op is not None):
                            avalue = self.build(avalue)
                    if (hasattr(avalue, 'tablename')) & (hasattr(avalue, 'fieldname')):
                        if (avalue.tablename is not None) & (avalue.fieldname is not None):
                            (
                                fieldname,
                                tablename
                            ) = \
                                (
                                    avalue.fieldname,
                                    avalue.tablename
                                )
                            if (hasattr(avalue.objp4, tablename) is True):
                                oTable = getattr(avalue.objp4, tablename)
                                if (hasattr(oTable, fieldname)):
                                    avalue = getattr(oTable, fieldname)
                                else:
                                    bail(f"field `{fieldname}` does not beliong to table `{tablename}`.")
                if (akey == "left"):
                    left = avalue
                else:
                    right = avalue
            opname = op.__name__ \
                if (callable(op) is True) \
                else op
            if (all_ops_table(opname) is not None):
                built = {'op': op, 'left': left, 'right': right}
            elif not (left or right):
                built = {'op': op}
            else:
                bail(f"Operator not supported: {opname}")
        return objectify(built)

    def __and__(self, x, y):
        try:
            return (x and y)
        except:
            return False

    def __or__(self, x, y):
        try:
            return (x or y)
        except:
            return False

    def __xor__(self, x, y):
        try:
            return (x ^ y)
        except:
            return False

    def __eq__(self, value):
        return objectify({"op": "=",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def __ne__(self, value):
        return objectify({"op": "!=",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def __lt__(self, value):
        return objectify({"op": "<",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def __gt__(self, value):
        return objectify({"op": ">",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def __le__(self, value):
        return objectify({"op": "<=",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def __ge__(self, value):
        return objectify({"op": ">=",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def __invert__(self, value):
        return objectify({"op": NOT,#"~",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          'inversion': True,
                          "right": value})

    def contains(self, value):
        return objectify({"op": "#",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def startswith(self, value):
        return objectify({"op": "#^",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def endswith(self, value):
        return objectify({"op": "#$",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def match(self, value):
        return objectify({"op": "##",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def search(self, value):
        return objectify({"op": "#?",
                          "left": {"tablename": self.tablename,
                                   "fieldname": self.fieldname},
                          "right": value})

    def QFunc(self, *exps):
        exps = Lst(exps)
        (exps, ret) = (exps.storageindex(reversed=True), StorageIndex({}))
        for exp in exps:
            expvalue = exps[exp]
            expresult = self.evaluate(expvalue) if (isinstance(expvalue, serializable)) else expvalue
            ret.mergeright(expresult)
        return ret

    def evaluate(self, qry, record=None):
        if (record is not None):
            try:
                # "@some[?,#,?#,op]@[?,#,?#,op]"
                (tablename, field, value, op) = (None, None, None, None)
                if (type(qry).__name__ in ('Storage', 'DLGQuery')):
                    tablename = qry.left.tablename
                    fieldname = qry.left.fieldname
                    value = qry.right
                    op = qry.op
                elif (isinstance(qry, str)):
                    (tablename, fieldname, value, op) = getTableOpKeyValue(qry)
                elif (isinstance(qry, list)):
                    qry = Lst(qry)
                    (field, value, op) = (qry(0), qry(1), qry(2))
                    if (is_fieldType(field) is True):
                        fieldname = field.fieldname
                fieldsmap = self.fieldsmap or self.objp4.fieldsmap
                fieldname = fieldsmap[fieldname.lower()]

                return self.__eq__(str(record[fieldname]), str(value)) \
                    if (op == '=') \
                    else self.__ne__(str(record[fieldname]), str(value)) \
                    if (op == '!=') \
                    else self.__lt__(float(record[fieldname]), float(value)) \
                    if (op == '<') \
                    else self.__le__(float(record[fieldname]), float(value)) \
                    if (op == '<=') \
                    else self.__gt__(float(record[fieldname]), float(value)) \
                    if (op == '>') \
                    else self.__ge__(float(record[fieldname]), float(value)) \
                    if (op == '>=') \
                    else self.__invert__(str(record[fieldname]), str(value)) \
                    if (op == '~') \
                    else (self.cache(value).search(str(record[fieldname])) is not None) \
                    if (op.startswith('#')) \
                    else (self.cache(value).search(str(record[fieldname])) is None) \
                    if (op.startswith('!#')) \
                    else (self.cache(value).match(f'^{str(record[fieldname])}') is not None) \
                    if (op.startswith('#^')) \
                    else (self.cache(value).match(f'{str(record[fieldname])}$') is not None) \
                    if (op.startswith('#$')) \
                    else (self.cache(value).match(f'^{str(record[fieldname])}$') is not None) \
                    if (op.startswith('#^$')) \
                    else (self.cache(value).search(str(record[fieldname])) is not None) \
                    if (op.startswith('#?')) \
                    else (qry in [str(rvalue) for rvalue in record.getvalues()]) \
                    if (isinstance(record[fieldname], (list, dict))) \
                    else ~record[fieldname] \
                    if (op.startswith('~')) \
                    else False
            except ValueError:
                return False

    def validate_field_type(self, left, right=None):
        field_type = None
        left_right_are_valid = False
        if AND((type(right).__name__ == "DLGRecords"),
               (type(left).__name__ in ('Py4Field', 'JNLField'))):
            rec0 = right(0)
            fieldvalue0 = rec0[left.fieldname]
            if AND((isnum(fieldvalue0)),
                  (type(fieldvalue0) in (str, int, float))):
                field_type = type(fieldvalue0)
                left_right_are_valid = True
        return (field_type, left_right_are_valid)

    def getSequence(self, *exps, **kwargs):
        values = self.QFunc(*exps).getvalues()
        sequence = Lst(exps) \
            if (len(values) == 0) \
            else values
        return sequence

def op_error(*items, name=None, err=None):
    items = Lst(items)
    msg = f"Invalid `{name}` operation.\n" \
        if (name is not None) \
        else "Invalid operation.\n"
    (
        left,
        right,
        op,
        other
    ) = (
        items(0),
        items(1),
        items(2),
        items(3)
    )
    if (op is not None):
        opname = op.__name__ \
                if (callable(op) is True) \
                else op
        operation = f"{left} {opname} {right}"
        if (other is not None):
            othername = other.__name__ \
                if (type(other) is FunctionType) \
                else other
            operation += f" | {othername}"
    else:
        operation = f"{name}({left}, {right})"
    msg += f"\n\t({operation})\n"
    error = err or "- Error message not provided -"
    msg += f"\n{error}.\n"
    bail(msg)


class clsNOTIMPLEMENTED(QClass):
    def __call__(self, *exp):
        exp = Lst(exp)
        try:
            ''' force an exception '''
            return exp(0)
        except IndexError:
            return (NotImplementedError,
                    "Operator not yet defined. call back later!")

class clsAND(QClass):
    ''' USAGE:

        Will the following conditions pickup this client record?

                {'code': 'stat',
                 'client': 'anastasia',
                 'Update': '1641848078',
                 'Access': '1689653983',
                 'Owner': 'mart',
                 'Options': 'noallwrite noclobber nocompress unlocked nomodtime normdir',
                 'SubmitOptions': 'submitunchanged',
                 'LineEnd': 'local',
                 'Root': '/home/pi',
                 'Host': '',
                 'Description': 'Created by mart.\n'}

            >>> Query1 = (oP4.client.client.startswith('ana'))
            >>> Query2 = (oP4.clients.owner == 'mart')
            >>> qry = AND(Query1, Query2)
            >>> qry
            <DLGQuery {'left': <DLGQuery {'left': <Py4Field client>,
                                        'objp4': <Py4 anastasia.local:1777 >,
                                        'op': <function STARTSWITH at 0x1052036a0>,
                                        'right': 'ana'}>,
                      'objp4': <Py4 anastasia.local:1777 >,
                      'op': <function AND at 0x13f1e0e00>,
                      'right': <DLGQuery {'left': <Py4Field Owner>,
                                         'objp4': <Py4 anastasia.local:1777 >,
                                         'op': <function EQ at 0x105203740>,
                                         'right': 'mart'}>}>
    '''
    def __call__(self, *exps):
        exps = Lst(exps)
        allbool = all(
            [
                True \
                    if (isinstance(item, (bool, int)) is True) \
                    else False for item in exps
            ]
        )
        if (allbool is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left & right),
                                  self.getSequence(*exps))
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        else:
            (left, right) = (exps(0), exps(1))
            try:
                if (type(left).__name__ == 'Storage'):
                    qres = Storage({'op': AND, 'left': left, 'right': right})
                elif (type(left).__name__ == 'DLGQuery'):
                    qres = DLGQuery(left.objp4, AND, left, right)
                elif (type(left).__name__ == 'DLGExpression'):
                    qres = DLGExpression(left.objp4, AND, left, right)
                elif (isinstance(left, str) is True):
                    (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                    qres = Storage(
                        {
                            'op': AND,
                            'left':
                                    {
                                        'tablename': tablename,
                                        'fieldname': fieldname
                                    },
                            'right': value
                        }
                    )
                else:
                    qres = self.build_query(
                                            left,
                                            right,
                                            AND
                    )
                return qres
            except Exception as err:
                op_error(lambda left, right, op: (
                    left,
                    right,
                    op or AND
                ), name='AND', err=err)


class clsOR(QClass):
    ''' USAGE:

            Will the following conditions pickup this client record?

                    {'code': 'stat',
                     'client': 'anastasia',
                     'Update': '1641848078',
                     'Access': '1689653983',
                     'Owner': 'mart',
                     'Options': 'noallwrite noclobber nocompress unlocked nomodtime normdir',
                     'SubmitOptions': 'submitunchanged',
                     'LineEnd': 'local',
                     'Root': '/home/pi',
                     'Host': '',
                     'Description': 'Created by mart.\n'}

                >>> Query1 = (oP4.client.client.startswith('ana'))
                >>> Query2 = (oP4.clients.owner == 'mart')
                >>> qry = OR(Query1, Query2)
                >>> client_records = oP4(qry).select()

            Nested:
                >>> Query1 = (oP4.client.client.startswith('x'))
                >>> Query2 = (oP4.clients.owner == 'mart')
                >>> Query3 = (oP4.clients.Description.contains('mart'))
                >>> nestedQuery = OR((AND(Query1, Query2)),(Query3))
                >>> nestedQuery
                <DLGQuery {'left': <DLGQuery {'left': <DLGQuery {'left': <Py4Field client>,
                                                              'objp4': <Py4 anastasia.local:1777 >,
                                                              'op': <function STARTSWITH at 0x103d0bd80>,
                                                              'right': 'x'}>,
                                            'objp4': <Py4 anastasia.local:1777 >,
                                            'op': <function AND at 0x147ad8f40>,
                                            'right': <DLGQuery {'left': <Py4Field Owner>,
                                                               'objp4': <Py4 anastasia.local:1777 >,
                                                               'op': <function EQ at 0x103d0b920>,
                                                               'right': 'mart'}>}>,
                          'objp4': <Py4 anastasia.local:1777 >,
                          'op': <function OR at 0x147ad8fe0>,
                          'right': <DLGQuery {'left': <Py4Field Description>,
                                             'objp4': <Py4 anastasia.local:1777 >,
                                             'op': <function CONTAINS at 0x103d0bce0>,
                                             'right': 'mart'}>}>
        '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': OR, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, OR, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, OR, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left | right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = Storage(
                    {
                        'op': OR,
                        'left': {
                                    'tablename': tablename,
                                    'fieldname': fieldname
                                },
                        'right': value
                    }
                )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        OR
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or OR
            ), name='OR', err=err)

class clsXOR(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': XOR, 'left': left, 'right': right})
            elif (type(left).__name__ in ('DLGQuery', 'DLGExpression')):
                qres = DLGQuery(left.objp4, XOR, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left ^ right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = Storage(
                    {'op': XOR,
                     'left': {
                                'tablename': tablename,
                                'fieldname': fieldname
                            },
                    'right': value
                     }
                )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        XOR
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or XOR
            ), name='XOR', err=err)

class clsINVERT(QClass):
    '''
    q = NOT((oP4.clients.Owner=='mart'))

    <DLGExpression {'left': <DLGQuery {'left': <Py4Field Owner>,
                                      'objp4': <Py4 anastasia.local:1777 >,
                                      'op': <function EQ at 0x1075ff6a0>,
                                      'right': 'mart'}>,
                    'objp4': <Py4 anastasia.local:1777 >,
                    'op': <function NOT at 0x169fe1080>,
                    'right': None}>  --> right is uniary, it is always None!
    '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        if (right is not None):
            bail(f"clsINVERT - right MUST be None! Check itn out.")
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': INVERT, 'left': left, 'right': right})
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'DLGQuery')):
                qres = DLGExpression(left.objp4, INVERT, left, right)

            elif ((isinstance(left, (bool, int)))):
                qres = not left
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = Storage(
                    {'op': INVERT,
                     'left': {
                                'tablename': tablename,
                                'fieldname': fieldname
                            },
                    'right': value
                     }
                )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        INVERT
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or INVERT
            ), name='NOT', err=err)

class clsNOT(QClass):
    '''
    q = NOT((oP4.clients.Owner=='mart'))

    <DLGExpression {'left': <DLGQuery {'left': <Py4Field Owner>,
                                      'objp4': <Py4 anastasia.local:1777 >,
                                      'op': <function EQ at 0x1075ff6a0>,
                                      'right': 'mart'}>,
                    'objp4': <Py4 anastasia.local:1777 >,
                    'op': <function NOT at 0x169fe1080>,
                    'right': None}>  --> right is uniary, it is always None!
    '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        inversion = True
        if (right is not None):
            bail(f"clsNOT - right MUST be None! Check itn out.")
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': NOT, 'left': left, 'right': right, 'inversion': True})
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'DLGQuery')):
                qres = DLGQuery(left.objp4, NOT, left, right, inversion=True)
            elif ((isinstance(left, (bool, int)))):
                qres = not left
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = Storage(
                    {'op': NOT,
                     'inversion': True,
                     'left': {
                                'tablename': tablename,
                                'fieldname': fieldname
                            },
                    'right': value
                     }
                )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        NOT,
                                        inversion
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op, inversion: (
                left,
                right,
                op or NOT,
                inversion
            ), name='NOT', err=err)


class clsNE(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': NE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, NE, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, NE, left, right)
            elif (type(left).__name__ in ('Py4Field', 'JNLField')):
                qres = DLGQuery(
                    **objectify(
                        {
                            'objp4': left.objp4,
                            'op': NE,
                            'left': {
                                'tablename': left.tablename,
                                'fieldname': left.fieldname
                            },
                            'right': right
                        }
                    )
                )
            elif (type(left) == type(right)):
                qres = reduce(lambda left, right: (left != right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = DLGQuery(
                    **objectify(
                        {
                            'objp4': self,
                            'op': NE,
                            'left': {
                                'tablename': tablename,
                                'fieldname': fieldname
                            },
                            'right': value
                        }
                    )
                )
            else:
                qres = self.build_query(
                    left,
                    right,
                    NE
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or NE
            ), name='NE', err=err)

class clsEQ(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': EQ, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, EQ, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, EQ, left, right)
            elif (type(left).__name__ in ('Py4Field', 'JNLField')):
                 qres = DLGQuery(
                                 **objectify({
                                    'objp4': left.objp4,
                                    'op': EQ,
                                    'left': {
                                              'tablename': left.tablename,
                                              'fieldname': left.fieldname
                                    },
                                    'right': right
                                    }
                            )
                 )
            elif (type(left) == type(right)):
                '''
                       (
                         (isinstance(left, (bool, int)) is True)
                        & (isinstance(right, (bool, int)) is True)
                       )
                    or
                       (
                         (isinstance(left, str) is True)
                        & (isinstance(right, str) is True)
                       )
                       
            ):
            '''
                qres = reduce(lambda left, right: (left == right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = DLGQuery(
                                **objectify({
                                  'objp4': self,
                                  'op': EQ,
                                  'left': {
                                            'tablename': tablename,
                                            'fieldname': fieldname
                                        },
                                  'right': value
                                }
                            )
                )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        EQ
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or EQ
            ), name='EQ', err=err)

class clsLT(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': LT, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, LT, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, LT, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left < right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = Storage({'op': LT,
                                'left': {
                                            'tablename': tablename,
                                            'fieldname': fieldname
                                        },
                               'right': value
                                }
                               )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        LT
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or LT
            ), name='LT', err=err)


class clsLE(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': LE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, LE, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, LE, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left <= right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = Storage({'op': LE,
                                'left': {
                                            'tablename': tablename,
                                            'fieldname': fieldname
                                        },
                               'right': value
                                }
                            )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        LE
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or LE
            ), name='LE', err=err)

class clsGT(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': GT, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, GT, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, GT, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left > right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = Storage({'op': GT,
                                'left':
                                        {
                                            'tablename': tablename,
                                            'fieldname': fieldname
                                        },
                               'right': value
                                }
                               )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        GT
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or GT
            ), name='GT', err=err)


class clsGE(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': GE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, GE, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, GE, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left >= right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                qres = Storage({'op': GE,
                                'left': {
                                            'tablename': tablename,
                                            'fieldname': fieldname
                                        },
                               'right': value
                                }
                               )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        GE
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or GE
            ), name='GE', err=err)


class clsCONTAINS(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': CONTAINS, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, CONTAINS, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, CONTAINS, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (re.search(f"{right}", left) is not None),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                try:
                    (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                except:
                    (tablename, fieldname, value, op) = (None, None, None, None)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.search(f"{right}", left) is not None),
                                  self.getSequence(*exps))
                else:
                    qres = Storage({'op': CONTAINS,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                }
                            )
            elif (is_array(right) is True):
                right = tuple([right])
                exps.pop(1)
                exps.insert(1, right)
                return True if (left in right) else False
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        CONTAINS
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or CONTAINS
            ), name='CONTAINS', err=err)

class clsBELONGS(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        ''' `right` should either be a tuple of arbitrary 
            objects or a reference to class `Select`.
        '''
        if (is_array(right) is False):
            if (type(right).__name__ == 'Select'):
                right = tuple(right.infield)
            else:
                right = tuple([right])
            exps.pop(1)
            exps.insert(1, right)
        try:
            if AND((type(left).__name__ == 'Storage'), (is_array(right) is True)):
                qres = Storage({'op': BELONGS, 'left': left, 'right': right})
            elif AND(
                        (type(left).__name__ == 'DLGQuery'),
                        (is_array(right) is True)
            ):
                qres = DLGQuery(left.objp4, BELONGS, left, right)
            elif AND(
                        (type(left).__name__ in ('Py4Field', 'JNLField', 'DLGExpression')),
                        (is_array(right) is True)
            ):
                qres = DLGExpression(left.objp4, BELONGS, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left in right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left in right),
                                self.getSequence(*exps))
                else:
                    qres = Storage({'op': BELONGS,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                   'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        BELONGS
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or BELONGS
            ), name='BELONGS', err=err)

class clsON(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (is_tableType(left) is True):
                qres = DLGExpression(left.objp4, ON, left, right)
            elif (type(left).__name__ == 'Storage'):
                qres = Storage({'op': ON, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, ON, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, ON, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (re.match(f"^{right}", left) is not None),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                try:
                    (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                except:
                    (tablename, fieldname, value, op) = (None, None, None, None)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.match(f"^{right}.*$", left) is not None),
                                  self.getSequence(*exps))
                else:
                    qres = Storage({'op': STARTSWITH,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                   'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        STARTSWITH
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or STARTSWITH
            ), name='STARTSWITH', err=err)


''' In case it comes up... but it could 
    cause more confusion than anything else.
    At some point, this will be removed
'''
clsIN = type('clsIN', clsBELONGS.__bases__, dict(clsBELONGS.__dict__))

class clsSTARTSWITH(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': STARTSWITH, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, STARTSWITH, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, STARTSWITH, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (re.match(f"^{right}", left) is not None),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                try:
                    (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                except:
                    (tablename, fieldname, value, op) = (None, None, None, None)
                if ((tablename, fieldname) == (None, None)):
                    condition = (re.match(f"^{right}.*$", left) is not None)
                    qres = reduce(lambda left, right: condition,
                                  self.getSequence(*exps))
                else:
                    qres = Storage({'op': STARTSWITH,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                   'right': value
                                    }
                                   )
                return qres
            else:
                qres = self.build_query(
                                    left,
                                    right,
                                    STARTSWITH
            )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or STARTSWITH
            ), name='STARTSWITH', err=err)

class clsENDSWITH(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': ENDSWITH, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, ENDSWITH, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, ENDSWITH, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (re.match(f".*{right}$", left) is not None),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                try:
                    (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                except:
                    (tablename, fieldname, value, op) = (None, None, None, None)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.match(f"^.*{right}$", left) is not None),
                                  self.getSequence(*exps))
                else:
                    qres = Storage({'op': ENDSWITH,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                   'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        ENDSWITH
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or ENDSWITH
            ), name='ENDSWITH', err=err)

class clsMATCH(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': MATCH, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, MATCH, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, MATCH, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (re.match(f"{right}", left) is not None),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                try:
                    (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                except:
                    (tablename, fieldname, value, op) = (None, None, None, None)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.match(f"{right}", left) is not None),
                                  self.getSequence(*exps))
                else:
                    qres = Storage({'op': MATCH,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                    },
                                   'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        MATCH
                )
            return qres
        except Exception as err:
            op_error(lambda left, right: (
                left,
                right,
                op or MATCH
            ), name='MATCH', err=err)

class clsSEARCH(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': SEARCH, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, SEARCH, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, SEARCH, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (re.search(f"{right}", left) is not None),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                try:
                    (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                except:
                    (tablename, fieldname, value, op) = (None, None, None, None)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.search(f"{right}", left) is not None),
                              self.getSequence(*exps))
                else:
                    qres = Storage({'op': SEARCH,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                   'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        SEARCH
                )
            return qres
        except Exception as err:
            op_error(lambda left, right,: (
                left,
                right,
                op or SEARCH
            ), name='SEARCH', err=err)

class clsADD(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': ADD, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, ADD, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, ADD, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left + right), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left + right), self.getSequence(*exps))
                else:
                    qres = Storage({'op': ADD,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                   'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        ADD
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or ADD
            ), name='ADD', err=err)

class clsSUB(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': SUB, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, SUB, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, SUB, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left - right), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left - right), self.getSequence(*exps))
                else:
                    qres = Storage({'op': SUB,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        SUB
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or SUB
            ), name='SUB', err=err)

class clsMUL(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': MUL, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, MUL, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, MUL, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left * right), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left * right), self.getSequence(*exps))
                else:
                    qres = Storage({'op': MUL,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        MUL
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MUL
            ), name='MUL', err=err)

class clsTRUEDIV(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': TRUEDIV, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, TRUEDIV, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, TRUEDIV, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left / right), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left / right), self.getSequence(*exps))
                else:
                    qres = Storage({'op': TRUEDIV,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        TRUEDIV
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or TRUEDIV
            ), name='TRUEDIV', err=err)


class clsMOD(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': MOD, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, MOD, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, MOD, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left % right), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left / right), self.getSequence(*exps))
                else:
                    qres = Storage({'op': MOD,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        MOD
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MOD
            ), name='MOD', err=err)

class clsSUM(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        (current_type, left_right_are_valid) = self.validate_field_type(left, right)
        try:
            if AND((current_type is None),
                   (is_array(left) is True)):
                qsum = sum([float(item) for item in left])
            elif AND(
                        (current_type is not None),
                        (left_right_are_valid is True)
            ):
                qsum = sum([float(record[left.fieldname]) for record in right])
            qsum = current_type(
                round(qsum) \
                    if ((qsum % 1) == 0) \
                    else round(qsum, 2)
            )
            return current_type(qsum)
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MAX
            ),name='MAX', err=err)

class clsAVG(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        (current_type, left_right_are_valid) \
            = self.validate_field_type(left, right)
        (values, average) = (set(), None)
        length = len(left) \
            if (isinstance(left, tuple) is True) \
            else len(right) \
            if (type(right).__name__ == 'DLGRecords') \
            else 0
        try:
            if AND((current_type is None),
                   (is_array(left) is True)):
                current_type = type(left[0])
                for value in left:
                    values.add(float(value))
                fsum = sum([float(value) for value in left])
                average = (fsum / length)
            elif AND((current_type is not None),
                     (left_right_are_valid is True)):
                for record in right:
                    value = record[left.fieldname]
                    values.add(float(value))
                fsum = sum(
                            [float(record[left.fieldname]) for record in right]
                )
                average = (fsum / length)
            average = current_type(
                round(average) \
                if ((average % 1) == 0) \
                else round(average, 2)
            )
            return average
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MAX
            ),name='MAX', err=err)

class clsABS(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        (current_type, left_right_are_valid) \
            = self.validate_field_type(left, right)
        try:
            if AND((current_type is None),
                   (is_array(left) is True)):
                if (isinstance(left, (str, int))):
                    left_type = type(left)
                    if (left_type is str):
                        left = float(left)
                    left = abs(left)
                    left = round(left) \
                        if ((left % 1) == 0) \
                        else round(left, 2)
                    return left_type(left)
            elif AND((current_type is not None),
                     (left_right_are_valid is True)):
                updated_records = Lst()
                for record in right:
                    current_value = record[left.fieldname]
                    updvalue = abs(float(current_value))
                    updvalue = current_type(
                        round(updvalue) \
                            if ((updvalue % 1) == 0) \
                            else round(updvalue, 2)
                    )
                    record.merge({left.fieldname: updvalue})
                    if (record.code is not None):
                        record.delete('code')
                    updated_records.append(record)
                return updated_records
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or ABS
            ), name='ABS', err=err)

class clsMIN(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        (current_type, left_right_are_valid) \
            = self.validate_field_type(left, right)
        (values, minvalue) = (set(), None)
        try:
            if AND((current_type is None),
                   (is_array(left) is True)):
                current_type = type(left[0])
                for value in left:
                    values.add(float(value))
                minvalue = min(values)
            elif AND((current_type is not None),
                     (left_right_are_valid is True)):
                for record in right:
                    value = record[left.fieldname]
                    values.add(float(value))
                minvalue = min(values)
            minvalue = current_type(
                                round(minvalue) \
                                    if ((minvalue % 1) == 0) \
                                    else round(minvalue, 2)
            )
            return minvalue
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MAX
            ), name='MAX', err=err)

class clsMAX(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        (current_type, left_right_are_valid) \
            = self.validate_field_type(left, right)
        (values, maxvalue) = (set(), None)
        try:
            if AND((current_type is None),
                   (is_array(left) is True)):
                current_type = type(left[0])
                for value in left:
                    values.add(float(value))
                maxvalue = max(values)
            elif AND((current_type is not None),
                     (left_right_are_valid is True)):
                for record in right:
                    value = record[left.fieldname]
                    values.add(float(value))
                maxvalue = max(values)

            maxvalue = current_type(round(maxvalue) \
                if ((maxvalue % 1) == 0) \
                else round(maxvalue, 2))
            return maxvalue
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MAX
            ), name='MAX', err=err)

class clsCASE(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': CASE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, CASE, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, CASE, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left ^ right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left ^ right),
                                  self.getSequence(*exps))
                else:
                    qres = Storage({'op': CASE,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(left, right, CASE)
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or CASE
            ), name='CASE', err=err)

class clsCASEELSE(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right, other) = (exps(0), exps(1), exps(2))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': CASEELSE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, CASEELSE, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, CASEELSE, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right, other: ((left ^ right) | other),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right, other: ((left ^ right) | other),
                                  self.getSequence(*exps))
                else:
                    qres = Storage({'op': CASEELSE,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        CASEELSE
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op, other,: (
                left,
                right,
                op or CASEELSE,
                other
            ), name='CASEELSE', err=err)

class REGEX(QClass):
    ''' case sensitive '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        OPERATOR = None
        if (isinstance(left, str)):
            if (left.startswith('%') is True):
                (left, OPERATOR) = (f"^{left}$", CONTAINS) \
                    if (left.endswith('%') is True) \
                    else (f"{left}$", ENDSWITH)
            else:
                (left, OPERATOR) = (f"^{left}", STARTSWITH) \
                    if (left.endswith('%') is True) \
                    else (f"^{left}$", CONTAINS)
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': LIKE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, LIKE, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, LIKE, left, right)
            elif AND(
                    (isinstance(left, (bool, int))),
                    (isinstance(right, (bool, int)))
            ):
                ismatch = (re.search(left, right) is not None)
                qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    ismatch = (re.search(left, right) is not None)
                    qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
                else:
                    qres = Storage({'op': LIKE,
                                    'left': {
                                        'tablename': tablename,
                                        'fieldname': fieldname
                                    },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                    left,
                    right,
                    OPERATOR
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or LIKE
            ), name='LIKE', err=err)

class clsLIKE(QClass):
    ''' case sensitive '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        left = re.sub('%', '', left)
        OPERATOR = None
        if (isinstance(left, str)):
            if (left.startswith('%') is True):
                (left, OPERATOR) = (f"^{left}$", CONTAINS) \
                    if (left.endswith('%') is True) \
                    else (f"{left}$", ENDSWITH)
            else:
                (left, OPERATOR) = (f"^{left}", STARTSWITH) \
                    if (left.endswith('%') is True) \
                    else (f"^{left}$", CONTAINS)
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': LIKE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, LIKE, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, LIKE, left, right)
            elif AND(
                        (isinstance(left, (bool, int))),
                        (isinstance(right, (bool, int)))
            ):
                ismatch = (re.search(left, right) is not None)
                qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    ismatch = (re.search(left, right) is not None)
                    qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
                else:
                    qres = Storage({'op': LIKE,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        OPERATOR
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or LIKE
            ), name='LIKE', err=err)

class clsILIKE(QClass):
    ''' case insensitive '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        left = re.sub('%', '', left).lower()
        OPERATOR = None
        if (isinstance(left, str)):
            if (left.startswith('%') is True):
                (left, OPERATOR) = (f"^{left}$", CONTAINS) \
                    if (left.endswith('%') is True) \
                    else (f"{left}$", ENDSWITH)
            else:
                (left, OPERATOR) = (f"^{left}", STARTSWITH) \
                    if (left.endswith('%') is True) \
                    else (f"^{left}$", CONTAINS)
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': LIKE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, LIKE, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, LIKE, left, right)
            elif AND(
                        (isinstance(left, (bool, int))),
                        (isinstance(right, (bool, int)))
            ):
                ismatch = (re.search(left, right) is not None)
                qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    ismatch = (re.search(left, right) is not None)
                    qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
                else:
                    qres = Storage({'op': LIKE,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        OPERATOR
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or ILIKE
            ), name='LIKE', err=err)

class clsLOWER(QClass):
    ''' convert a field value to lower case -> it should also support like(value%), so implement it!
    >>> name = f'{name[0:3]}%'
    >>> for rec in oP4(oP4.user.user.lower().like(name)).select():
    >>>     print(rec.user)
    mart
    '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': LOWER, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, LOWER, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, LOWER, left, right)
            elif (isinstance(left, (bool, int))):
                return left.lower()
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if (tablename, fieldname) == (None, None):
                    qres = left.lower()
                else:
                    qres = Storage({'op': LOWER,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                   'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        LOWER
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or LOWER
            ), name='LOWER', err=err)

class clsUPPER(QClass):
    ''' convert a field value to upper case -> it should also support like(value%), so implement it!
        >>> name = f'{name[0:3]}%'
        >>> for rec in oP4(oP4.user.user.upper().like(name)).select():
        >>>     print(rec.user)
        MART
        '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': UPPER, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, UPPER, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, UPPER, left, right)
            elif (isinstance(left, (bool, int))):
                qres = reduce(lambda left: left.upper(), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if (tablename, fieldname) == (None, None):
                    qres = left.upper()
                else:
                    qres = Storage({'op': UPPER,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        UPPER
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or UPPER
            ), name='UPPER', err=err)


class clsEPOCH(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        oDateTime = DLGDateTime()
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': EPOCH, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, EPOCH, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, EPOCH, left, right)
            elif (isinstance(left, (int, float, datetime.datetime)) is True):
                qres = oDateTime.to_epoch(left)
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    left = oDateTime.to_p4date(left)
                    qres = oDateTime.to_epoch(left)
                else:
                    qres = Storage({'op': EPOCH, 'left':
                        {'tablename': tablename, 'fieldname': fieldname},
                                    'right': value})
            else:
                qres = self.breakdown_query(left, right, EPOCH)
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or EPOCH
            ), name='EPOCH', err=err)


class clsDIFF(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': DIFF, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, DIFF, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, DIFF, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left - right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = getTableOpKeyValue(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left - right),
                                  self.getSequence(*exps))
                else:
                    qres = Storage({'op': DIFF,
                                    'left': {
                                                'tablename': tablename,
                                                'fieldname': fieldname
                                            },
                                    'right': value
                                    }
                                   )
            else:
                qres = self.build_query(
                                        left,
                                        right,
                                        DIFF
                )
            return qres
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or DIFF
            ), name='DIFF', err=err)

class clsCOUNT(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            qcount = len(left) \
                if (isinstance(left, tuple) is True) \
                else len(right) \
                if (type(right).__name__ == 'DLGRecords') \
                else 0
            return qcount
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MAX
            ), name='MAX', err=err)

class clsBETWEEN(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsPRIMARYKEY(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsCLOAESCE(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsCOALESCEZERO(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsEXTRACT(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsSUBSTRING(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsDATE(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsDATETIME(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsTIME(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsYEAR(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsMONTH(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsDAY(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsHOUR(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsMINUTE(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsSECOND(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsJOIN(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsLEFT(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

''' DLGQuery & DLGExpression live here now to avoid circular imports
'''

"""
<DLGQuery {'left': <DLGQuery {'left': <Py4Field depotFile>,
                            'objp4': <Py4 anastasia.local:1777 >,
                            'op': <function ENDSWITH at 0x1062123e0>,
                            'right': '\\.py'}>,
          'objp4': <Py4 anastasia.local:1777 >,
          'op': <function AND at 0x168224f40>,
          'right': <DLGExpression {'left': <DLGQuery {'left': <DLGQuery {'left': <Py4Field type>,
                                                                       'objp4': <Py4 anastasia.local:1777 >,
                                                                       'op': <function EQ at 0x106211ee0>,
                                                                       'right': 'ktext'}>,
                                                     'objp4': <Py4 anastasia.local:1777 >,
                                                     'op': <function AND at 0x168224f40>,
                                                     'right': <DLGExpression {'left': <Py4Field action>,
                                                                              'objp4': <Py4 anastasia.local:1777 >,
                                                                              'op': <function MATCH at 0x106212480>,
                                                                              'right': 'add|edit'}>}>,
                                    'objp4': <Py4 anastasia.local:1777 >,
                                    'op': <function NOT at 0x168225120>,
                                    'right': None}>}>


AND(
    (oP4.files.depotFile.endswith('\.py')), 
    NOT(
        AND(
            (oP4.files.type == 'ktext'), 
            (oP4.files.action.match('add|edit'))
            )
        )
)                                    
 """


class DLGQuery(object):
    def __init__(
             self,
             objp4,
             op,
             left=None,
             right=None,
             inversion=False,
             *args,
             **kwargs
    ):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        self.objp4 = objp4

        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.objp4.logger \
                        if (hasattr(self.objp4, 'logger')) \
                        else kwargs.logger or 'INFO',
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
        )
        ]

        self.op = op
        self.left = left
        self.right = right
        self.inversion=kwargs.inversion or inversion
        self.oDate = DLGDateTime()

        if OR(
                (is_field_tableType(self) is False),
                (is_field_tableType(right) is False)
        ):
            self.set_fieldname_tablename()

        opname = op.__name__ \
            if (callable(op) is True) \
            else op
        if (opname in notops):
            self.inversion = True

    def __getattr__x(self, name):
        if (name in ('fieldname', 'tablename')):
            try:
                return  (
                    object.__getattribute__(self, name)
                )
            except: pass

    def set_fieldname_tablename(self):
        (
            left,
            right,
            fieldname,
            tablename
        ) = \
            (
                self.left,
                self.right,
                None,
                None
            )

        '''
        if AND(
                (not hasattr(self, 'fieldname')),
                 (not hasattr(self, 'tablename'))
        ):
            if (left is not None):
                sumtypes = sum(
                    (
                        is_queryType(left),
                        is_expressionType(left),
                        is_dictType(left)
                    )
                )
                if OR(
                        (sumtypes > 1),
                        is_field_tableType(left)
                ):
                    for item in ('fieldname', 'tablename'):
                        if (item is not None):
                            setattr(self, item, left[item])
        '''

    def __repr__(self):
        qdict = Storage(
            {
                'objp4': self.objp4,
                'op': self.op,
                'left': self.left,
                'right': self.right,
                'inversion': self.inversion
            }
        )
        return f'<DLGQuery {qdict}>'

    __str__ = __repr__

    __hash__ = lambda self: hash((frozenset(self), frozenset(self.objp4)))

    def __and__(self, value):
        return DLGQuery(self.objp4, AND, self, value)

    def __or__(self, value):
        return DLGQuery(self.objp4, OR, self, value)

    def __xor__(self, value):
        return DLGQuery(self.objp4, XOR, self, value)

    __rand__ = __and__
    __ror__ = __or__

    def __invert__(self):
        self.inversion = True
        return DLGQuery(self.objp4, NOT, self, inversion=True)

    def __eq__(self, value):
        return DLGQuery(self.objp4, EQ, self, value)

    def __ne__(self, value):
        return DLGQuery(self.objp4, NE, self, value)

    def case(self, true=1, false=0):
        return DLGExpression(self.objp4, CASE, self, (True, False))

    def as_dict(self, flat=False):
        def loop(q):
            qdict = dict()
            for key, value in q.items():
                if key in ("left", "right"):
                    qdict[key] = loop(value.__dict__) \
                        if ((isinstance(value, self.__class__)) or
                            (type(value).__name__ == fieldType(self.objp4))) \
                        else {"tablename": value.tablename, "fieldname": value.fieldname} \
                        if (isinstance(value, DLGExpression)) \
                        else self.oDate.to_string(value) \
                        if (isinstance(value, (date, time, datetime))) \
                        else value
                elif (key == 'op'):
                    qdict[key] = value.__name__ if callable(value) else value
                elif (isinstance(value, serializable)):
                    qdict[key] = loop(value) if (isinstance(value, dict)) else value
            return qdict

        if flat:
            return Storage(loop(self.__dict__))
        else:
            resd = Storage()
            for (key, value) in Storage(self.__dict__).getitems():
                if key in ('objp4', 'op', 'left', 'right', 'fieldname', 'tablename'):
                    resd.merge({key: value})
            return resd

class DLGExpression(object):

    __hash__ = lambda self: hash((frozenset(self), frozenset(self.objp4)))

    def __or__(self, value):
        return Lst(self, value)

    def __init__(
            self,
            objp4,
            op,
            left=None,
            right=None,
            inversion=False,
            *args,
            **kwargs
    ):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        self.objp4 = objp4

        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.objp4.logger \
                        if (hasattr(self.objp4, 'logger')) \
                        else kwargs.logger or 'INFO',
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
        )
        ]

        self.op = op
        self.left = left
        self.right = right
        self.oDate = DLGDateTime()
        if AND((not hasattr(self, 'fieldname')), (hasattr(self.left, 'fieldname'))):
            self.fieldname = left.fieldname

        if AND((not hasattr(self, 'tablename')), (hasattr(self.left, 'tablename'))):
            self.tablename = left.tablename

        opname = op.__name__ \
            if (callable(op) is True) \
            else op
        self.inversion = inversion

        if (opname in notops):
            self.inversion = True

        if (is_tableType(self) is False):
            if (None in (
                            self.fieldname,
                            self.tablename
                    )
            ):
                if (left is not None):
                    sumtypes = sum(
                                    (
                                        is_queryType(left),
                                        is_expressionType(left),
                                        is_dictType(left)
                                    )
                               )
                    if OR(
                            (sumtypes > 1),
                            is_field_tableType(left)
                    ):
                        if (hasattr(left, 'fieldname')):
                            self.fieldname = left.fieldname
                        if (hasattr(left, 'tablename')):
                            self.tablename = left.tablename

        self.type = right.type \
            if ((right is not None) and (hasattr(right, 'type'))) \
            else type(right)

        if isinstance(self.type, str):
            self.itype = reg_type.match(self.type).group(0)

    def __repr__(self):
        qdict = pformat(
            {
                'objp4': self.objp4,
                'op': self.op,
                'left': self.left,
                'right': self.right,
                'inversion': self.inversion
            }
        )
        return f'<DLGExpression {qdict}>'

    def __repr__new(self):
        qdict = self.as_dict()
        diff = qdict.getkeys().diff(['objp4', 'op', 'left', 'right', 'inversion'])
        qdict.delete(*diff)
        return f'<DLGExpression {qdict}>'

    __str__ = __repr__

    def __str__Old(self):
        qdict = pformat(
            {
                'objp4': self.objp4,
                'op': self.op,
                'left': self.left,
                'right': self.right,
                'inversion': self.inversion
            }
        )
        return f'<DLGExpression {qdict}>'

    def as_dict(self, flat=False):
        def loop(d):
            newd = dict()
            for k, v in d.items():
                if (k in ("left", "right")):
                    if (isinstance(v, self.__class__)):
                        newd[k] = loop(v.__dict__)
                    elif (type(v).__name__ == fieldType(self.objp4)):
                        newd[k] = {"tablename": v.tablename,
                                   "fieldname": v.name}
                    elif (isinstance(v, DLGExpression)):
                        newd[k] = loop(v.__dict__)
                    elif (isinstance(v, serializable)):
                        newd[k] = v
                    elif (isinstance(v, (date, time, datetime))):
                        newd[k] = self.oDate.to_string(v)
                elif (k == "op"):
                    if (callable(v)):
                        newd[k] = v.__name__
                    elif (isinstance(v, basestring)):
                        newd[k] = v
                    else:
                        pass
                elif (isinstance(v, serializable)):
                    newd[k] = loop(v) \
                        if (isinstance(v, dict)) \
                        else v
            return newd

        if flat:
            return Storage(loop(self.__dict__))
        else:
            resd = Storage()
            for (key, value) in Storage(self.__dict__).getitems():
                if key in ('objp4', 'op', 'left', 'right', 'fieldname', 'tablename'):
                    resd.merge({key: value})
            return resd

    def __getitem__(self, i):
        if (isinstance(i, slice) is True):
            (start, stop) = (i.start or 0, i.stop)
            pos0 = '(%s - %d)' % (self.len(), abs(start) - 1) \
                if (start < 0) \
                else (start + 1)
            maxint = sys.maxsize
            length = self.len() \
                if ((stop is None) or (stop == maxint)) \
                else f'({self.len()} - {(abs(stop) - 1)} - {pos0})' \
                if (stop < 0) \
                else f'({stop + 1} - {pos0})'
            return DLGExpression(self.objp4, SUBSTRING, self, (pos0, length))
        else:
            return self[i:(i + 1)]

    def __invert__(self):
        ''' Overloading a query's operator causes grief when
            begining the query with `~` or `NOT()` operator.
        '''
        self.inversion = True
        return DLGQuery(self.objp4, NOT, self, inversion=True)

    def __eq__(self, value):
        return DLGQuery(self.objp4, EQ, self, value)

    def __ne__(self, value):
        return DLGQuery(self.objp4, NE, self, value)

    def __lt__(self, value):
        return DLGQuery(self.objp4, LT, self, value)

    def __le__(self, value):
        return DLGQuery(self.objp4, LE, self, value)

    def __gt__(self, value):
        return DLGQuery(self.objp4, GT, self, value)

    def __ge__(self, value):
        return DLGQuery(self.objp4, GE, self, value)

    def __len__(self):
        return lambda i: len(self.__dict__[i])

    def __contains__(self, value):
        return DLGQuery(self.objp4, CONTAINS, self, value)

    def belongs(self, value):
        ''' USAGE:

                straightup belongs:

                >>> clientnames = ('myclient', 'pycharmclient', otherclient')
                >>> clientrecords = oJnl(oJnl.domain.name.belongs(clientnames)).select()

                nested belongs:

                >>> qry1 = AND((oJnl.domain.type == '99'), (oJnl.domain.owner == 'mart'))
                >>> myclients = oJnl(qry1)._select(oJnl.domain.name)
                >>> qry2 = (oJnl.domain.name.belongs(myclients))
                >>> clientrecords = oJnl(qry2).select()
        '''
        return DLGExpression(self.objp4, BELONGS, self, value)

    def contains(self, value):
        ''' USAGE:

                >>> qry = (oP4.files.depotFile.contains('/myProjectName'))
                >>> record = oP4(qry).select()(0) (or .first())
        '''
        return DLGQuery(self.objp4, CONTAINS, self, value)

    def startswith(self, value):
        ''' USAGE:
                >>> qry = (oP4.files.depotFile.startswith('//dev/'))
                >>> records = oP4(qry).select()
        '''
        return DLGQuery(self.objp4, STARTSWITH, self, value)

    def endswith(self, value):
        ''' USAGE:
                >>> qry = (oP4.files.depotFile.endswith('.py'))
                >>> records = oP4(qry).select()
        '''
        return DLGQuery(self.objp4, ENDSWITH, self, value)

    def match(self, value):
        ''' USAGE:
                >>> qry = (oP4.files.depotFile.match('^\/\/dev\/.*.py$'))
                >>> records = oP4(qry).select()
        '''
        return DLGExpression(self.objp4, MATCH, self, value)

    def search(self, value):
        ''' USAGE:
                >>> qry = (oP4.clients.client.search('art'))
                >>> records = oP4(qry).select()
        '''
        return DLGExpression(self.objp4, SEARCH, self, value)

    def count(self):
        return DLGExpression(self.objp4, COUNT, self, None)

    def sum(self):
        return DLGExpression(self.objp4, SUM, self, None)

    def max(self):
        return DLGExpression(self.objp4, MAX, self, None)

    def min(self):
        return DLGExpression(self.objp4, MIN, self, None)

    def len(self):
        return DLGExpression(self.objp4, len, self, None)

    def avg(self):
        return DLGExpression(self.objp4, AVG, self, None)

    def abs(self):
        return DLGExpression(self.objp4, ABS, self, None)

    def lower(self):
        return DLGExpression(self.objp4, LOWER, self, None)

    def upper(self):
        return DLGExpression(self.objp4, UPPER, self, None)

    def epoch(self):
        return DLGExpression(self.objp4, EPOCH, self, None)

    def like(self, value, case_sensitive=True):
        op = case_sensitive and LIKE or ILIKE
        return DLGQuery(self.objp4, op, self, value)

    def ilike(self, value):
        return self.like(value, case_sensitive=False)

    def on(self, reference):
        ''' `ON` the SQL conditional clause for joining. AT any rate that's what is should be, however
            I got the idea to simply use ON as the springboard to defined join routines... it may or
            may not turn out to be a good idea.

            USAGE:

            join (innerjoin)

                >>> table_1 = jnl.rev
                >>> table_2 = jnl.change
                >>> fieldname = 'change'
                >>> reference = (table_1[fieldname] == table_2[fieldname])

                >>> records = jnl(table_1).select(
                                join=table_2.on(reference)
                                )

                    or, this syntax is also supported:

                >>> records = jnl(reference).select()

            left (left outerjoin)

                >>> table_1 = jnl.rev
                >>> table_2 = jnl.change
                >>> fieldname = 'change'
                >>> reference = (table_1[fieldname] == table_2[fieldname])

                >>> records = jnl(table_1).select(
                                table_1.ALL, table_2.ALL,
                                left=table_2.on(reference),
                                orderby=table_2.change
                                )
        '''
        return DLGExpression(self.objp4, ON, self, reference)

    def regexp(self, value):
        return DLGQuery(self.objp4, REGEX, self, value)

    def year(self, value):
        return DLGExpression(self.objp4, YEAR, self, value)

    def month(self, value):
        return DLGExpression(self.objp4, MONTH, self, value)

    def day(self, value):
        return DLGExpression(self.objp4, DAY, self, value)

    def hour(self, value):
        return DLGExpression(self.objp4, HOUR, self, value)

    def minute(self, value):
        return DLGExpression(self.objp4, MINUTE, self, value)

    def second(self, value):
        return DLGExpression(self.objp4, SECOND, self, value)

    def __add__(self, value):
        return DLGExpression(self.objp4, ADD, self, value)

    def __sub__(self, value):
        return DLGExpression(self.objp4, SUB, self, value)

    def __mul__(self, value):
        return DLGExpression(self.objp4, MUL, self, value)

    def __div__(self, value):
        return self.__truediv__(value)

    def __truediv__(self, value):
        return DLGExpression(self.objp4, TRUEDIV, self, value)

    def __mod__(self, value):
        return DLGExpression(self.objp4, MOD, self, value)


''' rependant table, field & op stuff 
'''
containlike_ops = (CONTAINS, STARTSWITH, ENDSWITH, '#', '#^', '#$')

def all_ops_table(op=None):
    '''
        always return the FunctionType

        USAGE:

            >>> op = '>='
            >>> opvalue = optable(op)
            >>> opvalue
            GE

            >>> op = 'GE'
            >>> opvalue = optable(op)
            >>> opvalue
            GE

            >>> op = GE
            >>> opvalue = optable(op)
            >>> opvalue
            GE

            >>> op = None
            >>> opvalue = optable(None)
            >>> opvalue
            {"=": EQ,
             "EQ: EQ,
             ...
             }
    '''
    ops = objectify(
        {
            "=":            EQ,
            "EQ":           EQ,
            "!=":           NE,
            "NE":           NE,
            "<":            LT,
            "LT":           LT,
            ">":            GT,
            "GT":           GT,
            "<=":           LE,
            "LE":           LE,
            ">=":           GE,
            "GE":           GE,
            "#":            CONTAINS,
            "CONTAINS":     CONTAINS,
            "#^":           STARTSWITH,
            "STARTSWITH":   STARTSWITH,
            "#$":           ENDSWITH,
            "ENDSWITH":     ENDSWITH,
            "|":            OR,
            "OR":           OR,
            'or':           OR,
            '&':            AND,
            "AND":          AND,
            'and':          AND,
            '#?':           SEARCH,
            "SEARCH":       SEARCH,
            '##':           MATCH,
            "MATCH":        MATCH,
            "NOT":          NOT,
        }
    )
    '''
    
    
    '''
    if (op is None):
        ''' return everything '''
        return ops
    if (op in ops.getkeys()):
        ''' return ops value from key '''
        return ops[op]
    elif (op in ops.getvalues()):
        ''' key is in values, return it '''
        return op
def optable(op=None):
    '''
        always return the FunctionType

        USAGE:

            >>> op = '>='
            >>> opvalue = optable(op)
            >>> opvalue
            GE

            >>> op = 'GE'
            >>> opvalue = optable(op)
            >>> opvalue
            GE

            >>> op = GE
            >>> opvalue = optable(op)
            >>> opvalue
            GE

            >>> op = None
            >>> opvalue = optable(None)
            >>> opvalue
            {"=": EQ,
             "EQ: EQ,
             ...
             }
    '''
    ops = objectify(
        {
            "=":            EQ,
            "EQ":           EQ,
            "!=":           NE,
            "NE":           NE,
            "<":            LT,
            "LT":           LT,
            ">":            GT,
            "GT":           GT,
            "<=":           LE,
            "LE":           LE,
            ">=":           GE,
            "GE":           GE,
            "|":            OR,
            "OR":           OR,
            'or':           OR,
            '&':            AND,
            "AND":          AND,
            'and':          AND,

            "#":            CONTAINS,
            "CONTAINS":     CONTAINS,
            "#^":           STARTSWITH,
            "STARTSWITH":   STARTSWITH,
            "#$":           ENDSWITH,
            "ENDSWITH":     ENDSWITH,

            '#?':           SEARCH,
            "SEARCH":       SEARCH,
            '##':           MATCH,
            "MATCH":        MATCH,

            "NOT":          NOT,

            "~":            INVERT,
            "!":            INVERT,
            "INVERT":       INVERT,

            '^':            DIFF,


        }
    )

    if (op is None):
        ''' return everything '''
        return ops
    if (op in ops.getkeys()):
        ''' return ops value from key '''
        return ops[op]
    elif (op in ops.getvalues()):
        ''' key is in values, return it '''
        return op

def expression_table(op):
    ''' if op is not None:
            returns the FunctionType (if op exists in keys or values) or None
        if op is None:
            returns all
    '''
    expression_ops = Storage(
        {
            "LOWER": LOWER,
            "UPPER": UPPER,
            "ADD": ADD,
            "SUB": SUB,
            "MUL": MUL,
            "MOD": MOD,
            "BETWEEN": BETWEEN,
            "CASE": CASE,
            "CASEELSE": CASEELSE,
            "DIFF": DIFF,
            "JOIN": JOIN,
            "LEFT": LEFT,
            "PRIMARYKEY": PRIMARYKEY,
            "COALESCE": COALESCE,
            "COALESCEZERO": COALESCEZERO,
            "COUNT": COUNT,
            "EXTRACT": EXTRACT,
            "SUBSTRING": SUBSTRING,
            "LIKE": LIKE,
            "ILIKE": ILIKE,
            "SUM": SUM,
            "ABS": ABS,
            "AVG": AVG,
            "MIN": MIN,
            "MAX": MAX,
            "BELONGS": BELONGS,
            "TRUEDIV": TRUEDIV,
            "YEAR": YEAR,
            "MONTH": MONTH,
            "DAY": DAY,
            "HOUR": HOUR,
            "MINUTE": MINUTE,
            "SECOND": SECOND,
            "CONTAINS": CONTAINS,
            "STARTSWITH": STARTSWITH,
            "ENDSWITH": ENDSWITH,
            "SEARCH": SEARCH,
            "MATCH": MATCH
        }
    )
    ''' return everything '''
    if (op is None):
        return None
    ''' return value from key '''
    if (op in expression_ops.getkeys()):
        return expression_ops[op]
    elif (op in expression_ops.getvalues()):
        return op

andops = (AND, 'AND', '&')
orops = (OR, 'OR', '|')
xorops = (XOR, 'XOR', '^')
notops = (NOT, 'NOT', '~')

dops = Storage({'equal': (EQ, "EQ", '='),
                'notequal': (NE, "NE", '!='),
                'greaterthan': (GT, "GT", '>'),
                'greaterequal': (GE, "GE", '>='),
                'lesserthan': (LT, "LT", '<'),
                'lesserequal': (LE, "LE", '<='),
 \
                'not': (NOT, 'NOT', '~'),
                'contains': (CONTAINS, 'CONTAINS', '#'),
 \
                'startswith': (STARTSWITH, 'STARTSWITH', '#^'),
                'endswith': (ENDSWITH, 'ENDSWITH', '#$'),
                'match': (MATCH, 'MATCH', '##'),
                'search': (SEARCH, 'SEARCH', '#?'),
 \
                'and': andops,
                'or': orops,
                'xor': xorops})

ops = Storage({EQ: ("EQ", '='),
               NE: ("NE", '!='),
               GT: ("GT", '>'),
               GE: ("GE", '>='),
               LT: ("LT", '<'),
               LE: ("LE", '<='),
 \
               NOT: ('NOT', 'not'),
               CONTAINS: ('CONTAINS', '#'),
 \
               STARTSWITH: ('STARTSWITH', '#^'),
               ENDSWITH: ('ENDSWITH', '#$'),
               MATCH: ('MATCH', '##'),
               SEARCH: ('SEARCH', '#?'),
 \
               AND: ('AND', '&'),
               OR: ('OR', '|'),
               XOR: ('XOR', '^')})
