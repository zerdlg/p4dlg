import re
import sys
from datetime import date, time, datetime
from functools import reduce
from pprint import pformat
from types import FunctionType

from libdlg.dlgStore import Lst, ZDict, StorageIndex
from libdlg.dlgDateTime import DLGDateTime
from libdlg.dlgUtilities import (
    getTableOpKeyValue,
    bail,
    serializable,
    isnum,
    is_array,
    basestring,
    isanyfile
)
from libsql.sqlValidate import *
from libsql.sqlRecords import Records

'''  [$File: //dev/p4dlg/libsql/sqlQuery.py $] 
     [$Change: 678 $] 
     [$Revision: #17 $]
     [$DateTime: 2025/04/01 04:47:46 $]
     [$Author: zerdlg $]
'''

__all__ = [
           'ops', 'andops', 'orops', 'xorops', 'notops', 'dops',
           'optable', 'expression_table', 'all_ops_table',
           'QClass',
           'NOT', 'AND', 'OR', 'XOR',
           'EQ', 'NE', 'GE', 'GT', 'LE', 'LT',
           'CONTAINS', 'ENDSWITH', 'STARTSWITH',
           'ADD', 'SUB', 'MUL', 'MOD', 'ALLOW_NONE',
           'CASE', 'CASEELSE', 'DIFF', 'MATCH', 'SEARCH',
           'LOWER', 'UPPER', 'JOIN', 'JOIN_LEFT',
           'PRIMARYKEY', 'COALESCE', 'COALESCE_ZERO',
           'EXTRACT', 'SUBSTRING', 'LIKE', 'ILIKE',
           'SUM', 'ABS', 'LEN', 'DIV',
           'AVG', 'MIN', 'MAX', 'BELONGS', 'IN',
           'TRUEDIV', 'COUNT',
           'YEAR', 'MONTH', 'DAY',
           'DLGQuery', 'DLGExpression',
]

objectify = ZDict.objectify

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
        (args, kwargs) = (Lst(args), ZDict(kwargs))
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

        opname = op.__name__ \
            if (callable(op) is True) \
            else op
        if (opname in notops):
            self.inversion = True

    def __repr__(self):
        qdict = ZDict(
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

    #__hash__ = lambda self: hash((frozenset(self), frozenset(self.objp4)))

    __hash__ = object.__hash__
    __iter__ = lambda self: self.__dict__.__iter__()

    def copy(self):
        return DLGQuery(self.objp4, self.op, self.left, self.right, self.inversion)

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
        return DLGExpression(self.objp4, CASE, self, (true, false))

    def as_dict(self, flat=False):
        def recurse(obji):
            objii = dict()
            for (key, value) in obji.items():
                if (key in ("left", "right")):
                    objii[key] = recurse(value.__dict__) \
                        if (
                            (isinstance(value, self.__class__)) |
                            (is_fieldType(value) is True)
                    ) \
                        else {
                        "tablename": value.tablename,
                        "fieldname": value.fieldname
                    } \
                        if (is_expressionType(value) is True) \
                        else self.oDate.to_string(value) \
                        if (isinstance(value, (date, time, datetime))) \
                        else value
                elif (key == 'op'):
                    objii[key] = value.__name__ if callable(value) else value
                elif (is_serializable(value) is True):
                    objii[key] = recurse(value) \
                        if (isinstance(value, dict)) \
                        else value
            return objii

        if flat:
            return ZDict(recurse(self.__dict__))
        else:
            resd = ZDict()
            for (key, value) in ZDict(self.__dict__).getitems():
                if key in ('objp4', 'op', 'left', 'right', 'fieldname', 'tablename'):
                    resd.merge({key: value})
            return resd


class DLGExpression(object):
    #__hash__ = lambda self: hash((frozenset(self), frozenset(self.objp4)))
    __hash__ = object.__hash__
    __iter__ = lambda self: self.__dict__.__iter__()

    def __or__(self, value):
        return Lst(self, value)

    def __init__(
            self,
            objp4,
            op,
            left=None,
            right=None,
            inversion=False,
            type=None,
            *options,
            **kwargs
    ):
        (options, kwargs) = (Lst(options), ZDict(kwargs))
        self.options = options
        self.objp4 = objp4
        self.type = left.type \
            if (
                (type is None) &
                (left is not None) &
                (hasattr(left, 'type'))
        ) \
            else type

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

        (
            self.op,
            self.left,
            self.right
        ) = \
            (
                op,
                left,
                right
            )

        self.oDate = DLGDateTime()

        opname = op.__name__ \
            if (callable(op) is True) \
            else op

        self.inversion = True \
            if (opname in notops) \
            else inversion

        [setattr(self, sqlitem, getattr(left, sqlitem)) for sqlitem in
         ('fieldname', 'tablename') if (hasattr(self.left, sqlitem))]

        ''' huh??? 
        '''
        if (is_tableType(self) is False):
            if (
                    (self.fieldname is None) &
                    (self.tablename is None)
            ):
                if (left is not None):
                    sumtypes = sum(
                                    (
                                        is_queryType(left),
                                        is_expressionType(left),
                                        is_dictType(left)
                                    )
                               )
                    if (
                            (sumtypes > 1) |
                            is_field_tableType(left)
                    ):
                        [setattr(self, sqlitem, getattr(left, sqlitem)) for sqlitem in
                         ('fieldname', 'tablename') if (hasattr(self.left, sqlitem))]

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

    __str__ = __repr__

    def as_dict(self, flat=False):
        def recurse(obji):
            objii = dict()
            for k, v in obji.items():
                if (k in ("left", "right")):
                    if (isinstance(v, self.__class__)):
                        objii[k] = recurse(v.__dict__)
                    elif (is_fieldType(v) is True):
                        objii[k] = {
                            "tablename": v.tablename,
                            "fieldname": v.name
                        }
                    elif (is_expressionType(v)):
                        objii[k] = recurse(v.__dict__)
                    elif (isinstance(v, serializable)):
                        objii[k] = v
                    elif (isinstance(v, (date, time, datetime))):
                        objii[k] = self.oDate.to_string(v)
                elif (k == "op"):
                    if (callable(v)):
                        objii[k] = v.__name__
                    elif (isinstance(v, basestring)):
                        objii[k] = v
                    else:
                        pass
                elif (isinstance(v, serializable)):
                    objii[k] = recurse(v) \
                        if (isinstance(v, dict)) \
                        else v
            return objii

        if flat:
            return ZDict(recurse(self.__dict__))
        else:
            resd = ZDict()
            for (key, value) in ZDict(self.__dict__).getitems():
                if key in (
                        'objp4',
                        'op',
                        'left',
                        'right',
                        'fieldname',
                        'tablename'
                ):
                    resd.merge({key: value})
            return resd

    ''' queries:
            ~ (invert) 
            eq / = 
            ne / != 
            lt / < 
            gt / >
            <= / le
            >= . ge 
            belongs (though it feels more like an expression - revisit this!
            contains, 
            startswith, 
            endswith
            like
            ilike
    '''
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

                >>> qry1 = AND((oJnl.domain.type == '99'), (oJnl.domain.owner == 'zerdlg'))
                >>> myclients = oJnl(qry1)._select(oJnl.domain.name)
                >>> qry2 = (oJnl.domain.name.belongs(myclients))
                >>> clientrecords = oJnl(qry2).select()
        '''
        return DLGQuery(self.objp4, BELONGS, self, value)

    def contains(self, value):
        ''' USAGE:

                >>> qry = (oP4.files.depotFile.contains('/myProjectName'))
                >>> records = oP4(qry).select()
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

    def like(self, value, case_sensitive=True):
        op = case_sensitive and LIKE or ILIKE
        return DLGQuery(self.objp4, op, self, value)

    def ilike(self, value):
        return self.like(value, case_sensitive=False)

    ''' expressions:
            add
            sub
            mul
            div / truediv
            mod
            count
            sum
            max
            min
            len
            abg
            abs
    
            regex
            match
            search
            count
            lower
            upper
            replace
            coalesce
            coalesce_zero
            on
            
            year
            month
            day
            hour
            minute
            second
            epoch
    '''
    def __add__(self, value):
        return DLGExpression(self.objp4, ADD, self, value, type=self.type)

    def __sub__(self, value):
        result_type = 'integer' \
            if (self.type == 'integer') \
            else 'float' \
            if (self.type in ('date', 'time', 'datetime', 'float')) \
            else  self.type \
            if (self.type.startswith('decimal(')) \
            else None

        if (result_type is None):
            bail("subtraction operation not supported for type")
        return DLGExpression(self.objp4, SUB, self, value, type=result_type)

    def __mul__(self, value):
        return DLGExpression(self.objp4, MUL, self, value, type=self.type)

    def __div__(self, value):
        return self.__truediv__(value)

    def __truediv__(self, value):
        return DLGExpression(self.objp4, TRUEDIV, self, value, type=self.type)

    def __mod__(self, value):
        return DLGExpression(self.objp4, MOD, self, value, type=self.type)

    def count(self, distinct=None):
        ''' USAGE:

                >>> counter = jnl.rev.count()
                >>> print db(person).select(counter).first()(counter)
                1

                >>> qry = (p4.clients.client.count(distinct=distinct))
                >>> records = oP4(qry).select(distinct=distinct)

                or try:
                >>> count = p4.changes.status.count()
        '''
        return DLGExpression(self.objp4, COUNT, self, distinct=distinct, type='integer')

    def sum(self):
        return DLGExpression(self.objp4, SUM, self, None, type=self.type)

    def max(self):
        return DLGExpression(self.objp4, MAX, self, type=self.type)

    def min(self):
        return DLGExpression(self.objp4, MIN, self, type=self.type)

    def len(self):
        return DLGExpression(self.objp4, LEN, self, type='integer')

    def avg(self):
        return DLGExpression(self.objp4, AVG, self, type=self.type)

    def abs(self):
        return DLGExpression(self.objp4, ABS, self, type=self.type)

    def regexp(self, value):
        return DLGExpression(self.objp4, REGEX, self, value)

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

    def lower(self):
        return DLGExpression(self.objp4, LOWER, self, None, type=self.type)

    def upper(self):
        ''' >>> qry = p4.changes.user.upper().startswith('Z')
            >>> qry
            {'op': STARTSWITH,
            'left': {'inversion': False,
                     'left': p4.user,
                     'op': UPPER,
                     'right': None},
             'right': 'Z',
             'inversion': False}

            >>> recs = p4(qry).select()
        '''
        return DLGExpression(self.objp4, UPPER, self, None, type=self.type)

    def replace(self, left, right):
        return DLGExpression(self.objp4, REPLACE, self, (left, right), type=self.type)

    def year(self):
        return DLGExpression(self.objp4, YEAR, self, 'year', type='integer')

    def month(self):
        return DLGExpression(self.objp4, MONTH, self, 'month', type='integer')

    def day(self):
        return DLGExpression(self.objp4, DAY, self, 'day', type='integer')

    def hour(self):
        return DLGExpression(self.objp4, HOUR, self, 'hour', type='integer')

    def minute(self):
        return DLGExpression(self.objp4, MINUTE, self, 'minute', type='integer')

    def second(self):
        return DLGExpression(self.objp4, SECOND, self, 'second', type='integer')

    def epoch(self):
        return DLGExpression(self.objp4, EPOCH, self, None, type='integer')

    def coalesce(self, *args):
        return DLGExpression(self.objp4, COALESCE, self, args, type=self.type)

    def coalesce_zero(self):
        return DLGExpression(self.objp4, COALESCE_ZERO, self, None, type=self.type)

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

''' TODO:
        compounds like these could be interesting... 
        think about it or get rid of it.
'''
def EQAND(*args, **kwargs):
    ''' a = a & b'''


def EQOR(*args, **kwargs):
    ''' a = a | b'''


def EQXOR(*args, **kwargs):
    ''' a = a ^ b'''

def AND(*args, **kwargs):
    return clsAND()(*args, **kwargs)


def OR(*args, **kwargs):
    return clsOR()(*args, **kwargs)

def XOR(*args, **kwargs):
    return clsXOR()(*args, **kwargs)


def NOT(*args, **kwargs):
    return clsNOT()(*args, **kwargs)


def INVERT(*args, **kwargs):
    return clsINVERT()(*args, **kwargs)

def EQ(*args, **kwargs):
    return clsEQ()(*args, **kwargs)


def NE(*args, **kwargs):
    return clsNE()(*args, **kwargs)

def LT(*args, **kwargs):
    return clsLT()(*args, **kwargs)

def LE(*args, **kwargs):
    return clsLE()(*args, **kwargs)


def GT(*args, **kwargs):
    return clsGT()(*args, **kwargs)


def GE(*args, **kwargs):
    return clsGE()(*args, **kwargs)


def CONTAINS(*args, **kwargs):
    return clsCONTAINS()(*args, **kwargs)


def STARTSWITH(*args, **kwargs):
    return clsSTARTSWITH()(*args, **kwargs)


def ENDSWITH(*args, **kwargs):
    return clsENDSWITH()(*args, **kwargs)


def MATCH(*args, **kwargs):
    return clsMATCH()(*args, **kwargs)


def SEARCH(*args, **kwargs):
    return clsSEARCH()(*args, **kwargs)


def DIFF(*args, **kwargs):
    return bool(clsDIFF()(*args, **kwargs))


def CASE(*args, **kwargs):
    return clsCASE()(*args, **kwargs)


def CASEELSE(*args, **kwargs):
    return clsCASEELSE()(*args, **kwargs)


def LEN(*args, **kwargs):
    return clsLEN()(*args, **kwargs)


def MIN(*args, **kwargs):
    return clsMIN()(*args, **kwargs)


def MAX(*args, **kwargs):
    return clsMAX()(*args, **kwargs)


def ABS(*args, **kwargs):
    return clsABS(**kwargs)(*args)


def AVG(*args, **kwargs):
    return clsAVG()(*args, **kwargs)


def LOWER(*args, **kwargs):
    return clsLOWER()(*args, **kwargs)


def UPPER(*args, **kwargs):
    return clsUPPER()(*args, **kwargs)


def LIKE(*args, **kwargs):
    return clsLIKE()(*args, **kwargs)


def ILIKE(*args, **kwargs):
    return clsILIKE()(*args, **kwargs)


def ADD(*args, **kwargs):
    return clsADD()(*args, **kwargs)


def SUB(*args, **kwargs):
    return clsSUB()(*args, **kwargs)


def MUL(*args, **kwargs):
    return clsMUL()(*args, **kwargs)


def TRUEDIV(*args, **kwargs):
    return clsTRUEDIV()(*args, **kwargs)


def DIV(*args, **kwargs):
    return clsTRUEDIV()(*args, **kwargs)


def MOD(*args, **kwargs):
    return clsMOD()(*args, **kwargs)


def BELONGS(*args, **kwargs):
    return clsBELONGS()(*args, **kwargs)


def IN(*args, **kwargs):
    return clsIN()(*args, **kwargs)


def ON(*args, **kwargs):
    return clsON()(*args, **kwargs)


def SUM(*args, **kwargs):
    return clsSUM()(*args, **kwargs)


def COUNT(*args, **kwargs):
    return clsCOUNT()(*args,**kwargs)


def COALESCE(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsCOALESCE()(*args, **kwargs)


def COALESCE_ZERO(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsCOALESCE_ZERO()(*args, **kwargs)


def JOIN(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsJOIN()(*args, **kwargs)


def JOIN_LEFT(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsJOIN_LEFT()(*args, **kwargs)

def ALLOW_NONE(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsALLOW_NONE()(*args, **kwargs)

def REPLACE(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    # return clsREPLACE()(*args, **kwargs)

def YEAR(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsYEAR()(*args, **kwargs)


def MONTH(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsMONTH()(*args, **kwargs)


def DAY(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsDAY(**kwargs)(*args)


def HOUR(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsHOUR()(*args, **kwargs)


def MINUTE(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsMINUTE()(*args, **kwargs)


def SECOND(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsSECOND()(*args, **kwargs)


def EPOCH(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsEPOCH()(*args, **kwargs)


def PRIMARYKEY(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsPRIMARYKEY()(*args, **kwargs)


def EXTRACT(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsEXTRACT()(*args, **kwargs)


def SUBSTRING(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsSUBSTRING()(*args, **kwargs)


def BETWEEN(*args, **kwargs):
    #return clsBETWEEN()(*args, **kwargs)
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
                    return ((bitem & 1) | 0)
                alltrue = [
                    true_or_false(i) for i in exps
                ]
                sum_alltrue = sum(alltrue)
                qres = (len(exps) == sum_alltrue is True)
                return qres

    def build_query(self, left, right, OP, inversion=False):
        qry = ZDict()
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

        return ZDict({'op': OP, 'left': left, 'right': right, 'inversion': inversion})

    def strQryItems(self, lrq):
        (tablename, fieldname, value, op) = (None, None, None, None)
        if (isanyfile(lrq) is False):
            try:
                (tablename, fieldname, value, op) = getTableOpKeyValue(lrq)
            except: pass
        return (tablename, fieldname, value, op)

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
                    if (hasattr(avalue, 'op')):
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
                            if (hasattr(avalue.objp4, tablename)):
                                oTable = getattr(avalue.objp4, tablename)
                                if (hasattr(oTable, fieldname)):
                                    avalue = getattr(oTable, fieldname)
                                else:
                                    bail(f"field `{fieldname}` does not belong to table `{tablename}`.")
                if (akey == "left"):
                    left = avalue
                else:
                    right = avalue

            opname = op.__name__ \
                if (callable(op) is True) \
                else op

            if (all_ops_table(opname) is not None):
                built = {'op': op, 'left': left, 'right': right}

            # expression is a string
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
                if (type(qry).__name__ in ('ZDict', 'DLGQuery')):
                    tablename = qry.left.tablename
                    fieldname = qry.left.fieldname
                    value = qry.right
                    op = qry.op
                elif (isinstance(qry, str)):
                    try:
                        (tablename, fieldname, value, op) = getTableOpKeyValue(qry)
                    except:
                        (tablename, fieldname, value, op) = (None, None, None, None)
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

        if (is_recordsType(right) is True):
            if (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname
                rec0 = right(0)
                fieldvalue0 = rec0[fieldname]
                if ((isnum(fieldvalue0)) &
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
                 'Owner': 'zerdlg',
                 'Options': 'noallwrite noclobber nocompress unlocked nomodtime normdir',
                 'SubmitOptions': 'submitunchanged',
                 'LineEnd': 'local',
                 'Root': '/home/pi',
                 'Host': '',
                 'Description': 'Created by zerdlg.\n'}

            >>> Query1 = (oP4.client.client.startswith('ana'))
            >>> Query2 = (oP4.clients.owner == 'zerdlg')
            >>> qry = (Query1 & Query2)
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
                                         'right': 'zerdlg'}>}>
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
                if (type(left).__name__ == 'ZDict'):
                    qres = ZDict({'op': AND, 'left': left, 'right': right})
                elif (type(left).__name__ == 'DLGQuery'):
                    qres = DLGQuery(left.objp4, AND, left, right)
                elif (type(left).__name__ == 'DLGExpression'):
                    qres = DLGExpression(left.objp4, AND, left, right)
                elif (isinstance(left, str) is True):
                    (tablename, fieldname, value, op) = self.strQryItems(left)
                    qres = ZDict(
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
                     'Owner': 'zerdlg',
                     'Options': 'noallwrite noclobber nocompress unlocked nomodtime normdir',
                     'SubmitOptions': 'submitunchanged',
                     'LineEnd': 'local',
                     'Root': '/home/pi',
                     'Host': '',
                     'Description': 'Created by zerdlg.\n'}

                >>> Query1 = (oP4.client.client.startswith('ana'))
                >>> Query2 = (oP4.clients.owner == 'zerdlg')
                >>> qry = OR(Query1, Query2)
                >>> client_records = oP4(qry).select()

            Nested:
                >>> Query1 = (oP4.client.client.startswith('x'))
                >>> Query2 = (oP4.clients.owner == 'zerdlg')
                >>> Query3 = (oP4.clients.Description.contains('zerdlg'))
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
                                                               'right': 'zerdlg'}>}>,
                          'objp4': <Py4 anastasia.local:1777 >,
                          'op': <function OR at 0x147ad8fe0>,
                          'right': <DLGQuery {'left': <Py4Field Description>,
                                             'objp4': <Py4 anastasia.local:1777 >,
                                             'op': <function CONTAINS at 0x103d0bce0>,
                                             'right': 'zerdlg'}>}>
        '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': OR, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                qres = ZDict(
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': XOR, 'left': left, 'right': right})
            elif (type(left).__name__ in ('DLGQuery', 'DLGExpression')):
                qres = DLGQuery(left.objp4, XOR, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left ^ right),
                              self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                qres = ZDict(
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
    q = NOT((oP4.clients.Owner=='zerdlg'))

    <DLGExpression {'left': <DLGQuery {'left': <Py4Field Owner>,
                                      'objp4': <Py4 anastasia.local:1777 >,
                                      'op': <function EQ at 0x1075ff6a0>,
                                      'right': 'zerdlg'}>,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': INVERT, 'left': left, 'right': right})
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'DLGQuery')):
                qres = DLGExpression(left.objp4, INVERT, left, right)

            elif ((isinstance(left, (bool, int)))):
                qres = not left
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                qres = ZDict(
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
    q = NOT((oP4.clients.Owner=='zerdlg'))

    <DLGExpression {'left': <DLGQuery {'left': <Py4Field Owner>,
                                      'objp4': <Py4 anastasia.local:1777 >,
                                      'op': <function EQ at 0x1075ff6a0>,
                                      'right': 'zerdlg'}>,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': NOT, 'left': left, 'right': right, 'inversion': True})
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'DLGQuery')):
                qres = DLGQuery(left.objp4, NOT, left, right, inversion=True)
            elif ((isinstance(left, (bool, int)))):
                qres = not left
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                qres = ZDict(
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': NE, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': EQ, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': LT, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                qres = ZDict({'op': LT,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': LE, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                qres = ZDict({'op': LE,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': GT, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                qres = ZDict({'op': GT,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': GE, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                qres = ZDict({'op': GE,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': CONTAINS, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.search(f"{right}", left) is not None),
                                  self.getSequence(*exps))
                else:
                    qres = ZDict({'op': CONTAINS,
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
            if (
                    (type(left).__name__ == 'ZDict') &
                    (is_array(right) is True)
            ):
                qres = ZDict({'op': BELONGS, 'left': left, 'right': right})
            elif (
                        (type(left).__name__ == 'DLGQuery') &
                        (is_array(right) is True)
            ):
                qres = DLGQuery(left.objp4, BELONGS, left, right)
            elif (
                        (type(left).__name__ in ('Py4Field', 'JNLField', 'DLGExpression')) &
                        (is_array(right) is True)
            ):
                qres = DLGExpression(left.objp4, BELONGS, left, right)
            elif (
                    (isinstance(left, (bool, int)))
                    & (isinstance(right, (bool, int)))
            ):
                qres = reduce(lambda left, right: (left in right),
                              self.getSequence(*exps))
            elif (isinstance(left, (str, int)) is True):
                left = str(left)
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left in right),
                                self.getSequence(*exps))
                else:
                    qres = ZDict({'op': BELONGS,
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
            elif (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': ON, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.match(f"^{right}.*$", left) is not None),
                                  self.getSequence(*exps))
                else:
                    qres = ZDict({'op': STARTSWITH,
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


clsIN = type('clsIN', clsBELONGS.__bases__, dict(clsBELONGS.__dict__))


class clsSTARTSWITH(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': STARTSWITH, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    condition = (re.match(f"^{right}.*$", left) is not None)
                    qres = reduce(lambda left, right: condition,
                                  self.getSequence(*exps))
                else:
                    qres = ZDict({'op': STARTSWITH,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': ENDSWITH, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.match(f"^.*{right}$", left) is not None),
                                  self.getSequence(*exps))
                else:
                    qres = ZDict({'op': ENDSWITH,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': MATCH, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.match(f"{right}", left) is not None),
                                  self.getSequence(*exps))
                else:
                    qres = ZDict({'op': MATCH,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': SEARCH, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (re.search(f"{right}", left) is not None),
                              self.getSequence(*exps))
                else:
                    qres = ZDict({'op': SEARCH,
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

'''
    COUNT, ADD, MOD, SUM, AVG, LEN, ABS, MIN, MAX, SUB, MUL, TRUEDIV, DIV
'''
class clsCOUNT(QClass):
    def __call__(self, *exps, distinct=None):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        (fieldname, grprecs, groups) = (None, None, None)
        if (distinct is not None):
            fieldname = distinct.fieldname \
                if (is_fieldType(distinct) is True) \
                else distinct.left.fieldname \
                if (is_query_or_expressionType(distinct) is True) \
                else distinct
        elif ((is_fieldType_or_expressionType(left) is True) |
                    (is_tableType(left) is True)):
            fieldname = left.fieldname
        if (is_recordsType(right) is True):
            grprecs = right.groupby(fieldname, as_groups=True)
            groups = grprecs.getkeys()
        try:
            #qcount = 0
            if (isinstance(distinct, bool) is True):
                if (distinct is True):
                    if (is_array(right) is True):
                        right = len(set(right))
                    elif (is_recordsType(right) is True):
                        distinctrecords = ZDict()
                        #grprecs  = right.groupby(fieldname, as_groups=True)
                        #groups = grprecs.getkeys()
                        for grp in groups:
                            for record in grprecs[grp]:
                            #for record in right:
                                #qcount += 1
                                if (not record[fieldname] in distinctrecords):
                                    #record.count = qcount
                                    distinctrecords.record[fieldname] = record
                        right = distinctrecords.getvalues()
                        setattr(right, 'count', len(right))
            elif (distinct is None):
                for grp in groups:
                    group = grprecs[grp]
                    grplen = len(group)#grprecs[grp].count()
                    for record in group:
                        record.count = grplen
                #for record in right:
                #    qcount += 1
                #    record.count = qcount
                #setattr(right, 'count', len(right))
                right.count = len(right)
            return right
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MAX
            ), name='COUNT', err=err)

class clsMOD(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': MOD, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left % right), self.getSequence(*exps))
                else:
                    qres = ZDict({'op': MOD,
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
        fieldname = left.fieldname if (
                (is_fieldType_or_expressionType(left) is True) |
                (is_tableType(left) is True)
        ) else left
        qsum = 0
        try:
            if (
                     (is_recordsType(right) is True) |
                    (type(right) is enumerate)
            ):
                for record in right:
                    qsum += int(record[fieldname])
            elif (is_array(right) is True):
                for item in right:
                    qsum += int(item)
            elif (is_array(left) is True):
                for item in left:
                    qsum += int(item)
            if (right is not None):
                qsum = round(qsum) \
                    if ((qsum % 1) == 0) \
                    else round(qsum, 2)
                setattr(right, 'sum', qsum)
                return right
            return qsum
        except ValueError as err:
            op_error(
                lambda left, right, op: (
                    left,
                    right,
                    op or SUM
                ),
                name='SUM',
                err=err
            )

class clsAVG(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))

        length = len(left) \
            if (isinstance(left, tuple) is True) \
            else len(right) \
            if (type(right).__name__ == 'Records') \
            else 0
        asum = 0
        fieldname = left.fieldname \
            if (
                (is_fieldType_or_expressionType(left) is True) |
                (is_tableType(left) is True)
        ) \
            else left

        try:
            if (
                    (is_recordsType(right) is True) |
                    (type(right) is enumerate)
            ):
                length = len(right)
                asum = sum([float(record[fieldname]) for record in right])
            elif (is_array(right) is True):
                length = len(right)
                asum = sum([float(item) for item in right])
            average = (asum / length)
            average = round(average) \
                    if ((average % 1) == 0) \
                    else round(average, 2)
            setattr(right, 'avg', average)
            return right
        except Exception as err:
            op_error(
                lambda left, right, op: (
                    left,
                    right,
                    op or AVG
                ),
                     name='AVG',
                     err=err
            )

class clsLEN(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        if (
                (right is None) &
                (isinstance(left, (str, bool)))
        ):
            return len(left)
        fieldname = left.fieldname if (
                (is_fieldType_or_expressionType(left) is True) |
                (is_tableType(left) is True)
        ) else left
        try:
            if (
                    (is_recordsType(right) is True) |
                    (isinstance(right, list) is True)
            ):
                for record in right:
                    record.merge({'len': len(record[fieldname])})
                return right
            elif (
                    (is_recordType(right) is True) |
                    (isinstance(right, dict) is True)
            ):
                return len(right[fieldname])
            elif (right is None):
                try:
                    return len(left)
                except:
                    return left
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or LEN
            ),name='LEN', err=err)

class clsABS(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        (current_type, left_right_are_valid) \
            = self.validate_field_type(left, right)
        fieldname = left.fieldname if (
                (is_fieldType_or_expressionType(left) is True) |
                (is_tableType(left) is True)
        ) else left
        try:
            if (
                    (current_type is None) &
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
            elif (
                    (current_type is not None) &
                    (left_right_are_valid is True)):
                updated_records = Lst()
                for record in right:
                    current_value = record[fieldname]
                    updvalue = abs(float(current_value))
                    updvalue = current_type(
                        round(updvalue) \
                            if ((updvalue % 1) == 0) \
                            else round(updvalue, 2)
                    )
                    record.merge({fieldname: updvalue})
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
        fieldname = left.fieldname if (
                (is_fieldType_or_expressionType(left) is True) |
                (is_tableType(left) is True)
        ) else left
        try:
            if (
                    (current_type is None) &
                    (is_array(left) is True)
            ):
                current_type = type(left[0])
                for value in left:
                    values.add(float(value))
                minvalue = min(values)
            elif (
                    (current_type is not None) &
                    (left_right_are_valid is True)
            ):
                for record in right:
                    value = record[fieldname]
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
                op or MIN
            ), name='MIN', err=err)


class clsMAX(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        #(current_type, left_right_are_valid) \
        #    = self.validate_field_type(left, right)
        (values, maxvalue) = (set(), None)
        fieldname = left.fieldname if (
                (is_fieldType_or_expressionType(left) is True) |
                (is_tableType(left) is True)
        ) else left
        try:
            if (left is not None):
                if (is_array(right) is True):
                    for value in right:
                        values.add(float(value))
                    maxvalue = max(values)
                elif (is_recordsType(right) is True):
                    for record in right:
                        value = record[fieldname]
                        values.add(float(value))
                    maxvalue = max(values)
            else:
                if (is_array(left) is True):
                    maxvalue = max(left)
            maxvalue = (round(maxvalue) \
                if ((maxvalue % 1) == 0) \
                else round(maxvalue, 2))
            return maxvalue
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MAX
            ), name='MAX', err=err)

class clsADD(QClass):
    """
    GT(ADD(p4.files.depotFile.len(), 1), 64)
    or
    ((p4.files.depotFile.len() + 1) > 64)

    {'inversion': False,
     'left': {'inversion': False,
              'left': {'inversion': False,
                       'left': '<Py4Field depotFile>',
                       'objp4': '<Py4 anastasia.local:1777 >',
                       'op': LEN,
                       'right': None},
              'objp4': '<Py4 anastasia.local:1777 >',
              'op': ADD,
              'right': 1},
     'objp4': '<Py4 anastasia.local:1777 >',
     'op': GT,
     'right': 64}
    """
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (
                    (isinstance(left, list) is True) &
                    (right is None)
            ):
                bail(f'First argument must be a number or an expression, got `{type(left)}`.')
            if (isinstance(left, dict) is True):
                return ZDict({'op': ADD, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, ADD, left, right)
            elif (is_fieldType_or_expressionType(left) is True):
                if (hasattr(left, 'left')):
                    if (is_fieldType(left.left) is True):
                        return DLGExpression(left.objp4, ADD, left, right)
                fieldname = left.fieldname if (hasattr(left, 'fieldname')) else left
                leftvalue = float(left.right)
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(record, dict) is True)
                        ):
                            total = int(leftvalue + record[fieldname])
                            record.update(**{'add': total})
                    return right
                elif (
                        (
                                (is_recordType(right) is True),
                                (isinstance(right, dict) is True),
                                (is_recordType(left))
                        ) & (isnum(left) is True)
                ):
                    return int(leftvalue + right[fieldname])
            elif (
                    (
                            (isnum(left) is True) &
                            (isnum(right) is True)
                    ) |
                    (
                            (isinstance(left, bool) is True) &
                            (isinstance(right, bool) is True)
                    )
            ):
                return reduce(lambda left, right: (left + right), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (
                        (tablename, fieldname) == (None, None)
                ):
                    return reduce(lambda left, right: (left + right), self.getSequence(*exps))
            return right
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or ADD
            ), name='ADD', err=err)

class clsSUB(QClass):
    """
    GT(SUB(p4.files.depotFile.len(), 1), 64)
    or
    ((p4.files.depotFile.len() - 1) > 64)

    {'inversion': False,
     'left': {'inversion': False,
              'left': {'inversion': False,
                       'left': '<Py4Field depotFile>',
                       'objp4': '<Py4 anastasia.local:1777 >',
                       'op': LEN,
                       'right': None},
              'objp4': '<Py4 anastasia.local:1777 >',
              'op': SUB,
              'right': 1},
     'objp4': '<Py4 anastasia.local:1777 >',
     'op': GT,
     'right': 64}
    """
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (
                    (isinstance(left, list) is True) &
                    (right is None)
            ):
                bail(f'First argument must be a number or an expression, got `{type(left)}`.')
            if (isinstance(left, dict) is True):
                return ZDict({'op': SUB, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, SUB, left, right)
            elif (is_fieldType_or_expressionType(left) is True):
                if (hasattr(left, 'left')):
                    if (is_fieldType(left.left) is True):
                        return DLGExpression(left.objp4, SUB, left, right)
                fieldname = left.fieldname if (hasattr(left, 'fieldname')) else left
                leftvalue = float(left.right)
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(record, dict) is True)
                        ):
                            total = int(leftvalue - record[fieldname])
                            record.update(**{'sub': total})
                    return right
                elif (
                        (
                                (is_recordType(right) is True),
                                (isinstance(right, dict) is True),
                                (is_recordType(left))
                        ) & (isnum(left) is True)
                ):
                    return int(leftvalue - right[fieldname])
            elif (
                    (
                            (isnum(left) is True) &
                            (isnum(right) is True)
                    ) |
                    (
                            (isinstance(left, bool) is True) &
                            (isinstance(right, bool) is True)
                    )
            ):
                return reduce(lambda left, right: (left - right), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (
                        (tablename, fieldname) == (None, None)
                ):
                    return reduce(lambda left, right: (left - right), self.getSequence(*exps))
            return right
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or SUB
            ), name='SUB', err=err)

class clsMUL(QClass):
    """
    GT(MUL(p4.files.depotFile.len(), 2), 128)
    or
    ((p4.files.depotFile.len() * 2) > 128)

    {'inversion': False,
     'left': {'inversion': False,
              'left': {'inversion': False,
                       'left': '<Py4Field depotFile>',
                       'objp4': '<Py4 anastasia.local:1777 >',
                       'op': LEN,
                       'right': None},
              'objp4': '<Py4 anastasia.local:1777 >',
              'op': MUL,
              'right': 2},
     'objp4': '<Py4 anastasia.local:1777 >',
     'op': GT,
     'right': 128}
    """
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (
                    (isinstance(left, list) is True) &
                    (right is None)
            ):
                bail(f'First argument must be a number or an expression, got `{type(left)}`.')
            if (isinstance(left, dict) is True):
                return ZDict({'op': MUL, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, MUL, left, right)
            elif (is_fieldType_or_expressionType(left) is True):
                if (hasattr(left, 'left')):
                    if (is_fieldType(left.left) is True):
                        return DLGExpression(left.objp4, MUL, left, right)
                fieldname = left.fieldname if (hasattr(left, 'fieldname')) else left
                leftvalue = float(left.right)
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(record, dict) is True)
                        ):
                            total = int(leftvalue * record[fieldname])
                            record.update(**{'sub': total})
                    return right
                elif (
                        (
                                (is_recordType(right) is True),
                                (isinstance(right, dict) is True),
                                (is_recordType(left))
                        ) & (isnum(left) is True)
                ):
                    return int(leftvalue * right[fieldname])
            elif (
                    (
                            (isnum(left) is True) &
                            (isnum(right) is True)
                    ) |
                    (
                            (isinstance(left, bool) is True) &
                            (isinstance(right, bool) is True)
                    )
            ):
                return reduce(lambda left, right: (left * right), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (
                        (tablename, fieldname) == (None, None)
                ):
                    return reduce(lambda left, right: (left * right), self.getSequence(*exps))
            return right
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or MUL
            ), name='MUL', err=err)

class clsTRUEDIV(QClass):
    """
    GT(TRUEDIV(p4.files.depotFile.len(), 2), 32)
    or
    ((p4.files.depotFile.len() / 2) > 32)

    {'inversion': False,
     'left': {'inversion': False,
              'left': {'inversion': False,
                       'left': '<Py4Field depotFile>',
                       'objp4': '<Py4 anastasia.local:1777 >',
                       'op': LEN,
                       'right': None},
              'objp4': '<Py4 anastasia.local:1777 >',
              'op': TRUEDIV,
              'right': 2},
     'objp4': '<Py4 anastasia.local:1777 >',
     'op': GT,
     'right': 21}
    """
    def __call__(self, *exps, force_int=True):
        ''' `force_int` applies to floating point results.

            simply,

            >>> res = 11.5
            >>> if (force_int is True):
            >>>     res = int(res)
            >>> res
            11

            otherwise, we need to deal with failing expressions, which may
            be more trouble than its worth. However, you can always set
            `force_int = False`.
        '''
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (
                    (isinstance(left, list) is True) &
                    (right is None)
            ):
                bail(f'First argument must be a number or an expression, got `{type(left)}`.')
            if (isinstance(left, dict) is True):
                return ZDict({'op': TRUEDIV, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, TRUEDIV, left, right)
            elif (is_fieldType_or_expressionType(left) is True):
                if (hasattr(left, 'left')):
                    if (is_fieldType(left.left) is True):
                        return DLGExpression(left.objp4, TRUEDIV, left, right)
                fieldname = left.fieldname if (hasattr(left, 'fieldname')) else left
                leftvalue = int(left.right)
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(record, dict) is True)
                        ):
                            total = int(leftvalue / record[fieldname]) \
                                if (force_int is True) \
                                else (leftvalue / record[fieldname])
                            record.update(**{'TRUEDIV': total})
                    return right
                elif (
                        (
                                (is_recordType(right) is True),
                                (isinstance(right, dict) is True),
                                (is_recordType(left))
                        ) & (isnum(left) is True)
                ):
                    return int(leftvalue / right[fieldname]) \
                        if (force_int is True) \
                        else (leftvalue / right[fieldname])
            elif (
                    (
                            (isnum(left) is True) &
                            (isnum(right) is True)
                    ) |
                    (
                            (isinstance(left, bool) is True) &
                            (isinstance(right, bool) is True)
                    )
            ):
                return reduce(lambda left, right: int(left / right)
                if (force_int is True)
                else (left / right), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (
                        (tablename, fieldname) == (None, None)
                ):
                    return reduce(lambda left, right: int(left / right)
                    if (force_int is True)
                    else (left / right), self.getSequence(*exps))
            return right
        except Exception as err:
            op_error(lambda left, right, op: (
                left,
                right,
                op or TRUEDIV
            ), name='TRUEDIV', err=err)

class clsCASE(QClass):
    ''' >>> condition = (p4.changes.user.startswith('z'))
        >>> true_false = condition.case('True','False')
        >>> records = p4(p4.changes).select(p4.changes.user, true_false):
        >>> for rec in records:
        >>>     print(rec.user,  rec(true_false))
        zerdlg True
        bigbird False
    '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': CASE, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left ^ right),
                                  self.getSequence(*exps))
                else:
                    qres = ZDict({'op': CASE,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': CASEELSE, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right, other: ((left ^ right) | other),
                                  self.getSequence(*exps))
                else:
                    qres = ZDict({'op': CASEELSE,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': LIKE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, LIKE, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, LIKE, left, right)
            elif (
                    (isinstance(left, (bool, int))) &
                    (isinstance(right, (bool, int)))
            ):
                ismatch = (re.search(left, right) is not None)
                qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    ismatch = (re.search(left, right) is not None)
                    qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
                else:
                    qres = ZDict({'op': LIKE,
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
        OPERATOR = None
        if (isinstance(right, str)):
            if (right.startswith('%') is True):
                (right, OPERATOR) = (f"^{right}$", CONTAINS) \
                    if (right.endswith('%') is True) \
                    else (f"{right}$", ENDSWITH)
            else:
                (right, OPERATOR) = (f"^{right}", STARTSWITH) \
                    if (right.endswith('%') is True) \
                    else (f"^{right}$", CONTAINS)
            right = re.sub('%', '', right)
        try:
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': LIKE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, LIKE, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, LIKE, left, right)
            elif (
                    (isinstance(left, (bool, int))) &
                    (isinstance(right, (bool, int)))
            ):
                ismatch = (re.search(right, left) is not None)
                qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    ismatch = (re.search(right, left) is not None)
                    qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
                else:
                    qres = ZDict({'op': LIKE,
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
        OPERATOR = None
        if (isinstance(right, str)):
            if (right.startswith('%') is True):
                (right, OPERATOR) = (f"^{right}$", CONTAINS) \
                    if (right.endswith('%') is True) \
                    else (f"{right}$", ENDSWITH)
            else:
                (right, OPERATOR) = (f"^{right}", STARTSWITH) \
                    if (right.endswith('%') is True) \
                    else (f"^{right}$", CONTAINS)
            (right, left) = (re.sub('%', '', right).lower(), left.lower())
        try:
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': LIKE, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, LIKE, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, LIKE, left, right)
            elif (
                    (isinstance(left, (bool, int))) &
                    (isinstance(right, (bool, int)))
            ):
                ismatch = (re.search(right, left) is not None)
                qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    ismatch = (re.search(right, left) is not None)
                    qres = reduce(lambda left, right: ismatch, self.getSequence(*exps))
                else:
                    qres = ZDict({'op': LIKE,
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
    ''' convert a field value to lower case -> it should also support like(), so implement it!
    >>> name = f'{name[0:3]}%'
    >>> recs = p4(p4.users.user.lower().like('zer%')).select():
    >>> print(recs(0))
    zerdlg
    '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        if (
                (right is None) &
                (isinstance(left, str))
        ):
            return left.lower()()
        if (isinstance(left, dict) is True):
            return ZDict({'op': LOWER, 'left': left, 'right': right})
        elif (is_queryType(left) is True):
            return DLGQuery(left.objp4, LOWER, left, right)
        elif (is_fieldType_or_expressionType(left) is True):
            fieldname = left.fieldname \
                if (is_fieldType_or_expressionType(left) is True) \
                else left
            try:
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    for record in right:
                        record.update(**{fieldname: record[fieldname].lower()})
                    return right
                elif (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    return right[fieldname].lower()
                elif (right is None):
                    try:
                        return left.lower()
                    except:
                        return left
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
    zerdlg


    '''
    def __call__(self, *exps, **kwargs):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        if (
                (right is None) &
                (isinstance(left, str))
        ):
            return left.upper()
        if (isinstance(left, dict) is True):
            return ZDict({'op': UPPER, 'left': left, 'right': right})
        elif (is_queryType(left) is True):
            return DLGQuery(left.objp4, UPPER, left, right)
        elif (is_fieldType_or_expressionType(left) is True):
            fieldname = left.fieldname \
                if (is_fieldType_or_expressionType(left) is True) \
                else left
            try:
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    for record in right:
                        record.update(**{fieldname: record[fieldname].upper()})
                    return right
                elif (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    return right[fieldname].upper()
                elif (right is None):
                    try:
                        return left.upper()
                    except:
                        return left
            except Exception as err:
                op_error(lambda left, right, op: (
                    left,
                    right,
                    op or UPPER
                ), name='UPPER', err=err)

class clsUPPER_(QClass):
    ''' convert a field value to upper case -> it should also support like(value%), so implement it!
        >>> name = f'{name[0:3]}%'
        >>> for rec in oP4(oP4.user.user.upper().like(name)).select():
        >>>     print(rec.user)
        zerdlg
        '''
    def __call__(self, *exps):
        exps = Lst(exps)
        (left, right) = (exps(0), exps(1))
        try:
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': UPPER, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, UPPER, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                qres = DLGExpression(left.objp4, UPPER, left, right)
            elif (isinstance(left, (bool, int))):
                qres = reduce(lambda left: left.upper(), self.getSequence(*exps))
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (tablename, fieldname) == (None, None):
                    qres = left.upper()
                else:
                    qres = ZDict({'op': UPPER,
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': EPOCH, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                qres = DLGQuery(left.objp4, EPOCH, left, right)
            elif (type(left).__name__ in ('DLGExpression', 'Py4Field', 'JNLField')):
                qres = DLGExpression(left.objp4, EPOCH, left, right)
            elif (isinstance(left, (int, float, datetime.datetime)) is True):
                qres = oDateTime.to_epoch(left)
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    left = oDateTime.to_p4date(left)
                    qres = oDateTime.to_epoch(left)
                else:
                    qres = ZDict({'op': EPOCH, 'left':
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
            if (type(left).__name__ == 'ZDict'):
                qres = ZDict({'op': DIFF, 'left': left, 'right': right})
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
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left - right),
                                  self.getSequence(*exps))
                else:
                    qres = ZDict({'op': DIFF,
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


class clsREPLACE(QClass):
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

class clsJOIN_LEFT(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)

class clsALLOW_NONE(QClass):
    def __call__(self, *exps):
        exps = Lst(exps)


''' TODO: BELOW IS SAD, please do better 
'''

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

    expression_ops = ZDict(
        {
            # Expression(self.objp4, op)
            "JOIN" : JOIN,
            "LEFT_JOIN": JOIN_LEFT,
            "ALLOW_NONE": ALLOW_NONE,

            # DLGExpression(self.objp4, op, left)
            "LOWER": LOWER,
            "UPPER": UPPER,
            "PRIMARYKEY": PRIMARYKEY,
            "COALESCE": COALESCE,
            "COALESCEZERO": COALESCE_ZERO,
            "INVERT": INVERT,
            'EPOCH': EPOCH,
            'LEN': LEN,

            # DLGExpression(self.objp4, op, left, right)
            "ADD": ADD,
            "SUB": SUB,
            "MUL": MUL,
            "MOD": MOD,
            "BETWEEN": BETWEEN,
            "CASE": CASE,
            "CASEELSE": CASEELSE,
            "DIFF": DIFF,
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
            #"CONTAINS": CONTAINS,
            #"STARTSWITH": STARTSWITH,
            #"ENDSWITH": ENDSWITH,
            #"SEARCH": SEARCH,
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
dops = ZDict({'equal': (EQ, "EQ", '='),
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
ops = ZDict({EQ: ("EQ", '='),
               NE: ("NE", '!='),
               GT: ("GT", '>'),
               GE: ("GE", '>='),
               LT: ("LT", '<'),
               LE: ("LE", '<='),
 \
               NOT: ('NOT', '~'),
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