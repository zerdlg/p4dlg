import re
from datetime import date, time, datetime
from functools import reduce
from types import FunctionType
from difflib import ndiff, unified_diff, context_diff, Differ

from libdlg.dlgStore import Lst, Storage, StorageIndex
from libdlg.dlgDateTime import DLGDateTime, DLGDateTimeConvert
from libdlg.dlgUtilities import (
    getTableOpKeyValue,
    bail,
    serializable,
    isnum,
    is_array,
    basestring,
    isanyfile,
    ALLUPPER,
    ALLLOWER
)
from libsql.sqlValidate import *
from libsql.sqlRecords import Records

'''  [$File: //dev/p4dlg/libsql/sqlQuery.py $] 
     [$Change: 728 $] 
     [$Revision: #44 $]
     [$DateTime: 2025/05/23 02:44:52 $]
     [$Author: zerdlg $]
'''

__all__ = [
            # classes, use directly
            'DLGQuery', 'DLGExpression',
            # used internally
            'QClass',
            # op tables - this is messed up, needs serious clean up!
            'comparison_ops', 'equal_ops', 'regex_ops', 'method_ops',
            'ops', 'andops', 'orops', 'xorops', 'notops', 'dops',
            'optable', 'expression_table', 'all_ops_table',

            'NOT', 'AND', 'OR', 'XOR',

            'EQ', 'NE', 'GE', 'GT', 'LE', 'LT',

            'CONTAINS', 'ENDSWITH', 'STARTSWITH',
            'JOIN', 'JOIN_LEFT', 'SUBSTR', 'LIKE', 'ILIKE',

            'SUM', 'ABS', 'LEN', 'TRUEDIV', 'AVG',
            'MIN', 'MAX', 'COUNT',
            'BELONGS',
            'COALESCE', 'COALESCE_ZERO',

            'ADD', 'SUB', 'MUL', 'MOD',
            'LOWER', 'UPPER',
            'ALLOW_NONE', 'DIFF', 'REPLACE', 'BETWEEN',
            'MATCH', 'SEARCH', 'REGEXP',

            'PRIMARYKEY', 'EXTRACT',

            'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND', 'EPOCH', 'DATETIME',
]

''' TODO: add these useful operators:

        arithmatic      --> !>, !<
        coumpoud        --> +=. -=, *=, /=, %=, &=, ^=, |=
        logical         --> ALL, ANY, EXISTS, SOME
        set             --> INTERSECT, UNION, DIFF, SYMMETRIC
'''

objectify = Storage.objectify

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
        opname = op.__name__ \
            if (callable(op) is True) \
            else op
        if (opname in notops):
            self.inversion = True

    def __str__(self):
        return f'<DLGQuery {self.as_dict()}>'

    def copy(self):
        return DLGQuery(self.objp4, self.op, self.left, self.right, self.inversion)

    def __and__(self, value):
        return DLGQuery(self.objp4, AND, self, value)

    def __or__(self, value):
        return DLGQuery(self.objp4, OR, self, value)

    def __xor__(self, value):
        return DLGQuery(self.objp4, XOR, self, value)

    __hash__ = object.__hash__
    __iter__ = lambda self: self.__dict__.__iter__()
    __rand__ = __and__
    __ror__ = __or__

    def __invert__(self):
        self.inversion = True
        return DLGQuery(self.objp4, NOT, self, inversion=True)

    def __eq__(self, value):
        return DLGQuery(self.objp4, EQ, self, value)

    def __ne__(self, value):
        return DLGQuery(self.objp4, NE, self, value)

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
            return Storage(recurse(self.__dict__))
        else:
            resd = Storage()
            for (key, value) in Storage(self.__dict__).getitems():
                if key in ('objp4', 'op', 'left', 'right', 'fieldname', 'tablename'):
                    resd.merge({key: value})
            return resd


class DLGExpression(object):
    __hash__ = object.__hash__
    __iter__ = lambda self: self.__dict__.__iter__()

    def __str__(self):
        return f'<DLGExpression {self.as_dict()}>'

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
        (options, kwargs) = (Lst(options), Storage(kwargs))
        self.__dict__ = objectify(self.__dict__)
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
        self.opname = opname

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
                            is_fieldType_or_tableType(left)
                    ):
                        [setattr(self, sqlitem, getattr(left, sqlitem)) for sqlitem in
                         ('fieldname', 'tablename') if (hasattr(self.left, sqlitem))]

    def __getitem__(self, value):
        if (isinstance(value, slice) is False):
            return self[value: (value + 1)]
        else:
            ''' resolve and rebuild the slice in case of None or 
                negative values, then return a valid expression.
            '''
            (
                start,
                stop,
            ) = \
                (
                    value.start or 0,
                    value.stop# or self.len(),
                )
            if (start < 0):
                start = SUB(
                    self.len(),
                    SUB(
                        abs(value.start),
                        1
                    )
                )
            if (
                    (is_expressionType(stop) is False) &
                    (isinstance(stop, int) is True)
            ):
                if (stop < 0):
                    stop = SUB(
                        self.len(),
                        SUB(
                            abs(value.stop),
                            1
                        ),
                        start
                    )
            return DLGExpression(
                self.objp4,
                SUBSTR,
                self,
                slice(start, stop),
                self.type
            )

    def as_dict(self, flat=False):
        def recurse(obji):
            objii = dict()
            for (k, v) in obji.items():
                if (k in ("left", "right")):
                    if (isinstance(v, self.__class__) is True):
                        objii[k] = recurse(v.__dict__)
                    elif (is_fieldType(v) is True):
                        objii[k] = {
                            "tablename": v.tablename,
                            "fieldname": v.name
                        }
                    elif (is_expressionType(v) is True):
                        objii[k] = recurse(v.__dict__)
                    elif (isinstance(v, serializable) is True):
                        objii[k] = v
                    elif (isinstance(v, (date, time, datetime)) is True):
                        objii[k] = self.oDate.to_string(v)
                elif (k == "op"):
                    if (callable(v) is True):
                        objii[k] = v.__name__
                    elif (isinstance(v, basestring) is True):
                        objii[k] = v
                    else: pass
                elif (isinstance(v, serializable) is True):
                    objii[k] = recurse(v) \
                        if (isinstance(v, dict) is True) \
                        else v
            return objii

        if flat:
            return Storage(recurse(self.__dict__))
        else:
            resd = Storage()
            for (key, value) in Storage(self.__dict__).getitems():
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

    def __invert__(self):
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
        ''' Can't use `IN`, so...

            USAGE:

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

    def contains(self, value, case_sensitive=False):
        ''' USAGE:

                >>> qry = (oP4.files.depotFile.contains('/myProjectName'))
                >>> records = oP4(qry).select()
        '''
        if isinstance(value, (list, tuple)):
            subqueries = [
                self.contains(str(v), case_sensitive=case_sensitive)
                for v in value
                if str(v)
            ]
            if (len(subqueries) == 0):
                return self.contains('')
            else:
                return reduce(all and AND or OR, subqueries)
        return DLGQuery(self.objp4, CONTAINS, self, value, case_sensitive=case_sensitive)

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
        ''' USAGE:
                >>> qry = (oP4.files.depotFile.endswith('.py'))
                >>> records = oP4(qry).select()
        '''
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
        valuetype = 'integer' \
            if ((self.type == 'integer') | (isnum(value) is True)) \
            else 'float' \
            if (self.type in ('date', 'time', 'datetime', 'float')) \
            else self.type \
            if (self.type.startswith('decimal(')) \
            else None
        if ((self.type != valuetype) & (value is not None)):
            self.type = valuetype
        if (valuetype is None):
            bail(f"addition operation not supported for type {valuetype}")
        return DLGExpression(self.objp4, ADD, self, value, type=self.type)

    def __sub__(self, value):
        valuetype = 'integer' \
            if ((self.type == 'integer') | (isnum(value) is True)) \
            else 'float' \
            if (self.type in ('date', 'time', 'datetime', 'float')) \
            else self.type \
            if (self.type.startswith('decimal(')) \
            else None
        if ((self.type != valuetype) & (value is not None)):
            self.type = valuetype
        if (valuetype is None):
            bail(f"subtraction operation not supported for type {valuetype}")
        return DLGExpression(self.objp4, SUB, self, value, type=valuetype)

    def __mul__(self, value):
        valuetype = 'integer' \
            if ((self.type == 'integer') | (isnum(value) is True)) \
            else 'float' \
            if (self.type in ('date', 'time', 'datetime', 'float')) \
            else self.type \
            if (self.type.startswith('decimal(')) \
            else None
        if ((self.type != valuetype) & (value is not None)):
            self.type = valuetype
        if (valuetype is None):
            bail(f"multiplication operation not supported for type {valuetype}")
        return DLGExpression(self.objp4, MUL, self, value, type=self.type)

    def __truediv__(self, value):
        valuetype = 'integer' \
            if ((self.type == 'integer') | (isnum(value) is True)) \
            else 'float' \
            if (self.type in ('date', 'time', 'datetime', 'float')) \
            else self.type \
            if (self.type.startswith('decimal(')) \
            else None
        if ((self.type != valuetype) & (value is not None)):
            self.type = valuetype
        if (valuetype is None):
            bail(f"division operation not supported for type {valuetype}")
        return DLGExpression(self.objp4, TRUEDIV, self, value, type=self.type)

    def __mod__(self, value):
        valuetype = 'integer' \
            if ((self.type == 'integer') | (isnum(value) is True)) \
            else 'float' \
            if (self.type in ('date', 'time', 'datetime', 'float')) \
            else self.type \
            if (self.type.startswith('decimal(')) \
            else None
        if ((self.type != valuetype) & (value is not None)):
            self.type = valuetype
        if (valuetype is None):
            bail(f"mod operation not supported for type {valuetype}")
        return DLGExpression(self.objp4, MOD, self, value, type=self.type)

    def add(self, value):
        return self.__add__(value)

    def sub(self, value):
        return self.__sub__(value)

    def mul(self, value):
        return self.__mul__(value)

    def truediv(self, value):
        return self.__truediv__(value)

    def div(self, value):
        return self.__truediv__(value)

    def mod(self, value):
        return self.__mod__(value)

    def substr(self, value, distinct=None):
        if (isinstance(value, tuple) is True):
            if (len(value) == 1):
                value += (None,)
            value = slice(*value)
        elif (isinstance(value, int) is True):
            value = slice(*(value, None))
        return DLGExpression(self.objp4, SUBSTR, self, value, distinct=distinct, type='integer')

    def count(self, distinct=None, **kwargs):
        ''' USAGE:

                >>> counter = jnl.rev.count()
                >>> print db(person).select(counter).first()(counter)
                1

                >>> qry = (p4.clients.client.count(distinct=distinct))
                >>> records = oP4(qry).select(distinct=distinct)

                or try:
                >>> count = p4.changes.status.count()
        '''
        return DLGExpression(self.objp4, COUNT, self, distinct=distinct, type='integer', **kwargs)
        #return DLGExpression(self.objp4, COUNT, self, groupby=groupby, distinct=distinct, type='integer')

    def sum(self):
        return DLGExpression(self.objp4, SUM, self, None, type=self.type)

    def max(self):
        return DLGExpression(self.objp4, MAX, self, type=self.type)

    def min(self):
        return DLGExpression(self.objp4, MIN, self, type=self.type)

    def diff(self, value):
        return DLGExpression(self.objp4, DIFF, self, value, type='integer')

    def len(self):
        return DLGExpression(self.objp4, LEN, self, type='integer')

    def avg(self):
        return DLGExpression(self.objp4, AVG, self, type=self.type)

    def abs(self):
        return DLGExpression(self.objp4, ABS, self, type=self.type)

    def regexp(self, value):
        return DLGQuery(self.objp4, REGEXP, self, value)

    def match(self, value):
        ''' USAGE:
                >>> qry = (oP4.files.depotFile.match('^\/\/dev\/.*.py$'))
                >>> records = oP4(qry).select()
        '''
        return DLGQuery(self.objp4, MATCH, self, value)

    def search(self, value):
        ''' USAGE:
                >>> qry = (oP4.clients.client.search('art'))
                >>> records = oP4(qry).select()
        '''
        return DLGQuery(self.objp4, SEARCH, self, value)

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

    def datetime(self):
        return DLGExpression(self.objp4, DATETIME, self, 'datetime', type='datetime')

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

def REGEXP(*args, **kwargs):
    return clsREGEXP()(*args, **kwargs)

def MATCH(*args, **kwargs):
    return clsMATCH()(*args, **kwargs)

def SEARCH(*args, **kwargs):
    return clsSEARCH()(*args, **kwargs)

def DIFF(*args, **kwargs):
    return clsDIFF()(*args, **kwargs)

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

def DIV(*args, **kwargs):
    return clsTRUEDIV()(*args, **kwargs)

def TRUEDIV(*args, **kwargs):
    return clsTRUEDIV()(*args, **kwargs)

def MOD(*args, **kwargs):
    return clsMOD()(*args, **kwargs)

def BELONGS(*args, **kwargs):
    return clsBELONGS()(*args, **kwargs)

def ON(*args, **kwargs):
    return clsON()(*args, **kwargs)

def SUM(*args, **kwargs):
    return clsSUM()(*args, **kwargs)

def COUNT(*args, **kwargs):
    return clsCOUNT()(*args,**kwargs)

def SUBSTR(*args, **kwargs):
    return clsSUBSTR()(*args, **kwargs)

def COALESCE(*args, **kwargs):
    return clsCOALESCE()(*args, **kwargs)

def COALESCE_ZERO(*args, **kwargs):
    return clsCOALESCE_ZERO()(*args, **kwargs)

def REPLACE(*args, **kwargs):
    return clsREPLACE()(*args, **kwargs)

def DATETIME(*args, **kwargs):
    return clsDATETIME()(*args, **kwargs)

def YEAR(*args, **kwargs):
    return clsYEAR()(*args, **kwargs)

def MONTH(*args, **kwargs):
    return clsMONTH()(*args, **kwargs)

def DAY(*args, **kwargs):
    return clsDAY(**kwargs)(*args)

def HOUR(*args, **kwargs):
    return clsHOUR()(*args, **kwargs)

def MINUTE(*args, **kwargs):
    return clsMINUTE()(*args, **kwargs)

def SECOND(*args, **kwargs):
    return clsSECOND()(*args, **kwargs)

def EPOCH(*args, **kwargs):
    return clsEPOCH()(*args, **kwargs)

def JOIN(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsJOIN()(*args, **kwargs)

def JOIN_LEFT(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsJOIN_LEFT()(*args, **kwargs)

def ALLOW_NONE(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsALLOW_NONE()(*args, **kwargs)

def PRIMARYKEY(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsPRIMARYKEY()(*args, **kwargs)

def EXTRACT(*args, **kwargs):
    return clsNOTIMPLEMENTED(**kwargs)(*args)
    #return clsEXTRACT()(*args, **kwargs)

def BETWEEN(*args, **kwargs):
    #return clsBETWEEN()(*args, **kwargs)
    return clsNOTIMPLEMENTED(**kwargs)(*args)

class QClass(object):
    expcache = {}
    __init__ = lambda self, *args, **kwargs: self.__dict__.update(*args, **kwargs)

    def is_bool_int_or_float(self, *exps):
        comparaisontypes = (
            bool,
            int,
            float,
            str
        )
        try:
            return all(
                [
                    True \
                        if (isinstance(item, comparaisontypes) is True) \
                        else False for item in exps
                ]
            )
        except: return False

    def cache(self, value):
        try:
            valcache = self.expcache[value]
        except KeyError:
            valcache = self.expcache[value] = re.compile(value)
        return valcache

    def rounder(self, n):
        try:
            return round(n) \
                if ((n % 1) == 0) \
                else round(n, 2)
        except Exception as err:
            bail(err)

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
            elif (
                    (left is not None) |
                    (right is not None)
            ):
                built = {'op': op}
            else:
                bail(f"Operator not supported: `{opname}`")
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
            expresult = self.evaluate(expvalue) \
                if (isinstance(expvalue, serializable)) \
                else expvalue
            ret.mergeright(expresult)
        return ret

    def evaluate(self, qry, record=None):
        if (record is not None):
            try:
                # "@some[?,#,?#,op]@[?,#,?#,op]"
                (tablename, field, value, op) = (None, None, None, None)
                if (type(qry).__name__ in ('Storage', 'DLGQuery', 'DLGExpression')):
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

    def define_type(self, left, right, clsName, *exps):
        try:
            if (type(left).__name__ == 'Storage'):
                return Storage({'op': clsName, 'left': left, 'right': right})
            elif (type(left).__name__ == 'DLGQuery'):
                return DLGQuery(left.objp4, clsName, left, right)
            elif (type(left).__name__ == 'DLGExpression'):
                return DLGExpression(left.objp4, clsName, left, right)
            elif (type(left) == type(right)):
                return reduce(lambda left, right: (left == right), exps)
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                return Storage(
                    {
                        'op': clsName,
                        'left':
                            {
                                'tablename': tablename,
                                'fieldname': fieldname
                            },
                        'right': value
                    }
                )
            else:
                return self.build_query(
                    left,
                    right,
                    clsName
                )
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or clsName
            ), name=clsName.__name__, err=err)


    def op_error(self, *items, name=None, err=None):
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
    def __call__(self, *exp, **kwargs):
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
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left & right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, AND)

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
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left | right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, OR)

class clsXOR(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left ^ right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, XOR)

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
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        if (right is not None):
            bail(f"clsINVERT - right MUST be None! Check itn out.")
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (
                    (
                            (len(exps) == 2) &
                            (exps(1) is None)
                    ) |
                    (len(exps) == 1)
            ):
                return not left
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, INVERT)

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
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        inversion = True
        if (right is not None):
            bail(f"clsNOT - right MUST be None! Check itn out.")

        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (
                    (
                            (len(exps) == 2) &
                            (exps(1) is None)
                    ) |
                    (len(exps) == 1)
            ):
                return not left
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, NOT)

class clsNE(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if ((bif is True) | (type(left) == type(right))):
            if (
                    (len(exps) == 2) &
                    (type(left) == type(right))
            ):
                return reduce(lambda left, right: (left != right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, NE)

class clsEQ(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if ((bif is True) | (type(left) == type(right))):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left == right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        ''' as with many others, it would be nice to take advantage of `self.define_type`
            instead of the bloc of code below, but... since we can compare types other than 
            `str`, and since `self.define_type' does in fact compare both the left and right 
            sides, it causes comparison results to be ambiguous because both conditions 
            below would be True (therefore yielding different results). 
            I.e:
            
            elif (type(left) == type(right)):
                return reduce(lambda left, right: (left == right), exps)
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                ...
        '''
        try:
            if (type(left).__name__ == 'Storage'):
                qres = Storage({'op': EQ, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                qres = DLGQuery(left.objp4, EQ, left, right)
            elif (is_expressionType(left) is True):
                qres = DLGExpression(left.objp4, EQ, left, right)
            elif (is_fieldType(left) is True):
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
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or EQ
            ), name='EQ', err=err)

class clsLT(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left < right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, LT)

class clsLE(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left <= right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, LE)

class clsGT(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left > right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, GT)

class clsGE(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left >= right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, GE)

class clsCONTAINS(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (self.cache(f"{right}").search(left) is not None), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        elif (is_array(right) is True):
            right = tuple([right])
            exps.pop(1)
            exps.insert(1, right)
            return True \
                if (left in right) \
                else False
        return self.define_type(left, right, CONTAINS)

class clsBELONGS(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        if (is_array(right) is False):
            if (type(right).__name__ == 'Select'):
                right = tuple(right.infield)
            else:
                right = tuple([right])
            exps.pop(1)
            exps.insert(1, right)
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left in right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        try:
            if (
                    (type(left).__name__ == 'Storage') &
                    (is_array(right) is True)
            ):
                qres = Storage({'op': BELONGS, 'left': left, 'right': right})
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
            elif (isinstance(left, (str, int)) is True):
                left = str(left)
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    qres = reduce(lambda left, right: (left in right), exps)
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
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or BELONGS
            ), name='BELONGS', err=err)

class clsON(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (self.cache(f"^{right}").match(left) is not None), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, ON)

class clsSTARTSWITH(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (self.cache(f"^{right}").search(left) is not None), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, STARTSWITH)

class clsENDSWITH(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (self.cache(f"{right}$").search(left) is not None), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, ENDSWITH)

class clsREGEXP(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (self.cache(f"{right}").search(left) is not None), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_style(left, right, REGEXP)

class clsMATCH(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (self.cache(f"{right}").search(left) is not None), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_style(left, right, MATCH)

class clsSEARCH(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (self.cache(f"{right}").search(left) is not None), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_style(left, right, SEARCH)

class clsCOUNT(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        (
            groupby,
            distinct
        ) = \
            (
                kwargs.groupby,
                kwargs.distinct
            )
        (fieldname, grprecs, groups, groupbyfield, distinctfield) = \
            (None, None, None, None, None)
        is_join = right.is_join \
            if (is_recordsType(right is True)) \
            else False
        if (groupby is not None):
            groupbyfield = groupby.fieldname \
                if (is_fieldType(groupby) is True) \
                else groupby
        if (distinct is None):
            distinct = False
        else:
            if (isinstance(distinct, bool) is False):
                distinctfield = distinct.fieldname \
                    if (is_fieldType(distinct) is True) \
                    else distinct.left.fieldname \
                    if (is_query_or_expressionType(distinct) is True) \
                    else distinct
            elif (groupbyfield is not None):
                distinctfield = groupbyfield
            else:
                distinctfield = left.fieldname
            distinct = True
        try:
            right.counts = Storage()
            distinctrecords = Storage()
            if (
                    (is_recordsType(right) is True) |
                    (type(right) is enumerate)
            ):
                if (
                        (groupbyfield is None) &
                        (distinctfield is not None)
                ):
                    groupbyfield = distinctfield
                if (is_join is True):
                    grprecords = right.groupby(groupbyfield, as_groups=True)
                    for (grpname, grprecs) in grprecords.items():
                        for record in grprecs:
                            record['count'] = len(grprecs)
                            if (
                                    (distinct is True) &
                                    (not record[distinctfield] in distinctrecords)
                            ):
                                distinctrecords.record[distinctfield] = record
                    if (distinct is True):
                        right = distinctrecords.getvalues()
                return right
            elif (is_array(right) is True):
                return len(right)
            elif (is_array(left) is True):
                return len(left)
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or MAX
            ), name='COUNT', err=err)

class clsMOD(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (left % right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        if (
                (isinstance(left, list) is True) &
                (right is None)
        ):
            bail(f'First argument must be a number or an expression, got `{type(left)}`.')
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': MOD, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, MOD, left, right)
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname if (hasattr(left, 'fieldname')) else left
                leftvalue = float(left.right)
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    #res = []
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(record, dict) is True)
                        ):
                            nvalue = int(record[fieldname] % leftvalue)
                            #res.append(res)
                            #record.update(**{'add': total})
                            record[fieldname] = nvalue
                    return right
                    #return res
                elif (
                        (
                                (is_recordType(right) is True),
                                (isinstance(right, dict) is True),
                                (is_recordType(left))
                        ) & (isnum(left) is True)
                ):
                    return int(right[fieldname] % leftvalue)
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (
                        (tablename, fieldname) == (None, None)
                ):
                    return reduce(lambda left, right: (left % right), exps)
            return right
        except ValueError as err:
            self.op_error(
                lambda left, right, op: (
                    left,
                    right,
                    op or SUM
                ),
                name='SUM',
                err=err
            )

class clsSUM(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (int(left) + int(right)), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)

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
                if (is_expressionType(left.left) is True):
                    res = left.left.op(left.left, right)
                    for item in res:
                        qsum += int(item)
                else:
                    for record in right:
                        qsum += int(record[fieldname])
                '''
                right.sums = Storage()
                grprecords = right.groupby(fieldname, as_groups=True)
                for (grpname, grprecs) in grprecords.items():
                    grpsum = 0
                    for grprec in grprecs:
                        grpsum += int(grprec[fieldname])
                    grpsum = self.rounder(grpsum)
                    right.sums.merge({grpname: grpsum})
                '''
                return qsum
            elif (is_array(right) is True):
                for item in right:
                    qsum += int(item)
                qsum = self.rounder(qsum)
                return qsum
            elif (is_array(left) is True):
                for item in left:
                    qsum += int(item)
                qsum = self.rounder(qsum)
                return qsum
            if (right is not None):
                qsum = self.rounder(qsum)
                right.sum = qsum
                return right
        except ValueError as err:
            self.op_error(
                lambda left, right, op: (
                    left,
                    right,
                    op or SUM
                ),
                name='SUM',
                err=err
            )

class clsAVG(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
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
                asum = (sum([float(record[fieldname]) for record in right]) / length)
                '''
                asum = sum([float(record[fieldname]) for record in right])
                right.avgs = Storage()
                grprecords = right.groupby(fieldname, as_groups=True)
                for (grpname, grprecs) in grprecords.items():
                    grpaverage = (sum([int(grprec[fieldname]) for grprec in grprecs]) / len(grprecs))
                    grpavg = self.rounder(grpaverage)
                    right.avgs.merge({grpname: grpavg})
                '''
            elif (is_array(right) is True):
                length = len(right)
                asum = sum([float(item) for item in right])
            elif (is_array(left) is True):
                length = len(left)
                asum = sum([float(item) for item in left])
            average = (asum / length)
            average = round(average) \
                    if ((average % 1) == 0) \
                    else round(average, 2)
            setattr(right, 'avg', average)
            return right
        except Exception as err:
            self.op_error(
                lambda left, right, op: (
                    left,
                    right,
                    op or AVG
                ),
                     name='AVG',
                     err=err
            )

class clsLEN(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        if (
                (right is None) &
                (is_fieldType(left) is True)
        ):
                return DLGExpression(left.objp4, LEN, left, type='integer')
        if (
                (right is None) &
                (
                        (isinstance(left, (str, bool)) is True) |
                        (is_array(left) is True)
                )
        ):
            try:
                return len(left)
            except Exception as err:
                bail(err)
        fieldname = left.fieldname \
            if (
                (is_fieldType_or_expressionType(left) is True) |
                (is_tableType(left) is True)
        ) else left
        try:
            if (
                    (is_recordsType(right) is True) |
                    (isinstance(right, list) is True) |
                    (type(right) is enumerate)
            ):
                lengths = Lst()
                [lengths.append(len(record[fieldname])) for record in right]
                return lengths
            elif (
                    (is_recordType(right) is True) |
                    (isinstance(right, dict) is True)
            ):
                return len(right[fieldname])
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or LEN
            ),name='LEN', err=err)

class clsABS(QClass):
    def __call__(self, *exps, update_records=False, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        if (
                (right is None) &
                (isinstance(left, (str, int, float)) is True)
        ):
            left_type = type(left)
            if (left_type is str):
                left = float(left)
            left = abs(left)
            return left_type(left)
        try:
            fieldname = left.fieldname if (
                    (is_fieldType_or_expressionType(left) is True) |
                    (is_tableType(left) is True)
            ) else left

            if (
                    (is_recordsType(right) is True) |
                    (isinstance(right, list) is True) |
                    (type(right) is enumerate)
            ):
                if (update_records is False):
                    absvalues = Lst()
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(right, dict) is True)
                        ):
                            value = record[fieldname]
                            if (
                                    (isinstance(value, str) is True) &
                                    (isnum(value) is True)
                            ):
                                value = float(value)
                            if (isinstance(value, (int, float)) is True):
                                absvalue = abs(value)
                                absvalues.append(absvalue)
                    return absvalues
                else:
                    #updated_records = Lst()
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(right, dict) is True)
                        ):
                            current_value = record[fieldname]
                            current_type = type(current_value)
                            updvalue = abs(float(current_value))
                            updvalue = current_type(updvalue)
                            record[fieldname] = updvalue
                            #updated_records.append(record)
                    #updated_records = Records(updated_records)
                    return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or ABS
            ), name='ABS', err=err)

class clsMIN(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        (values, minvalue) = (set(), None)
        oDateTime = DLGDateTime()
        if (
                (right is None) &
                (is_array(left) is True)
        ):
            try:
                [values.add(float(value)) for value in left]
                minvalue = min(values)
                return self.rounder(minvalue)
            except Exception as err:
                bail(err)
        try:
            fieldname = left.fieldname if (
                    (is_fieldType_or_expressionType(left) is True) |
                    (is_tableType(left) is True)
            ) else left
            if (
                    (is_recordsType(right) is True) |
                    (isinstance(right, list) is True) |
                    (type(right) is enumerate)
            ):
                for record in right:
                    if (
                            (is_recordType(record) is True) |
                            (isinstance(right, dict) is True)
                    ):
                        value = record[fieldname]
                        if (isinstance(oDateTime.guess(value), (datetime, date, time)) is True):
                            value = oDateTime.to_epoch(value)
                        if (value is not None):
                            values.add(float(value))
                minvalue = self.rounder(min(values))
                return minvalue
            #right.min = minvalue
            #return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or MIN
            ), name='MIN', err=err)

class clsMAX(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        (values, maxvalue) = (set(), None)
        oDateTime = DLGDateTime()
        if (
                (right is None) &
                (is_array(left) is True)
        ):
            try:
                [values.add(float(value)) for value in left]
                maxvalue = max(values)
                return self.rounder(maxvalue)
            except Exception as err:
                bail(err)
        try:
            fieldname = left.fieldname if (
                    (is_fieldType_or_expressionType(left) is True) |
                    (is_tableType(left) is True)
            ) else left
            if (
                    (is_recordsType(right) is True) |
                    (isinstance(right, list) is True) |
                    (type(right) is enumerate)
            ):
                for record in right:
                    if (
                            (is_recordType(record) is True) |
                            (isinstance(right, dict) is True)
                    ):
                        value = record[fieldname]
                        if (isinstance(oDateTime.guess(value), (datetime, date, time)) is True):
                            value = oDateTime.to_epoch(value)
                        if (value is not None):
                            values.add(float(value))
                maxvalue = self.rounder(max(values))
                return maxvalue
            #right.max = maxvalue
            #return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or MAX
            ), name='MAX', err=err)

class clsADD(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (int(left) + int(right)), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        if (
                (isinstance(left, list) is True) &
                (right is None)
        ):
            bail(f'First argument must be a number or an expression, got `{type(left)}`.')
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': ADD, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, ADD, left, right)
            elif (is_fieldType_or_expressionType(left) is True):
                (fieldname, leftvalue) = (None, None)
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    fieldname = left.fieldname if (hasattr(left, 'fieldname')) else left
                    leftvalue = float(left.right)
                    #res = []
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(record, dict) is True)
                        ):
                            nvalue = int(record[fieldname] + leftvalue)
                            #res.append(res)
                            #record.update(**{'add': total})
                            record[fieldname] = nvalue
                    return right
                    #$return res
                elif (
                        (
                                (is_recordType(right) is True) |
                                (isinstance(right, dict) is True)
                        ) &
                        (isnum(left) is True)
                ):
                    return int(right[fieldname] + leftvalue)
                else:
                    return DLGExpression(left.objp4, ADD, left, right)
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (
                        (tablename, fieldname) == (None, None)
                ):
                    return reduce(lambda left, right: (left + right), exps)
            return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or ADD
            ), name='ADD', err=err)

class clsSUB(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (int(left) - int(right)), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        if (
                (isinstance(left, list) is True) &
                (right is None)
        ):
            bail(f'First argument must be a number or an expression, got `{type(left)}`.')
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': SUB, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, SUB, left, right)
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname \
                    if (hasattr(left, 'fieldname')) \
                    else left
                leftvalue = float(left.right)
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    #ints = Lst()
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(record, dict) is True)
                        ):
                            nvalue = int(record[fieldname] - leftvalue)
                            record[fieldname] = nvalue
                            #ints.append(nvalue)
                            #record.update(**{'sub': total})
                    return right
                    #return ints
                elif (
                        (
                                (is_recordType(right) is True) |
                                (isinstance(right, dict) is True)
                        ) &
                        (isnum(left) is True)
                ):
                    return int(right[fieldname] - leftvalue)
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (
                        (tablename, fieldname) == (None, None)
                ):
                    return reduce(lambda left, right: (left - right), exps)
            return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or SUB
            ), name='SUB', err=err)

class clsMUL(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (int(left) * int(right)), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        if (
                (isinstance(left, list) is True) &
                (right is None)
        ):
            bail(f'First argument must be a number or an expression, got `{type(left)}`.')
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': MUL, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, MUL, left, right)
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname if (hasattr(left, 'fieldname')) else left
                leftvalue = float(left.right)
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True)
                ):
                    #ints = Lst()
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(record, dict) is True)
                        ):
                            nvalue = int(record[fieldname] * leftvalue)
                            record[fieldname] = nvalue
                            #ints.append(nvalue)
                            #record.update(**{'sub': total})
                    return right
                    #return ints
                elif (
                        (
                                (is_recordType(right) is True),
                                (isinstance(right, dict) is True),
                                (is_recordType(left))
                        ) & (isnum(left) is True)
                ):
                    return int(right[fieldname] * leftvalue)
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (
                        (tablename, fieldname) == (None, None)
                ):
                    return reduce(lambda left, right: (left * right), exps)
            return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or MUL
            ), name='MUL', err=err)

class clsTRUEDIV(QClass):
    def __call__(self, *exps, force_int=True, **kwargs):
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
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: int(left / right)
                if (force_int is True)
                else (left / right), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        if (
                (isinstance(left, list) is True) &
                (right is None)
        ):
            bail(f'First argument must be a number or an expression, got `{type(left)}`.')
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': TRUEDIV, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, TRUEDIV, left, right)
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname if (hasattr(left, 'fieldname')) else left
                leftvalue = int(left.right)
                if (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True) |
                        (type(right) is enumerate)
                ):
                    #ints = Lst()
                    for record in right:
                        if (
                                (is_recordType(record) is True) |
                                (isinstance(record, dict) is True)
                        ):
                            nvalue = int(record[fieldname] / leftvalue) \
                                if (force_int is True) \
                                else (record[fieldname] / leftvalue)
                            record[fieldname] = nvalue
                            #ints.append(nvalue)
                            #record.update(**{'TRUEDIV': total})
                    return right
                    #return ints
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
                return reduce(lambda left, right: int(left / right)
                    if (force_int is True)
                    else (left / right), exps)
            elif (isinstance(left, str) is True):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if (
                        (tablename, fieldname) == (None, None)
                ):
                    return reduce(lambda left, right: int(left / right)
                    if (force_int is True)
                    else (left / right), exps)
            return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or TRUEDIV
            ), name='TRUEDIV', err=err)

class clsLIKE(QClass):
    ''' case sensitive '''
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (isinstance(right, str)):

                if (right.startswith('%') is True):
                    right = f"{right}$" \
                        if (right.endswith('%') is False) \
                         else f"^.*{right}.*$"
                elif (right.endswith('%') is True):
                    if (right.startswith('%') is False):
                        right = f"{right}.*$"
                else:
                    right = f"^.*{right}.*$"
                right = re.sub('%', '', right)

                # check for `/` (as in `move/delete` in files.action fields)

                if (len(exps) == 2):
                    return reduce(lambda left, right: (self.cache(right).search(left) is not None), exps)
                else:
                    allres = sum(i and 1 or 0 for i in exps)
                    return (len(exps) == allres)
        res = self.define_type(left, right, LIKE)
        return res

class clsILIKE(QClass):
    ''' case insensitive '''
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (isinstance(right, str)):
                if (right.startswith('%') is True):
                    right = f"{right}$" \
                        if (right.endswith('%') is False) \
                        else f"^.*{right}.*$"
                elif (right.endswith('%') is True):
                    if (right.startswith('%') is False):
                        right = f"{right}.*$"
                else:
                    right = f"^.*{right}.*$"
                (right, left) = (re.sub('%', '', right).lower(), left.lower())

                # check for `/` (as in `move/delete` in files.action fields)

                if (len(exps) == 2):
                    return reduce(lambda left, right: (self.cache(right).search(left) is not None), exps)
                else:
                    allres = sum(i and 1 or 0 for i in exps)
                    return (len(exps) == allres)
        res = self.define_type(left, right, ILIKE)
        return res

class clsSUBSTR(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        if (
                (right is None) &
                (
                        (isinstance(left, (str, bool)) is True) |
                        (is_array(left) is True)
                )
        ):
            try:
                return len(left)
            except Exception as err:
                bail(err)

        fieldname = left.fieldname if (
                (is_fieldType_or_expressionType(left) is True) |
                (is_tableType(left) is True)
        ) else left

        try:
            if (
                    (is_recordsType(right) is True) |
                    (isinstance(right, list) is True) |
                    (type(right) is enumerate)
            ):
                [record.merge({'len': len(record[fieldname])}) for record in right]
                return right
            elif (
                    (is_recordType(right) is True) |
                    (isinstance(right, dict) is True)
            ):
                return right[fieldname][left.right]
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or LEN
            ),name='LEN', err=err)

class clsLOWER(QClass):
    ''' convert a field value to lower case -> it should also support like(), so implement it!
    >>> name = f'{name[0:3]}%'
    >>> recs = p4(p4.users.user.lower().like('zer%')).select():
    >>> print(recs(0))
    zerdlg
    '''
    def __call__(self, *exps, update_records=False, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        if (right is None):
            if (isinstance(left, str) is True):
                return left.lower()
            elif (is_array(left) is True):
                return ALLLOWER(left)
        if (isinstance(left, dict) is True):
            return Storage({'op': LOWER, 'left': left, 'right': right})
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
                    #res = Lst()
                    for record in right:
                        #if (update_records is True):
                        record.update(**{fieldname: record[fieldname].lower()})
                        #else:
                        #    res.append(record[fieldname].lower())
                    #if (len(res) > 0):
                    #    return res
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
                self.op_error(lambda left, right, op: (
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
    def __call__(self, *exps, update_records=False, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        if (right is None):
            if (isinstance(left, str) is True):
                return left.upper()
            elif (is_array(left) is True):
                return ALLUPPER(left)
        if (isinstance(left, dict) is True):
            return Storage({'op': UPPER, 'left': left, 'right': right})
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
                    [record.update(**{fieldname: record[fieldname].upper()}) for record in right]
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
                self.op_error(lambda left, right, op: (
                    left,
                    right,
                    op or UPPER
                ), name='UPPER', err=err)

class clsDIFF(QClass):
    ''' An easy-peasy field differ. You can diff a str against a specified record field value.

        * you can specify the diff format (withb the `difftype` keyword)
            eg. difftype=difflib.ndiff          (default)
                difftype=difflib.unified_diff
                difftype=difflib.context_diff
                difftype=difflib.Differ()       (invokes `compare`)

        * `update_record` keyword (default is False).
            if True, the record's field value will be updated to contain the diff's delta string.
            if False, an extra field will be added to each record containing the delta string.

            eg. >>> recs = p4(p4.changes).select(p4.changes.desc.diff('updates'), difftype=ndiff, update_record=False)
                >>> print(recs.first().desc)
                updated

                >>> print(recs.first().diff)
                - updated
                ?       ^

                + updates
                ?       ^

                >>> recs = p4(p4.changes).select(p4.changes.desc.diff('updates'), difftype=ndiff, update_record=True)
                >>> print(recs.first().desc)
                - updated
                ?       ^

                + updates
                ?       ^
    '''
    def __call__(self, *exps, update_record=False, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        default = ndiff #unified_diff context_diff
        difftype = kwargs.difftype or default
        if (type(difftype) == Differ):
            difftype = difftype.compare
        fieldname = left.fieldname \
            if (is_fieldType_or_expressionType(left) is True) \
            else left
        bif = self.is_bool_int_or_float(*exps)
        try:
            if (
                    (bif is True) &
                    (len(exps) == 2)
            ):
                left_diffitems = left.split('\n')
                right_diffitems = right.split('\n')
                return '\n'.join([line for line in difftype(left_diffitems, right_diffitems)])
            elif (
                    (is_recordType(left) is True) &
                    (isinstance(right, str) is True)
            ):
                left_diffitems = left[fieldname].split('\n')
                right_diffitems = right.split('\n')
                return '\n'.join([line for line in difftype(left_diffitems, right_diffitems)])
            elif (
                    (is_expressionType(left) is True) &
                    (
                            (is_recordsType(right) is True) |
                            (isinstance(right, list) is True) |
                            (type(right) is enumerate)
                    )
            ):
                for rec in right:
                    left_diffitems = rec[fieldname].split('\n')
                    right_diffitems = left.right.split('\n')
                    diff = '\n'.join([line for line in difftype(left_diffitems, right_diffitems)])
                    if (update_record is True):
                        rec[fieldname] = diff
                    else:
                        setattr(rec, 'diff', diff)
                return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or DIFF
            ), name='DIFF', err=err)


class clsREPLACE(QClass):
    ''' Replace a target field's value with another.
    '''
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right, value) = (exps(0), exps(1), exps(2))
        bif = self.is_bool_int_or_float(*exps)
        if (bif is True):
            if (len(exps) == 2):
                return reduce(lambda left, right: (self.cache(right).sub(left, value) is not None), exps)
            else:
                allres = sum(i and 1 or 0 for i in exps)
                return (len(exps) == allres)
        return self.define_type(left, right, REPLACE)

class clsSUBST(QClass):
    ''' Using regex, update a target field's value (like re.sub)
    '''
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))


class clsBETWEEN(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))


class clsPRIMARYKEY(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))


class clsEXTRACT(QClass):
    ''' extract a well formatted archive.
        Should support: tar, zip, gzip, gzip.tar, etc.
    '''
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))


class clsCOALESCE(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))


class clsCOALESCE_ZERO(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))

class clsEPOCH(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        oDateTime = DLGDateTime()
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': EPOCH, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, EPOCH, left, right)
            elif (isinstance(left, (int, float, datetime)) is True):
                return oDateTime.to_epoch(left)
            elif (
                    (isinstance(left, str) is True) &
                    (right in ('epoch', None))
            ):
                (tablename, fieldname, value, op) = self.strQryItems(left)
                if ((tablename, fieldname) == (None, None)):
                    return oDateTime.to_epoch(left)
                #dt = oDateTime.to_datetime(left)
                #return oDateTime.to_epoch(dt)
                return oDateTime.to_epoch(left)
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname \
                    if (is_fieldType_or_expressionType(left) is True) \
                    else left
                if (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    epoch = oDateTime.to_epoch(right[fieldname])
                    return epoch
                elif (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True) |
                        (type(right) == enumerate)
                ):
                    #epochs = Lst()
                    for record in right:
                        epoch = oDateTime.to_epoch(record[fieldname])
                        record[fieldname] = epoch
                        #epochs.append(epoch)
                    #return epochs
                    return right
            else:
                return self.breakdown_query(left, right, EPOCH)
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or EPOCH
            ), name='EPOCH', err=err)

class clsYEAR(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        oDateTime = DLGDateTime()
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': YEAR, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, YEAR, left, right)
            elif (
                    (isinstance(left, str) is True) &
                    (right in ('year', None))
            ):
                dt = oDateTime.to_datetime(left)
                return dt.year
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname \
                    if (is_fieldType_or_expressionType(left) is True) \
                    else left
                if (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    dt = oDateTime.to_datetime(right[fieldname])
                    return dt.year
                elif (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True) |
                        (type(right) == enumerate)
                ):
                    #years = Lst()
                    for record in right:
                        dt = oDateTime.to_datetime(record[fieldname])
                        record[fieldname] = dt.year
                    #    years.append(dt.year)
                    #return years
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or YEAR
            ), name='YEAR', err=err)

class clsDATETIME(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        oDateTime = DLGDateTime()
        ''' convert date/time related record field values.

                                            datetype:

                                                Is the date/time stamp's default return value.

                                                eg.
                                                +----------+-----------------------+
                                                | datetime | '2024/06/29 00:00:00' |
                                                +----------+-----------------------+
                                                | date     | '2014/06/29'          |
                                                +----------+-----------------------+
                                                | time     | '00:00:00'            |
                                                +----------+-----------------------+
                                        '''
        '''
        record = (
            DLGDateTimeConvert(self.objp4)
                  (
                record=record,
                tablename=tablename,
                datetype=datetype
            )
        )
        '''
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': DATETIME, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, DATETIME, left, right)
            elif (
                    (isinstance(left, str) is True) &
                    (right in ('datetime', None))
            ):
                dt = oDateTime.to_datetime(left)
                return dt.year
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname \
                    if (is_fieldType_or_expressionType(left) is True) \
                    else left
                if (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    dt = oDateTime.to_datetime(right[fieldname])
                    return dt.year
                elif (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True) |
                        (type(right) == enumerate)
                ):
                    #datetimes = Lst()
                    oDateTimeConvert = DLGDateTimeConvert(left.objp4)
                    for record in right:
                        #dt = oDateTime.to_datetime(record[fieldname])
                        record = oDateTimeConvert(
                            record=record,
                            tablename=left.tablename,
                            datetype='datetime')
                    return right
                        #datetimes.append(dt)
                    #return datetimes
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or YEAR
            ), name='YEAR', err=err)


class clsMONTH(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        oDateTime = DLGDateTime()
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': MONTH, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, MONTH, left, right)
            elif (
                    (isinstance(left, str) is True) &
                    (right in ('month', None))
            ):
                dt = oDateTime.to_datetime(left)
                return dt.month
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname \
                    if (is_fieldType_or_expressionType(left) is True) \
                    else left
                if (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    dt = oDateTime.to_datetime(right[fieldname])
                    return dt.month
                elif (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True) |
                        (type(right) == enumerate)
                ):
                    #months = Lst()
                    for record in right:
                        dt = oDateTime.to_datetime(record[fieldname])
                        record[fieldname] = dt.month
                        #months.append(dt.month)
                    #return months
                    return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or MONTH
            ), name='MONTH', err=err)

class clsDAY(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        oDateTime = DLGDateTime()
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': DAY, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, DAY, left, right)
            elif (
                    (isinstance(left, str) is True) &
                    (right in ('day', None))
            ):
                dt = oDateTime.to_datetime(left)
                return dt.day
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname \
                    if (is_fieldType_or_expressionType(left) is True) \
                    else left
                if (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    dt = oDateTime.to_datetime(right[fieldname])
                    return dt.day
                elif (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True) |
                        (type(right) == enumerate)
                ):
                    days = Lst()
                    for record in right:
                        dt = oDateTime.to_datetime(record[fieldname])
                        record[fieldname] = dt.day
                        #days.append(dt.day)
                    #return days
                    return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or DAY
            ), name='DAY', err=err)


class clsHOUR(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        oDateTime = DLGDateTime()
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': HOUR, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, HOUR, left, right)
            elif (
                    (isinstance(left, str) is True) &
                    (right in ('hour', None))
            ):
                dt = oDateTime.to_datetime(left)
                return dt.hour
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname \
                    if (is_fieldType_or_expressionType(left) is True) \
                    else left
                if (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    dt = oDateTime.to_datetime(right[fieldname])
                    return dt.hour
                elif (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True) |
                        (type(right) == enumerate)
                ):
                    #hours = Lst()
                    for record in right:
                        dt = oDateTime.to_datetime(record[fieldname])
                        record[fieldname] = dt.hour
                    #    hours.append(dt.hour)
                    #return hours
                    return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or HOUR
            ), name='HOUR', err=err)


class clsMINUTE(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        oDateTime = DLGDateTime()
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': MINUTE, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, MINUTE, left, right)
            elif (
                    (isinstance(left, str) is True) &
                    (right in ('minute', None))
            ):
                dt = oDateTime.to_datetime(left)
                return dt.month
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname \
                    if (is_fieldType_or_expressionType(left) is True) \
                    else left
                if (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    dt = oDateTime.to_datetime(right[fieldname])
                    return dt.minute
                elif (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True) |
                        (type(right) == enumerate)
                ):
                    #minutes = Lst()
                    for record in right:
                        dt = oDateTime.to_datetime(record[fieldname])
                        record[fieldname] = dt.minute
                    #    minutes.append(dt.minute)
                    #return minutes
                    return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or MINUTE
            ), name='MINUTE', err=err)

class clsSECOND(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))
        oDateTime = DLGDateTime()
        try:
            if (isinstance(left, dict) is True):
                return Storage({'op': SECOND, 'left': left, 'right': right})
            elif (is_queryType(left) is True):
                return DLGQuery(left.objp4, SECOND, left, right)
            elif (
                    (isinstance(left, str) is True) &
                    (right in ('second', None))
            ):
                dt = oDateTime.to_datetime(left)
                return dt.second
            elif (is_fieldType_or_expressionType(left) is True):
                fieldname = left.fieldname \
                    if (is_fieldType_or_expressionType(left) is True) \
                    else left
                if (
                        (is_recordType(right) is True) |
                        (isinstance(right, dict) is True)
                ):
                    dt = oDateTime.to_datetime(right[fieldname])
                    return dt.second
                elif (
                        (is_recordsType(right) is True) |
                        (isinstance(right, list) is True) |
                        (type(right) == enumerate)
                ):
                    #seconds = Lst()
                    for record in right:
                        dt = oDateTime.to_datetime(record[fieldname])
                        record[fieldname] = dt.second
                    #    seconds.append(dt.second)
                    #return seconds
                    return right
        except Exception as err:
            self.op_error(lambda left, right, op: (
                left,
                right,
                op or SECOND
            ), name='SECOND', err=err)


class clsJOIN(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))


class clsJOIN_LEFT(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))


class clsALLOW_NONE(QClass):
    def __call__(self, *exps, **kwargs):
        (exps, kwargs) = (Lst(exps), Storage(kwargs))
        (left, right) = (exps(0), exps(1))


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
            "LIKE":         LIKE,
            "ILIKE":        ILIKE,

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
            # Expression(self.objp4, op)
            "JOIN" : JOIN,
            "LEFT_JOIN": JOIN_LEFT,
            "ALLOW_NONE": ALLOW_NONE,

            # DLGExpression(self.objp4, op, left)
            "PRIMARYKEY": PRIMARYKEY,
            "LOWER": LOWER,
            "UPPER": UPPER,
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
            "DIFF": DIFF,
            "COUNT": COUNT,
            "EXTRACT": EXTRACT,
            "SUBSTR": SUBSTR,

            "SUM": SUM,
            "ABS": ABS,
            "AVG": AVG,
            "MIN": MIN,
            "MAX": MAX,
            "TRUEDIV": TRUEDIV,
            "DIV": TRUEDIV,
            "BELONGS": BELONGS,
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
dops = Storage({'equal': (EQ, "EQ", '='),
                'notequal': (NE, "NE", '!='),
                'greaterthan': (GT, "GT", '>'),
                'greaterequal': (GE, "GE", '>='),
                'lesserthan': (LT, "LT", '<'),
                'lesserequal': (LE, "LE", '<='),
 \
                'not': (NOT, 'NOT', '~'),
 \
                'like': LIKE,
                'ilike': ILIKE,
 \
                'contains': (CONTAINS, 'CONTAINS', '#'),
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
               NOT: ('NOT', '~'),
\
               CONTAINS: ('CONTAINS', '#'),
               STARTSWITH: ('STARTSWITH', '#^'),
               ENDSWITH: ('ENDSWITH', '#$'),
               SEARCH: ('SEARCH', '#?'),
               LIKE: ('LIKE', '%'),
               ILIKE: ('ILIKE', '%'),
\
               REGEXP: ('REGEXP', '#&'),
               MATCH: ('MATCH', '##'),
 \
               AND: ('AND', '&'),
               OR: ('OR', '|'),
               XOR: ('XOR', '^')})
method_ops = (
    CONTAINS, 'CONTAINS', '#',
    STARTSWITH, 'STARTSWITH', '#^',
    ENDSWITH, 'ENDSWITH', '#$',
    SEARCH, 'SEARCH', '#?',
    LIKE, 'LIKE', '%',
    ILIKE, 'ILIKE', '%',
)
regex_ops = (
    REGEXP, 'REGEXP', '#&',
    MATCH, 'MATCH', '##',
    SEARCH, 'SEARCH', '#?'
)
equal_ops = (
    EQ, "EQ", '=',
    NE, "NE", '!='
)
comparison_ops = (
    EQ, "EQ", '=',
    NE, "NE", '!=',
    GT, "GT", '>',
    GE, "GE", '>=',
    LT, "LT", '<',
    LE, "LE", '<='
)