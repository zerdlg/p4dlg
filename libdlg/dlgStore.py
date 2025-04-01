from types import *
from pprint import pformat

'''  [$File: //dev/p4dlg/libdlg/dlgStore.py $] [$Change: 678 $] [$Revision: #15 $]
     [$DateTime: 2025/04/01 04:47:46 $]
     [$Author: zerdlg $]
'''

__all__ = [
    'Lst',
    'ZDict',
    'StorageIndex',
    'objectify',
    'unobjectify'
]

def xrange(x):
    return iter(range(x))

class Lst(list):
    __hash__ = lambda self: hash(frozenset(self))

    def iter(self):
        for i in xrange(len(self)):
            yield self[i]

    def __init__(self, *args):
        if (len(args) == 0):
            args = list()
        elif (len(args) == 1):
            args = args[0] \
                if (isinstance(args[0], list))\
                else list(args[0])
        else:
            args = list(args)
        super(Lst, self).__init__(args)

    def __call__(self, idx, default=None, cast=None):
        '''  Don't raise an  exception on IndexError, return default
        '''
        invalid_types = ['NoneType', 'BooleanType']
        (
            value,
            cast
        ) = \
            (self.getvalue(
                len(self),
                idx,
                default,
                cast
            )
        )
        if (value is not None):
            try:
                return cast(value) \
                    if (
                            (not type(cast).__name__ in invalid_types)
                            & (callable(cast) is True)
                ) \
                    else value
            except Exception as err:
                print(err)
        return value

    def copy(self):
        return Lst(self[:])

    def storageindex(
            self,
            reversed=False,
            startindex=0,
            order_by=None,
    ):
        return StorageIndex()(
            self,
            reversed=reversed,
            startindex=startindex,
            order_by=order_by
        )

    def idx_is_valid(self, idx, length):
        try:
            if ((idx < length) & (length > 0)) \
                    | ((-length < idx) & (idx < 0)):
                return True
        except Exception as err:
            print(err)
        return False

    def getvalue(self, length, idx, default, cast):
        defaultvalue = lambda: 0
        ret = (default, False)
        if (self.idx_is_valid(idx, length) is True):
            ret = (self[idx], cast)
        elif (default is defaultvalue):
            ret = (default, cast)
        return ret

    first = lambda self: self(0)
    last = lambda self: self(-1)

    def _getsets(self, *args):
        args = Lst(args)
        return self(
            set(args(0)),
            set(args(1))) \
            if (args(1) is not None) \
            else Lst(
                [
                    set(self),
                    set(args(0))
                ]
        )
    ''' borrowing a from set() for quick operations on 2 lists
        union - intersect - symetric/no_intersect - issubset = issuperset - diff - nodups
        
        s1 = Lst('a', 'b', 'c')
        s2 = Lst('d', 'e', 'f', 'b')
        
        >>> l1.union(12)
        ['c', 'a', 'e', 'b', 'f', 'd']
        
        >>> l1.intersect(l2)
        ['b']
        
        >>> l1.no_intersect(l2)     # same as symetric
        ['a', 'c', 'e', 'f', 'd']
        
        >>> Lst('a', 'b').issubset(l1)
        True
        
        >>> l1.issuperset(Lst('a', 'b'))
        True
        
        >>> l1.diff(l2)
        ['c', 'a']
        
        >>> l1.nodups(l2)
        >>> l1
        ['c', 'a', 'e', 'b', 'f', 'd']
        
        >>> Lst('a', 'b', 'b', 'c', 'c', 'd', 'd', 'd', 'e').nodups()
        ['a', 'b', 'c', 'e', 'd']
        
    '''
    def union(self, *args):
        (first, second) = self._getsets(*args)
        return Lst(first | second)

    def intersect(self, *args):
        (first, second) = self._getsets(*args)
        return Lst(first & second)

    def no_intersect(self, *args):
        return self.symetric(*args)

    def issubset(self, *args):
        (first, second) = self._getsets(*args)
        return (first < second)

    def issuperset(self, *args):
        (first, second) = self._getsets(*args)
        return (first > second)

    def diff(self, *args):
        (first, second) = self._getsets(*args)
        return Lst(first - second)

    def symetric(self, *args):
        (first, second) = self._getsets(*args)
        return Lst(first ^ second)

    def nodups(self, *args):
        if (len(args) == 0):
            return Lst(set(self))
        (first, second) = self._getsets(*args)
        self = Lst(first | second)

    def clean(self, *args, **kwargs):
        ''' remove items from a copy without affecting the original
        '''
        (args, kwargs) = (Lst(args), ZDict(kwargs))
        if (len(args) == 0):
            args = Lst(
                '',
                None,
                [],
                {},
                ()
            )

        iterable = self.copy()
        for arg in args:
            iterable = Lst(
                filter(
                    lambda i: i != arg, iterable
                )
            )
        return iterable

    def moveitem(self, *args):
        args = Lst(args)
        ''' in place item mover
            * expect 2 args (indices): src & dst
        '''
        try:
            (
                args0,
                args1
            ) = \
                (
                    int(args(0)),
                    int(args(1))
                )
            self.insert(args1, self.pop(args0))
        except ValueError:
            print('parameters: source index, dest index')

    def appendleft(self,value):
        self.insert(0,value)

    def indices(self, arg):
        return Lst(idx for (idx, item) in enumerate(self) if (item == arg))

    def replace(self, arg1, arg2, all=False):
        argdices = self.indices(arg1)
        if (all is False):
            argdices = Lst([argdices(0)]).clean()
        for argidx in argdices:
            self.pop(argidx)
            self.insert(argidx, arg2)

    def merge(self, *arglists, all=False, **sub):
        ''' merge Lst, [], () or *items to Lst
            ** substitute old item with new item
            all=True -> replaces all occurences of item
            all=False (default) replace the first occurence of item

            l1 = Lst('a', 'b', 'b', 'c', 'c', 'd', 'd', 'd', 'b', 'e')
            >>> l1.merge(
                        *[['f','g','h', 'b', 'b'], ['z', 'z', 'z', 'z']],
                        all=True,
                        **{'b': 'B', 'a': 'A', 'h': 'H', 'x': 'X'}
                        ).nodups()
            >>> l1
            ['e', 'H', 'A', 'z', 'd', 'B', 'g', 'c', 'f']

            >>> new_list = l1.merge(*['dog', 'cat', 'cow', ('bert', 'ernie', 'zerdlg')], **{'cow': 'kangaroo'})
            >>> new_list
            ['e',
             'H',
             'A',
             'zzz',
             'd',
             'B',
             'g',
             'c',
             'f',
             'dog',
             'cat',
             'kangaroo',
             ('bert', 'ernie', 'zerdlg')]
        '''
        for arg in arglists:
            if (isinstance(arg, list) is False):
                arg = [arg]
            self += arg
        for (key, value) in sub.items():
            self.replace(key, value, all=all)
        return self

class ZDict(dict):
    ''' a dict with object-like attributes (and a few convenient methods) collections
    '''
    __slots__ = ()
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    __getitem__ = dict.get
    __call__ = __getitem__
    __getstate__ = lambda self: None
    __copy__ = lambda self: ZDict(self)
    __hash__ = lambda self: hash(
        (
            frozenset(self),
            frozenset(self.getvalues())
        )
    )

    def __and__(self, other):
        return self.merge(other)

    def keysmap(self):
        keynames = self.getkeys()
        return ZDict(zip([f.lower() for f in keynames], keynames))

    def __getattr__(self, key):
        keysmap = self.keysmap()
        if (str(key).lower() in keysmap):
            key = keysmap[str(key).lower()]
            return self[key]

    def get(self, key, default=None):
        if (str(key).lower() in self.getkeys()):
            key = self.keysmap()[str(key).lower()]
        try:
            return self.__getitem__(key)
        except(
                KeyError,
                AttributeError,
                TypeError
        ):
            return default

    def iterate(self):
        return (self.getlist(key) for key in self.keys())

    def _objectify(self, *args, **kwargs):
        self.update(**self.objectify(*args, **kwargs))

    def _unobjectify(self, *args, **kwargs):
        self.update(**self.unobjectify(*args, **kwargs))

    @staticmethod
    def objectify(any):
        return ZDict(
            {
                key: ZDict.objectify(value) for (key, value) in any.items()
            }
        ) \
            if (type(any) in (dict, ZDict)) \
            else Lst(ZDict.objectify(value) for value in any) \
            if (
                type(any) in (
                list,
                tuple,
                set,
                Lst
            )
        ) \
            else any

    @staticmethod
    def unobjectify(any):
        return dict(
            {
                key: ZDict.unobjectify(value) for (key, value) in any.items()
            }
        ) \
            if (isinstance(any, dict)) \
            else Lst(ZDict.unobjectify(value) for value in any) \
            if (isinstance(any, list)) \
            else any

    def rename(self, oldkey, newkey):
        if (self[oldkey] is not None):
            self[newkey] = self.pop(oldkey)
        return self

    def delete(self, *keys):
        for key in keys:
            if (key in self.getkeys()):
                try:
                    self.__delitem__(key)
                except KeyError as err:
                    print('no such key... {}'.format(key))

    ''' get all occurences of key - otherwise all keys
    '''
    def getlist(self, key = Lst()):
        value = self.get(key, Lst()) \
            if (key is not None) \
            else self.keys()
        return value \
            if (isinstance(value, (list, tuple))) \
            else Lst([value]) \
            if (value is not None) \
            else Lst()

    ''' get first or last occurence of dict[key] - otherwise first or last value of dict keys
    '''
    def getfirst(self):
        return self.getlist().first() or None

    def getlast(self):
        return self.getlist().last() or None

    first = lambda self, x: self.getfirst(x)
    last = lambda self, x: self.getlast(x)

    ''' backward compatibility ?       
    
        d.keys() -> list(d.keys())
        d.values() -> list(d.values())
        d.items() -> list(d.items())
        d.iterkeys() -> iter(d.keys())
        d.itervalues() -> iter(d.values())
        d.iteritems() -> iter(d.items())
        d.viewkeys() -> d.keys()
        d.viewvalues() -> d.values()
        d.viewitems() -> d.items()    
    '''
    ''' keys, values, items
    '''
    def getkeys(self):
        return Lst(self.keys())

    def getvalues(self):
        return Lst(self.values())

    def getitems(self):
        return Lst(self.items())

    ''' iterkeys, itervalues, iteritems
    '''
    def iterkeys(self):
        return iter(self.keys())

    def itervalues(self):
        return iter(self.values())

    def iteritems(self):
        return iter(self.items())

    ''' *** recursion is screwed up! Needs rewrite!

                a safe & non-destructive dict().update()

                overwrite - add_missing - allow_nonevalue - allow_value_askey - extend_listvalues

        overwrite           -->     a.port='avalue'
                                    b.port='rsh:...'
                                    if (overwrite is True):
                                        a.port=b.port
                                        a.port='rsh:...'

        add_missing         -->     a.port="no such key 'port'"
                                    b.port='rsh:...'
                                    if (add_missing is True):
                                        a.port=b.port
                                        a.port='rsh:...'

        allow_nonevalue     -->     a.port='avalue'
                                    b.port=None
                                    if (allow_nonevalue is True):
                                        a.port=b.port
                                        a.port=None

        allow_value_askey   -->     a.port='avalue'
                                    b.port={'avalue':'blabla'}
                                    if (a.port==b.port.getkeys().first()):
                                        if (allow_value_askey is True):
                                            a.port=b.port
                                            a.port={'avalue':'blabla'}
                                    elif (overwrite is True):
                                        a.port={'avalue':'blabla'}
                                    elif (overwrite is False):
                                        a.port='avalue'

        extend_listvalues   -->     a.port=['avalue','bvalue']
                                    b.port=['a','b','c']
                                    if ((type(a.port) in (ListType,Lst()) & (type(b.port in (ListType,Lst))):
                                        if (extend_listvalues is True):
                                            a.port=a.port+b.port
                                            a.port=['avalue','bvalue','a','b','c']
    '''
    def merge(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), ZDict(kwargs))
        constrainsts = ZDict(
            {
                'overwrite': True,
                'add_missing': True,
                'allow_nonevalue': True,
                'allow_value_askey': True,
                'extend_listvalues': True
            }
        )
        [
            setattr(constrainsts, ckey, kwargs.pop(ckey)) for ckey in [
            key for (key, value) in kwargs.items() if key in constrainsts
            ]
        ]

        any = args.storageindex(reversed=True) \
            if (len(args) > 0) \
            else Lst([kwargs]).storageindex(reversed=True) \
            if (len(kwargs) > 0) \
            else Lst().storageindex(reversed=True)

        def mergedict(anydict):
            def updater(key, value):
                gkeys = self.getkeys()
                if (key in gkeys):
                    if (
                            (value is None)
                            and (constrainsts.allow_nonevalue is False)
                    ):
                        self.delete(key)
                    elif (
                            (constrainsts.overwrite is True)
                            and (
                                    (value is not None)
                                    or (constrainsts.allow_nonevalue is True)
                            )
                    ):
                        self[key] = value
                elif (constrainsts.add_missing is True):
                    if (
                            (value is not None) \
                            or (constrainsts.allow_nonevalue is True)
                    ):
                        self[key] = value

            for key in anydict.getkeys():
                value = anydict[key]
                if (isinstance(value, dict)):
                    if (len(value) > 0):
                        if (
                                (value.getkeys().first() == self[key]) \
                                and (constrainsts.allow_value_askey is True)
                        ):
                            key = value.getkeys().first()
                elif (isinstance(value, list)):
                    if (
                            (isinstance(self[key], list)) \
                            and (constrainsts.extend_listvalues is True)
                    ):
                        value = Lst(value + self[key])
                if (
                        (type(value) is not NoneType) \
                        or (constrainsts.allow_nonevalue is False)
                ):
                    updater(key, value)

        for anyitem in any:
            mergedict(self.objectify(any[anyitem]) \
                    if (isinstance(any[anyitem], (ZDict, StorageIndex)) is False) \
                    else any[anyitem])
        return self

    ''' keys to lower & keys to upper - modifies original
    '''
    def lower(self, **any):
        anydict = ZDict(any) or self
        [
            anydict.update(
                **{
                    key.lower(): anydict.pop(key)
                }
            ) for key in anydict.getkeys() if (isinstance(key, str))
        ]
        return anydict

    def upper(self, **any):
        anydict = ZDict(any) or self
        [
            anydict.update(
                **{
                    key.upper(): anydict.pop(key)}) for key in\
                    anydict.getkeys() if (isinstance(key, str))
        ]
        return anydict

    ''' keys to lower & keys to upper - does not modify original
    '''
    def keystolower(self, **any):
        anydict = ZDict(self.copy() \
              if (len(any) == 0) \
              else any.copy())
        [
            anydict.rename(str(key), str(key).lower()) for key in anydict.getkeys()
        ]
        return anydict

    def keystoupper(self, **any):
        anydict = ZDict(self.copy() \
            if (len(any) == 0) \
            else any.copy())
        [
            anydict.rename(str(key), str(key).upper()) for key in anydict.getkeys()
        ]
        return anydict

    def exclude(self, *args):
        ''' exclude keys but don't modify original
        '''
        d = objectify(self.copy())
        for key in args:
            if (key in self.getkeys()):
                try:
                    d.__delitem__(key)
                except KeyError as err:
                    print('no such key... {}'.format(key))
        return d

(
    objectify,
    unobjectify
) = \
    (
        ZDict.objectify,
        ZDict.unobjectify
    )

class StorageIndex(ZDict):
    __str__ = __repr__ = lambda self: f'<{type(self).__name__}({pformat(dict(self))})>'

    def __init__(self, *args, **kwargs):
        super(StorageIndex, self).__init__(*args, **kwargs)

    def rename(self, oldkey, newkey):
        self[newkey] = self.pop(oldkey)
        return self

    def getkey(self, key):
        return dict(
            zip(
                self.getvalues(),
                self.getkeys()
            )
        )[key]

    def firstkey(self):
        return self.getkeys().first()

    def lastkey(self):
        return self.getkeys().last()

    def mergein(self, item, idx):
        mKeys = Lst(self.getkeys())
        lastkey = mKeys.last()
        idx = 0 \
            if (len(mKeys) == 0) \
            else int(idx)
        if (len(mKeys) == 0):
            return self.merge({0: item})
        if (
                (idx == -1)
                | (idx > mKeys.last())
        ):
            return self.merge({(lastkey + 1): item})
        if (idx >= 0):
            self.shiftright(idx)
            return self.merge({idx: item})
        return self

    ''' shift keys right or left, starting at idx   
    '''
    def shiftright(self, idx=0):
        rkeys = self.getkeys()
        rkeys.reverse()
        for rkey in rkeys:
            if (rkey >= idx):
                self.rename(rkey, rkey + 1)
            else:
                break

    def shiftleft(self, idx):
        rkeys = self.getkeys().copy()
        [self.rename(i, i - 1) for i in rkeys if (i >= idx)]

    def mergeright(self, *any, **kwargs):
        (any, kwargs) = (Lst(any), ZDict(kwargs))
        if (len(any) > 0):
            [self.mergein(item, -1) for item in any]
        elif (len(kwargs) > 0):
            self.mergein(kwargs, -1)
        return self

    def mergeleft(self, *any, **kwargs):
        (
            any,
            kwargs
        ) = \
            (
                Lst(any),
                ZDict(kwargs)
            )
        if (len(any) > 0):
            [self.mergein(item, 0) for item in any]
        elif (len(kwargs) > 0):
            self.mergein(kwargs, 0)
        return self

    def __call__(
            self,
            objLst,
            reversed=False,
            startindex=0,
            order_by=None
    ):
        storelist = Lst()
        if (order_by is not None):
            objLst = Lst(
                sorted(
                    objLst, key=lambda k: k[order_by]
                )
            )
        if (len(objLst) > 0):
            if (type(objLst) is not StorageIndex):
                if (isinstance(objLst(0), tuple) is False):
                    if (reversed is False):
                        [
                            storelist.append((item, enum)) for (enum, item)
                            in enumerate(objLst, startindex)
                        ]
                        storelist = storelist.storageindex(reversed=True)
                    else:
                        try:
                            [
                                storelist.append((enum, item)) for (enum, item)
                                in enumerate(objLst, startindex)
                            ]
                            storelist = StorageIndex(storelist)
                        except Exception as err:
                            return objLst
                elif (reversed is True):
                    storelist = StorageIndex(
                        {
                            j: i for (i, j) in objLst
                        }
                    )
            elif (reversed is True):
                storelist = StorageIndex(
                    {
                        j: i for (i, j) in enumerate(objLst.getvalues())
                    }
                )
        else:
            storelist = StorageIndex(storelist)
        return storelist

    def popkey(self, key):
        return self.pop(str(key)) \
            if (key in self) \
            else None

    def reset_order(
            self,
            st=None,
            start=0,
            reversed=False
    ):
        (
            anystore,
            nkey,
            newstore
        ) = \
            (
                st or self,
                start,
                StorageIndex()
            )
        [
            newstore.mergeright(value) for (key, value) in anystore.items()
        ]
        self = newstore
        return self