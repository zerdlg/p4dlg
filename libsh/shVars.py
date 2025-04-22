import os
from pprint import pformat
from types import *
from typing import *
import textwrap
import re
from datetime import datetime
from pickle import load

from libdlg.dlgStore import Storage, objectify, Lst
from libdlg.dlgUtilities import noneempty, casttype
from libfs.fsFileIO import loadpickle, dumppickle

(
    OSetattr,
    OGetattr,
    ODelattr
) = \
    (
        object.__setattr__,
        object.__getattribute__,
        object.__delattr__
    )
objdict_regex = re.compile('^(\w+)\.([^.]+)$')
now = datetime.now

'''  [$File: //dev/p4dlg/libsh/shVars.py $] [$Change: 691 $] [$Revision: #11 $]
     [$DateTime: 2025/04/19 01:29:22 $]
     [$Author: mart $]
'''

__all__ = ('VARSObject', 'clsVars')

class VARSObject(object):
    '''  TODO:

            why do we have VARSObject???

            a light-weight storage system that provides persistence of key/value pairs of arbitrary
            data from one session to the next.



            hard coded variables within DLGShell (class attributes)

                DLGShell assumes that a variables that start with one of the defined prefixes (within its own
                scop) must be of type Vars (or clsVars). As such, the following rulles will apply.

                hard coded variables (I.e. has a prefix like, self.var_editor = 'blabla'), persist as long as
                they remain hard coded. Though we can't make them disapear (because they are hard coded),
                we can however upate their value. These updated values will persist so long as they are not
                reverted (by given them a None value) in which case, they revert to their initial value (these
                can not be set to None!).

                eg.

                create a hard coded variable in the DLGShell class:

                def __init__(*args, **kwargs):
                    ... (
                    var_myNewVariable = 'Ernie'

                ... In session ...
                >>> configvars('myNewVariable')
                'ernie'

                if this variable gets updated at any point (by any means), it will be force to adopt the new
                value as its default value...

                >>>  configvars('myNewVariable', 'bert')
                >>> configvars('myNewVariable')
                'bert'

                the variable's value will remain as such, from one session to the next (until we attempt to
                delete it by giving it a None value).

                >>> configvars('myNewVariable', None)
                >>> configvars('myNewVariable')
                'ernie'

            Vars (or clsVars) variables created on the fly (in the shell or in script) will also persist from
            one session to the next so long as they are not given a None value, in which case they will simply
            disapear.

                >>> configvars('myOtherVariable', 'bob')
                >>> configvars('myOtherVariable')
                'bob'

                >>> configvars('myOtherValue', None)
                >>> configvars('myOtherVariabler')
                <user begets nothing (None) - variable no longer exists>


            General rules:

            Syntax: its pretty slack... the following are equivalent:

            >>> configvars('editor', 'VIM')
            >>> configvars({'editor', 'VIM'})
            >>> configvars(**{'editor', 'VIM'})
            >>> configvars(editor='VIM')

            we can create/update/delete variables en mass!
            >>> configvars(cat='gareth', snake='lucy', wolf='bernard')


            I find doing something like this is convenient since the shell instance does get passed around
            thereby exposing any and all Vars objects (I.e. configvars, p4vars, jnlvars, qtvars, etc.)


            another thing... I added these things so I wouldn't have to continuously type the attributes
            owner

                this is annoying @ a terminal:
                >>> self.myvar

                this is not annoying:
                >>> myvar

            A Workflow example:
                session 1:
                >>> cvars('bla',['a','b','c',])
                >>> close()

                session 2:
                >>> cvars('bla')
                ['a','b','c',]

                >>> cvars(bla='some_string')
                >>> close()

                session 3:
                >>> cvars('bla')
                'some_string'
                >>> close()

                session 4:
                >>> cvars('bla', None)
                >>> cvars('bla')
                ['a', 'b', 'c',]

    '''

    def getarg(self, key):
        return self.varsobject[key].varvalue \
            if (key in self.varsobject) \
            else None

    def getargs_all(self):
        return objectify({varkey: self.varsobject[varkey].varvalue \
                    for varkey in self.varsobject})

    def __getitem__(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        if ((len(args) + len(kwargs)) == 0):
            '''
                    no args, no kwargs, only one thing to do...
                    return the entire thing to the caller.

                        >>> cvars()
                        {...}
            '''
            return self.getargs_all()

        if (len(args) == 1):
            if (isinstance(args(0), str)):
                '''     retrieve its value
                        >>> cvars('gc')
                        'cat'
                '''
                return self.getarg(args.pop(0), **kwargs)

            elif (isinstance(args(0), list)):
                '''
                args(0) is a List, for now we should allow:

                >>> cvars([None,])                     --> remove *all* sqVars, dynamic args are permanently deleted
                                                            & static args are moved from the current session only

                >>> cvars(['var1', 'var2', var3',])    --> request for the listed var values (return as dict??) 

                >>> cvars([])                          --> empty list, skip / or empty the Vars thing ???? hum...
                '''
                return self.getargs_fromlist(Lst(args.pop(0)), **kwargs)
            elif (isinstance(args(0), dict)):
                kwargs = args.pop(0)

        elif (len(args) == 2):
            kwargs[args(0)] = args(1)

        if (len(kwargs) > 0):
            return self.update_add_remove(**kwargs)
        elif (len(args) > 0):
            '''  there really shouldn't be anything left at this point, but...
            '''
            arg = str(args(0))
            return OGetattr(self, arg) \
                if (isinstance(args(0), str)) \
                else self.__dict__.get(arg, None)

    __setitem__ = lambda self, key, value: setattr(self, str(key), value)
    has_key = __contains__ = lambda self, key: key in self.__dict__
    __nonzero__ = lambda self: len(self.__dict__) > 0
    __getattr__ = __getitem__
    __delitem__ = object.__delattr__
    __copy__ = lambda self: Storage(self)
    __call__ = __getitem__

    def __eq__(self, other):
        try:
            return (self == other)
        except AttributeError:
            return False

    def __ne__(self, other):
        return not (self == other)

    def __copy__(self):
        return Storage(dict(self))

    def __init__(self, clsObj, varsname):
        self.__dict__ = Storage({})
        self.clsObj = clsObj
        self.varsname = varsname
        self.maxtextwidth = 80
        self.varsobject = Storage()
        self.varsdata = self.clsObj.varsdata
        self.prefix = self.varsdata[varsname].prefix \
            if (hasattr(self.varsdata, varsname)) \
            else None
        self.varspickle = Storage()

    def haskeys(self, *args):
        return True \
            if (len(Lst(arg for arg in args if arg in self.getkeys())) == len(self.getkeys())) \
            else False

    def add(self, *args, **kwargs):
        kwargs = Storage(kwargs)
        if (len(Lst(args)) > 0):
            argsdata = {
                iValues(0): iValues(1) \
                    for (idx, iValues) in args.inpairs() \
                    if (not iValues(0) in (self.varsobject, self.varspickle))
            }
            if (noneempty(argsdata) is False):
                kwargs.merge(argsdata)
        return self.update_add_remove(**kwargs)

    def remove(self, *args, **kwargs):
        kwargs = Storage(kwargs)
        if (len(Lst(args)) > 0):
            kwargs.merge(
                {
                    arg: None for arg in args if (arg in (
                    self.varsobject,
                    self.varspickle
                )
                                                  )
                }
            )
        return self.update_add_remove(**kwargs)

    def update(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), Storage(kwargs))
        if (len(Lst(args)) > 0):
            argsdata = {
                iValues(0): iValues(1) for (idx, iValues) in args.inpairs()
                    if (iValues(0) in (self.varsobject, self.varspickle))
            }
            if (noneempty(argsdata) is False):
                kwargs.merge(argsdata)
        return self.update_add_remove(**kwargs)

    def defineargs(self, *args):
        return Lst(args) \
            if (len(args) > 0) \
            else Lst(key for key in self.varsobject) \
            if (noneempty(self.varsobject) is False) \
            else Lst()

    def getargs_from_list(self, argslist):
        argslist = Lst(argslist)
        if (type(argslist.first()) is NoneType):
            return self.getargs_all()
        elif (argslist.sametype(str) is True):
            return Storage(
                {
                    varkey: self.varsobject[varkey].varvalue for varkey \
                        in argslist if (varkey in self.varsobject)
                }
            )

    def formatfieldvalue(self, fieldvalue):
        return pformat(fieldvalue) \
            if (isinstance(fieldvalue, str)) \
            else textwrap.fill(
                textwrap.dedent(fieldvalue).strip(),
                width=self.maxtextwidth
        )

    def update_add_remove(self, **kwargs):
        urows = objectify(
            [
                {
                'varname': varname,
                'varvalue': varvalue,
                'vartype': (type(varvalue) or type(varvalue).__name__),
                'prefixname': f'{self.prefix}_{varname}'
                } for (varname, varvalue) in kwargs.items()
            ]
        )
        rows = Storage()
        for row in urows:
            varname = row.pop('varname')
            row = Storage({varname: row})
            if (row[varname].varvalue is not None):
                self.varsobject.merge(row)
            elif (
                    (row.varvalue is None) \
                    & (varname in self.varsobject)
            ):
                self.varsobject.delete(varname)
                if (row.prefixname in self.clsObj.__dict__.keys()):
                    vvalue = self.clsObj.__dict__[row.prefixname]
                    res = clsVars(self.clsObj, self.varsname)(varname, vvalue)
                    valuetype = dict \
                        if (type(vvalue) is Storage) \
                        else type(vvalue)
                    row[varname].merge(
                        {
                            'varvalue': vvalue,
                            'vartype': valuetype
                        }
                    )
            self.update_varspickle(**row)
            # set_unset='set' if (row[varname].varvalue is not None) else 'unset'
            rows.merge(row)
        self.dump_varspickle()
        return rows

    def define_row(self, **kwargs):
        kwargs = Storage(kwargs)
        varname = kwargs.varname \
            if (noneempty(kwargs.varname) is False) \
            else self.varspickle.first() \
            if (noneempty(self.varspickle.first()) is False) \
            else None
        if (noneempty(varname) is False):
            prefixname = kwargs.prefixname \
                if (noneempty(kwargs.prefixname) is False) \
                else self.varspickle[varname].prefixname \
                if (noneempty(self.varspickle[varname].prefixname) is False) \
                else '{self.prefix}_{varname}'
            vartype = kwargs.vartype.__name__ \
                if (
                    (noneempty(kwargs.vartype) is False) \
                    and (type(kwargs.vartype) is Type)
            ) \
                else self.varspickle[varname].vartype \
                if (noneempty(self.varspickle[varname].vartype) is False) \
                else kwargs.vartype
            precastedvalue = kwargs.varvalue \
                if (kwargs.varvalue is not None) \
                else (self.varspickle[varname].varvalue) \
                if (self.varspickle[varname].varvalue is not None) \
                else None
            varvalue = precastedvalue \
                if (precastedvalue is not None) \
                else casttype(vartype, precastedvalue) \
                if (type(precastedvalue).__name__ != vartype) \
                else 'str'
            return Storage(
                {
                    'prefixname': prefixname,
                    'varname': varname,
                    'vartype': vartype,
                    'varvalue': varvalue
                }
            )

    def init_vars(self, **kw_vars):
        '''     init_vars()     --> initialize a varsObject reference

                this is a good time to load the varspickle
                and update any target sqVars with this content

                a few notes:
                varsdata[self.varsname].sqVars contains the
                caller's full inventory to static sqVars
        '''
        self.varspickle = self.load_varspickle()

        for (prefixname, _value) in kw_vars.items():
            (
                varname,
                stored_value,
                stored_type
            ) = \
                (
                    Lst(prefixname.split('_', 1))(1),
                    None,
                    None
                )
            if (noneempty(self.varspickle[varname]) is False):
                (
                    stored_value,
                    stored_type
                ) = \
                    (
                        self.varspickle[varname].varvalue,
                        self.varspickle[varname].vartype
                    )
            value = stored_value or _value
            vartype = stored_type \
                if (
                    (noneempty(stored_type) is False) \
                    & (type(value) is not Storage)
            ) \
                else type(value)
            varvalue = casttype(vartype, value)
            self.varsobject.merge(
                {
                    varname: {
                        'prefixname': prefixname,
                        'varvalue': varvalue,
                        'vartype': vartype
                    }
                }
            )
            '''     any var with a value of None, must be rejected!

                    value is None   --> remove the var
                    not None        --> add/update the var
            '''
        self.merge_varspickle()
        return self.varsobject

    def merge_varspickle(self):
        pickledkeys = self.varspickle.getkeys()
        objectkeys = self.varsobject.getkeys()
        '''     the diff is the set of keys in pickledkeys,
                not in objectkeys... if any, insert them into
                objectkeys
        '''
        diff = pickledkeys.diff(objectkeys)
        if (noneempty(diff) is False):
            for dkey in diff:
                if (noneempty(self.varspickle[dkey]) is False):
                    self.varsobject.merge(
                        {
                            dkey: {
                                    'vartype': self.varspickle[dkey].vartype,
                                    'varvalue': self.varspickle[dkey].varvalue,
                                    'prefixname': self.varspickle[dkey].prefixname
                                    }
                            }
                    )

    def update_varspickle(self, **kwargs):
        kwargs = objectify(kwargs)
        '''  update varspickle as:

                    {_name:{'varvalue':_value,
                           ,'vartype':_type,
                           ,'prefixname':_othername}}
        '''
        for (key, value) in kwargs.items():
            if (
                    (value is None) \
                    and (key in self.varspickle)
            ):
                self.varspickle.__delitem__(key)
            else:
                self.varspickle.merge({key: value})

    def load_varspickle(self):
        p = self.varsdata[self.varsname].path
        if (os.path.exists(p)):
            try:
                return loadpickle(p) or Storage()
            except Exception as err:
                return load(p) or Storage()
        self.dump_varspickle()
        return Storage()

    def dump_varspickle(self):
        if (noneempty(self.varsobject) is False):
            dumppickle(self.varsobject,
                       self.varsdata[self.varsname].path)

class clsVars:
    def __init__(self, clsObj, varsname=None):
        (
            self.clsObj,
            self.varsname
        ) = \
            (
                clsObj,
                varsname
            )
        self.varsdata = self.clsObj.varsdata

    def __call__(self, *args, **kwargs):
        record = self.varsdata[self.varsname].objvars(*args, **kwargs)
        if (isinstance(record, dict)):
            prefix = self.varsdata[self.varsname].prefix
            Vars = self.varsdata[self.varsname].Vars
            for varname in record:
                configvalue = record[varname]
                if (isinstance(configvalue, Storage) is True):#if (hasattr(configvalue, 'action')):
                    if (configvalue.action == 'set'):
                        self.clsObj.clsVars(
                            self.clsObj,
                            self.varsname).set(
                                        varname,
                                        configvalue.varvalue,
                                        configvalue.vartype,
                                        prefix
                        )
                        #self.set(
                        #                varname,
                        #                configvalue.varvalue,
                        #                configvalue.vartype,
                        #                prefix
                        #)
                        Vars.merge(
                            {
                                varname: {
                                    'varvalue': configvalue.varvalue,
                                    'vartype': configvalue.vartype,
                                    'prefixname': configvalue.prefixname
                                }
                            }
                        )
                    elif (configvalue.action == 'unset'):
                        self.clsObj.clsVars(self, self.varsname).unset(varname, prefix)
                        #self.unset(varname, prefix)
                        [Vars.delete(name) for name in (varname, \
                            configvalue.prefixname) if (name in Vars)]
        return record

    def getprefix(self, name, prefix=None):
        return prefix \
            if (
                (noneempty(prefix) is False) \
               & (noneempty(self.varsname) is False)
        ) \
            else Lst(self.varsname.split('_', 1))(0) \
            if (
                (noneempty(prefix) is True) \
               & (noneempty(self.varsname) is False)
        ) \
            else None

    def unset(self, name, prefix=None):
        varsprefix = self.getprefix(name, prefix)
        prefixname = '{}_{}'.format(varsprefix, name)
        names = Lst([name, ]) \
            if (noneempty(varsprefix) is True) \
            else Lst(
            [
                name,
                prefixname
            ]
        )
        objContainers = [self.clsObj, globals()]
        if (
                (hasattr(self, 'varsname') is True) \
                & (noneempty(getattr(self, 'varsname')) is False)
        ):
            objContainers.append(self.varsdata[self.varsname].sqVars)
        if (hasattr(self.clsObj, 'bs')):
            objContainers.append(self.clsObj.bs.locals)
        for varname in names:
            for _obj in objContainers:
                if (isinstance(_obj, dict)):
                    if (varname in _obj):
                        _obj.__delitem__(varname)
                elif (hasattr(_obj, varname)):
                    delattr(_obj, varname)

    def set(self, *args, **kwargs):
        args = Lst(args)

        def setvars(varname, varvalue, _vartype=None, _prefix=None):
            '''
                        >>> self.myvar  --> this works, but is annoying
                        'blablabla'

                        >>> myvar       --> this also works & is less annoying
                        'blablabla
            '''
            if (noneempty(varname) is False) & (varvalue is not None):
                prefixname = f'{_prefix}_{varname}'
                vartype = _vartype \
                    if (noneempty(_vartype) is False) \
                    else type(varvalue)
                setattr(self.clsObj, varname, varvalue)
                globals().__setitem__(varname, varvalue)
                if (
                        (noneempty(prefixname) is False)
                        & (noneempty(self.varsname) is False)
                ):
                    setattr(self.clsObj, prefixname, varvalue)
                    globals().__setitem__(prefixname, varvalue)
                    self.varsdata[self.varsname].sqVars.merge(
                        {
                            varname: {
                                'varvalue': varvalue,
                                'vartype': vartype,
                                'prefixname': prefixname
                            }
                        }
                    )

        def get_varname(_name):
            if (noneempty(_name) is False):
                return _name \
                    if (isinstance(_name, str)) \
                    else str(_name)

        def get_varvalue(_value, _type=None):
            if (_value is not None):
                return casttype(_type, _value)

        if (noneempty(args) is False):
            setvars(
                get_varname(args(0)),
                get_varvalue(
                    args(1),
                    args(2)
                ),
                args(2),
                args(3)
            )
        elif (noneempty(kwargs) is False):
            [setvars(get_varname(key), get_varvalue(kwargs[key])) for key in kwargs]