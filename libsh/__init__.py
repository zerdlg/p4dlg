"""
        --[$File: //dev/p4dlg/libsh/__init__.py $]
        --[$Change: 693 $] 
        --[$Revision: #7 $]
        --[$DateTime: 2025/04/22 07:22:55 $]
        --[$Author: mart $]
"""
''' absolute path to schemaxml directory
'''
import os
from os.path import dirname
import schemaxml
from resc import journals, db
from libsh.shVars import *
from libdlg.dlgStore import objectify

default_xmlschema_version = 'r15.2'
schemadir = dirname(schemaxml.__file__)
journaldir = dirname(journals.__file__)
projectdir = dirname(schemadir)
dbdir = dirname(db.__file__)
varsdir = os.path.join(
    *[
        projectdir,
        'libsh',
        'Vars'
    ]
)
varsdata = objectify({
            'configvars':    {'prefix': 'var', 'Vars': {}, 'objvars': None},
            'jnlvars':       {'prefix': 'jnl', 'Vars': {}, 'objvars': None},
            'p4vars':        {'prefix': 'p4', 'Vars': {}, 'objvars': None},
            'p4dbvars':      {'prefix': 'p4db', 'Vars': {}, 'objvars': None},
            'qtvars':        {'prefix': 'qt', 'Vars': {}, 'objvars': None},
            'dbvars':        {'prefix': 'db', 'Vars': {}, 'objvars': None},
            'novars':        {'prefix': 'no', 'Vars': {}, 'objvars': None},
            'p4recordvars':  {'prefix': 'p4rec', 'Vars': {}, 'objvars': None},
            'jnlrecordvars': {'prefix': 'jnlrec', 'Vars': {}, 'objvars': None},
        })