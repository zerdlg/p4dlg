#
"""
        --[$File: //dev/p4dlg/libsh/__init__.py $]
        --[$Change: 411 $] 
        --[$Revision: #4 $]
        --[$DateTime: 2024/06/25 07:02:28 $]
        --[$Author: mart $]
"""
''' absolute path to schemaxml directory
'''
import os
from os.path import dirname
import schemaxml
from resc import journals, db

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
varsdata = {
             'configvars': {'prefix': 'var', 'Vars': {}, 'objvars': None},
             'jnlvars':    {'prefix': 'jnl', 'Vars': {}, 'objvars': None},
             'p4vars':     {'prefix': 'p4', 'Vars': {}, 'objvars': None},
             'p4dbvars':   {'prefix': 'p4db', 'Vars': {}, 'objvars': None},
             'qtvars':     {'prefix': 'qt', 'Vars': {}, 'objvars': None},
             'dbvars':     {'prefix': 'db', 'Vars': {}, 'objvars': None},
             'novars':     {'prefix': 'no', 'Vars': {}, 'objvars': None}
             }