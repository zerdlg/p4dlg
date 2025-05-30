'''  [$File: //dev/p4dlg/libdlg/__init__.py $] [$Change: 609 $] [$Revision: #7 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
'''

''' libdlg for quick access
'''
from libdlg.dlgDateTime import *
from libdlg.dlgControl import *
from libdlg.dlgStore import *
from libdlg.dlgLogger import *
from libdlg.dlgSearch import *
from libdlg.dlgExtract import *
from libdlg.dlgOptions import *
from libdlg.dlgTables import *
from libdlg.dlgUtilities import *

''' contrib
'''
from libdlg.contrib import diff_match_patch, six
from libdlg.contrib.prettytable.prettytable import PrettyTable
from libdlg.contrib.pydal.pydal import DAL
from libdlg.contrib.pydal.pydal.objects import (
    Table,
    Query,
    Field,
    Rows,
    Row
)
