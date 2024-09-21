'''  [$File: //dev/p4dlg/librcs/__init__.py $] [$Change: 411 $] [$Revision: #2 $]
     [$DateTime: 2024/06/25 07:02:28 $]
     [$Author: mart $]
'''

__all__ = [
    'P4QError',
    'MutuallyExclusiveError',
    'InternalRCSError',
    'CreateRevisionError',
    'InvalidEditScript',
    'MissingRevision'
]

class P4QError(Exception): pass
class MutuallyExclusiveError(P4QError):
    def __init__(self, *args):
        P4QError.__init__(self)
        (
            self.left,
            self.right,
        ) \
            = (
            args[0],
            args[1]
        )

    def __str__(self):
        return f'Values are mutually exclusive, they cannot both be True (or False). Got ({self.left}, {self.right}).'

''' RCS SPECIFIC ERRORS
'''
class InternalRCSError(P4QError): pass
class CreateRevisionError(P4QError): pass
class InvalidEditScript(P4QError): pass
class MissingRevision(P4QError):
    def __init__(self, RCSobject, revstring):
        P4QError.__init__(self)
        self.RCSobject = RCSobject
        self.revstring = revstring

    def __str__(self):
        return f'Revision {self.revstring} missing from deltatext section in {self.RCSobject.sourcefilename}.'