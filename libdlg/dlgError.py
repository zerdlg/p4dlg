'''  [$File: //dev/p4dlg/librcs/__init__.py $] [$Change: 411 $] [$Revision: #2 $]
     [$DateTime: 2024/06/25 07:02:28 $]
     [$Author: mart $]
'''

__all__ = [
    'DLGError',
    'raiseException',
    'MutuallyExclusiveError',
    'InternalRCSError',
    'CreateRevisionError',
    'InvalidEditScript',
    'MissingRevision',
    'LockingError',
    'InvertError',
]
def raiseException(exception, msg):
    return exception(msg)

class DLGError(Exception): pass

class LockingError(DLGError): pass

class InvertError(DLGError): pass

class ExtractError(DLGError): pass

class MutuallyExclusiveError(DLGError):
    def __init__(self, *args):
        DLGError.__init__(self)
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
class InternalRCSError(DLGError): pass
class CreateRevisionError(DLGError): pass
class InvalidEditScript(DLGError): pass
class MissingRevision(DLGError):
    def __init__(self, RCSobject, revstring):
        DLGError.__init__(self)
        self.RCSobject = RCSobject
        self.revstring = revstring

    def __str__(self):
        return f'Revision {self.revstring} missing from deltatext section in {self.RCSobject.sourcefilename}.'