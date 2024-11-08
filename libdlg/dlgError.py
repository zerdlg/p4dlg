'''  [$File: //dev/p4dlg/librcs/__init__.py $] [$Change: 411 $] [$Revision: #2 $]
     [$DateTime: 2024/06/25 07:02:28 $]
     [$Author: mart $]
'''

__all__ = [
    'DLGError',
    'raiseException',
    'AttributeNotBelongToFieldError',
    'FieldNotBelongToTableError',
    'TableNotExistError',
    'depotFileNotExistError',
    'clientFileNotEsxistError',
    'MutuallyExclusiveError',
    'InternalRCSError',
    'CreateRevisionError',
    'InvalidEditScript',
    'MissingRevision',
    'LockingError',
    'InvertError',
    'RecordFieldsNotMatchCols',
    'FieldNotInRecord',
    'NoSuchTableError',
    'NoSuchFieldError'
]
def raiseException(exception, msg):
    return exception(msg)

class DLGError_(Exception):
    def __init__(self, msg=None):
        self.message = msg
        super(DLGError, self).__init__(msg)

class DLGError(Exception): pass

class LockingError(DLGError): pass

class InvertError(DLGError): pass

class ExtractError(DLGError): pass

class FieldNotInRecord(DLGError):
    def __init__(self, field, fields):
        DLGError.__init__(self)
        self.field = field
        self.fields = fields

    def __str__(self):
        return f"field `{self.field}` not in record fields {self.fields}"

class RecordFieldsNotMatchCols(DLGError):
    def __init__(self, fields, cols):
        DLGError.__init__(self)
        self.fieldslength = len(fields)
        self.colslength = len(cols)

    def __str__(self):
        return f'Len `cols` ({self.colslength}) does not match len of record fields ({self.fieldslength}) .'

class AttributeNotBelongToFieldError(DLGError):
    def __init__(self, fieldname, att):
        DLGError.__init__(self)
        self.fieldname = fieldname
        self.att = att

    def __str__(self):
        return f'Attribute `{self.att}` does not belong to field `{self.fieldname}`.'

class FieldNotBelongToTableError(DLGError):
    def __init__(self, tablename, fieldname):
        DLGError.__init__(self)
        self.tablename = tablename
        self.fieldname = fieldname

    def __str__(self):
        return f'Field `{self.fieldname}` does not belong to table `{self.tablename}`.'

class TableNotExistError(DLGError):
    def __init__(self, tablename):
        DLGError.__init__(self)
        self.tablename = tablename

    def __str__(self):
        return f'Table `{self.tablename}` does not exist.'

class depotFileNotExistError(DLGError):
    def __init__(self, depotFile):
        DLGError.__init__(self)
        self.depotFile = depotFile

    def __str__(self):
        return f'depotFile `{self.depotFile}` does not exist.'

class clientFileNotEsxistError(DLGError):
    def __init__(self, clientFile):
        DLGError.__init__(self)
        self.clientFile = clientFile

    def __str__(self):
        return f'clientFile `{self.clientFile}` does not exist.'

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

class NoSuchTableError(DLGError):
    def __init__(self, tablename):
        DLGError.__init__(self)
        self.tablename = tablename

    def __str__(self):
        return f"No such table `{self.tablename}`"

class NoSuchFieldError(DLGError):
    def __init__(self, tablename, fieldname):
        DLGError.__init__(self)
        self.fieldname = fieldname
        self.tablename = tablename

    def __str__(self):
        return f"No such field (`{self.fieldname}`)  in table `{self.tablename}`"


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