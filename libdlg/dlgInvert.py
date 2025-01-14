from libdlg.dlgError import *
from libdlg.dlgQuery_and_operators import *

__all__ = ['invert']

from libdlg.dlgQuery_and_operators import is_qType_or_field, is_fieldType, is_tableType


def invert(qry, inversion=False):
    ''' do we need to invert?
    '''
    if (is_tableType(qry) is True):
        op = None
        right = None
    else:
        (
            op,
            left,
            right,
            inversion
        ) \
            = (
            qry.op,
            qry.left,
            qry.right,
            qry.inversion or False
        )
    try:
        if (op is not None):
            opname = op.__name__ \
                if (callable(op) is True) \
                else op
            if (opname in (andops + orops + xorops)):
                for lr in (left, right):
                    if (is_qType_or_field(lr) is True):
                        lr = invert(lr, inversion=inversion)
                        if AND(
                                (is_fieldType(lr) is True),
                                (lr.op is None)
                        ):
                            lr.op = op
            elif (opname in notops):
                inversion = left.inversion = True
                if (hasattr(left, 'left')):
                    if (left.left is not None):
                        left.left = invert(left.left, inversion=left.inversion)
                if (hasattr(left, 'right')):
                    if (left.right is not None):
                        if is_qType_or_field(left.right):
                            left.right = invert(left.right, inversion=left.inversion)
                qry = left
            elif AND(
                    (is_fieldType(left) is True),
                    (is_fieldType(right) is True)
            ):
                return invert(qry, inversion=inversion)
            else:
                for lr in (left, right):
                    if (is_qType_or_field(lr) is True):
                        lr = invert(lr, inversion=inversion)
                        ''' Crap! what did I want to do next...?
                        '''
        return qry
    except Exception as err:
        raiseException(InvertError, err)