from libdlg import *
from libdlg.dlgError import *

__all__ = ['invert']

def invert(qry, inversion=False):
    ''' do we need to invert?
    '''
    (
        op,
        left,
        right,
        #inversion
    ) \
        = (
        qry.op,
        qry.left,
        qry.right,
        #qry.inversion or False
    )
    try:
        qry.inversion = inversion
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
                #op = left.op = NOT
                if (hasattr(left, 'left')):
                    if (left.left is not None):
                        left.left = invert(left.left, inversion=left.inversion)
                if (hasattr(left, 'right')):
                    if (left.right is not None):
                        if is_qType_or_field(left.right):
                            left.right = invert(left.right, inversion=left.inversion)
                qry = left
            else:
                for lr in (left, right):
                    if (is_qType_or_field(lr) is True):
                        qry = invert(lr, inversion=inversion)
                        ''' Crap! what did I want to do next...?
                        '''
        return qry
    except Exception as err:
        raiseException(InvertError, err)