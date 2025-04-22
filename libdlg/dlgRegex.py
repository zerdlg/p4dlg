import re

class Regex(object):
    '''
            something for quick matches (return 'string', (True/False))

            >>> Regex('^.*[0-9]$')('blabla123')
            blabla123, True

            >>> Regex('^.*[0-9]$')('fred')
            fred, False
    '''
    def __init__(
            self,
            expr,
            strict=False,
            search=False,
            extract=False,
            groupidx=None
    ):
        if (not hasattr(expr, 'search')):
            if (
                    ((strict is True) | (search is True))
                    & (not expr.startswith('^'))
            ):
                expr = f'^{expr}'
            if (
                    ((strict is True) | (search is True))
                    & (not expr.endswith('$'))
            ):
                expr = f'{expr}$'
        self.regex = re.compile(expr) \
            if (isinstance(expr, str)) \
            else expr \
            if (hasattr(expr, 'search')) \
            else None
        (self.groupidx, self.extract) = (groupidx, extract)

    def __call__(self, value):
        if (
                (self.regex is not None)
                & (hasattr(self.regex, 'search'))
        ):
            match = self.regex.search(value)
            if (match is not None):
                match_group = match.group() \
                    if (self.groupidx is None) \
                    else match.group(self.groupidx)
                return (self.extract and match_group or value, True)
        return (value, False)

if (__name__ == '__main__'):
    x = Regex('[mM]art')('martin')
    print(x)
