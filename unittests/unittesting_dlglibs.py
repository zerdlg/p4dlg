import unittest
from libdlg import (
    DLGDateTime,
)


class TestDLG(unittest.TestCase):
    def testDateTime(self):
        ''' /libdlg/dlgDateTime/DLGDateTime
        '''
        oDateTime = DLGDateTime()                                                   # DLGDateTime instance

        ''' str to datetime / guess
        '''
        guessdate = oDateTime.guess('2019/07/26')                                   # guess date
        guesstime = oDateTime.guess('16:30:45')                                     # guess time
        guessdatetime = oDateTime.guess('2019/07/26 16:30:45')                      # guess datetime
        print('\n')
        print(f'DLGDateTime - guess_date:       {guessdate}')
        print(f'DLGDateTime - guess_time:       {guesstime}')
        print(f'DLGDateTime - guess_datetime:   {guessdatetime}')
        print('\n')

        ''' string_to_datetime from ...
        '''
        oDateTime.string_to_datetime('2019/08/29')                                  # given a date with a / separator
        oDateTime.string_to_datetime('2019 08 29')                                  # given a date with a ' ' separator
        oDateTime.string_to_datetime('2019-08-29')                                  # given a date with a / separator
        res = oDateTime.string_to_datetime('2019:08:29')                            # given a date with a : separator
        print(f'DLGDateTime - string_to_datetime: {res}')

        ''' to_datetime from ...
        '''
        oDateTime.to_datetime("1547856000.0")
        oDateTime.to_datetime(1547856000.0)
        oDateTime.to_datetime(2019, 8, 19)
        oDateTime.to_datetime(*[2019, 8, 19])
        oDateTime.to_datetime("2019/8/19")
        oDateTime.to_datetime("2019-8-19")
        res = oDateTime.to_datetime(*[2019, 6, 9])
        print(f'DLGDateTime - to_datetime: {res}')

        ''' epoch
        '''
        oDateTime.to_epoch('2019/09/03')                                            # to epoch from str
        d = oDateTime.guess('2019/09/03')                                           # create date
        oDateTime.to_epoch(d)                                                       # to epoch from date
        oDateTime.to_epoch(2019, 9, 3)                                              # args[0], args[1], args[2]
        oDateTime.to_epoch([2019, 9, 3])                                            # [args[0], args[1], args[2]]
        res = oDateTime.to_epoch(*[2019, 9, 3])                                     # *[args[0], args[1], args[2]]
        print(f'DLGDateTime - to_epoch: {res}')

        ''' internally, perforce uses unix time
            externally, to the user, it uses this 
            format '2019/8/1' (the 'p4date')
        '''
        oDateTime.to_p4date(2019, 8, 19)
        oDateTime.to_p4date('2019', '8', '19')
        oDateTime.to_p4date('2019,8,19')
        oDateTime.to_p4date('2019-8-19')
        oDateTime.to_p4date('2019 8 19')
        oDateTime.to_p4date('2019:8:19')
        oDateTime.to_p4date('2019:8,19')
        oDateTime.to_p4date(2019, 8, 19)
        oDateTime.to_p4date(*[2019, 8, 19])
        oDateTime.to_p4date('1547856000.0')
        res = oDateTime.to_p4date(1547856000.0)
        print(f'DLGDateTime - to_p4date: {res}')

if (__name__ == '__main__'):
    (
    loader,
    suite
    ) = \
    (
    unittest.TestLoader(),
    unittest.TestSuite()
    )
    for item in (
            loader.loadTestsFromTestCase(TestDLG)
    ):
        suite.addTests(item)