import re

from libdlg import *

class Help(object):
    def __init__(self, objp4):
        self.objp4 = objp4
        [
            setattr(
                self,
                f'log{logitem}',
                getattr(
                    self.objp4.logger,
                    f'log{logitem}'
                )
            ) for logitem in (
            'info',
            'warning',
            'error',
            'critical'
        )
        ]

    def helpusage(self):
        return self.objp4.help('--explain')

    def usagelist(self):
        help = self.objp4.help()
        if (
                (type(help).__name__ == 'P4QRecords')
                | (isinstance(help, list) is True)
        ):
            help = help(0).data
        try:
            return Lst(
                re.sub(
                    'p4 help ', '', hitem.strip()
                ).split()[0] for hitem in help.splitlines() \
                    if (hitem.startswith('\t'))
                       )
        except Exception as err:
            bail(err)

    def commandslist(self):
        help = self.objp4.help('commands')
        if (
                (type(help).__name__ == 'P4QRecords')
                | (isinstance(help, list) is True)
        ):
            help = help(0).data
        try:
            return Lst(
                hitem.strip('\t').split(' ')[0] for hitem in help.splitlines() \
                    if (hitem.startswith('\t'))
            )
        except Exception as err:
            bail(err)

    def administrationlist(self):
        help = self.objp4.help('administration')
        if (
                (type(help).__name__ == 'P4QRecords')
                | (isinstance(help, list) is True)
        ):
            help = help(0).data
        try:
            return Lst(
                hitem.strip('\t').split(' ')[0] for hitem in help.splitlines() \
                    if (hitem.startswith('\t'))
            )
        except Exception as err:
            bail(err)

    def get_allcmds(self):
        administrationlist = self.administrationlist()
        commandslist = self.commandslist()
        return Lst(
                set(
                       administrationlist \
                       + commandslist \
                       + ['export', 'configure']
                )
        )

    def parseundoc(self):
        undoc = self.objp4.help('undoc')
        if (
                (type(undoc).__name__ == 'P4QRecords')
                | (isinstance(undoc, list) is True)
        ):
            undoc = undoc(0)

        paragraphes = Lst(
            Lst(
                re.split(
                    '\n', paragraph)
            ).clean() for paragraph in re.split('\n\n', undoc)
        ).clean()
        undoc_cmds = set()
        for x in paragraphes:
            cmdline = x(0).lstrip()
            if (re.match(r'^p4\s', cmdline) is not None):
                cmdname = Lst(
                    re.split('\s', cmdline)[1:]
                )(0)
                if (re.match(r'^-', cmdname) is None):
                    if (re.search(r'/', cmdname) is not None):
                        for item in re.split('/', cmdname):
                            undoc_cmds.add(item)
                    else:
                        undoc_cmds.add(cmdname)
        return Lst(undoc_cmds)

class HelpConnect(object):
    def __init__(self, *args, **kwargs):
        pass