from libdlg.dlgStore import ZDict, Lst
from libdlg.dlgControl import DLGControl

'''  [$File: //dev/p4dlg/libpy4/py4Filetypes.py $] [$Change: 609 $] [$Revision: #5 $]
     [$DateTime: 2025/02/21 03:36:09 $]
     [$Author: zerdlg $]
'''

__all__ = ['Py4Filetypes']

''' TODO:
'''

class Py4Filetypes(DLGControl):
    def __init__(self, objp4, *args, **kwargs):
        self.objp4 = objp4
        if (self.objp4 is None):
            self.objp4 = ZDict()
        loglevel = kwargs.loglevel \
            if (self.objp4 is None) \
            else self.objp4.loglevel or 'DEBUG'
        loggername = Lst(__name__.split('.'))(-1)
        super(Py4Filetypes, self).__init__(
            loggername=loggername,
            loglevel=loglevel
        )
        """
        Perforce file type determines how the file is handled on both the
        client and the server.

        A filetype can be specified as 'type', with modifiers as 'type+mods'
        or as just the modifiers '+mods'.

        The following are the base Perforce filetypes:

            Type        Client Use              Server ZDict
            ----        ----------              --------------
            text        newlines translated     deltas in RCS format
            binary      raw bytes               compressed binary
            symlink     symbolic link           deltas in RCS format
            apple       Mac resource + data     compressed AppleSingle
            resource    Mac resource fork       compressed binary
            unicode     newlines translated     deltas in RCS format
                                                stored as UTF-8
            utf16	newlines translated     deltas in RCS format
                        client file UTF-16      stored as UTF-8

                        Files of type utf16 are stored in the depot in UTF-8.
                        These files are in utf16 in the client workspace.
                        The automatic type detection requires a BOM be present
                        at the start of the file.  Files without a BOM are
                        assumed to be in client byte order.  When utf16 files
                        are written to a client, they are written with a BOM
                        in client byte order.

        The following are the modifiers:

            Modifier    Meaning
            --------    -------
            +m		always set modtime on client
                (overrides client's nomodtime)
            +w          always writable on client
            +x          exec bit set on client

            +k		$Keyword$ expansion of Id, Header, Author
                Date, DateUTC, DateTime, DateTimeUTC, DateTimeTZ
                Change, File, Revision
            +ko         $Keyword$ expansion of ID, Header only
            +l		exclusive open: disallow multiple opens

            +C          server stores compressed file per revision
            +D          server stores deltas in RCS format
            +F          server stores full file per revision
            +S          server stores only single head revision
            +S<n>       server stores <n> number of revisions, where <n>
                        is a number 1-10 or 16,32,64,128,256,512.
            +X		server runs archive trigger to access files

        The following aliases for filetypes are supported for backwards
        compatibility:

            Type        Is Base Type         Plus Modifiers
            --------    ------------         --------------
            ctempobj    binary               +Sw
            ctext       text                 +C
            cxtext      text                 +Cx
            ktext       text                 +k
            kxtext      text                 +kx
            ltext       text                 +F
            tempobj     binary               +FSw
            ubinary     binary               +F
            uresource   resource             +F
            uxbinary    binary               +Fx
            xbinary     binary               +x
            xltext      text                 +Fx
            xtempobj    binary               +Swx
            xtext       text                 +x
            xunicode    unicode              +x
            xutf16	utf16                +x

        'p4 add', 'p4 edit', and 'p4 reopen' accept the '-t filetype'
        flag to specify the filetype.  If you omit the -t flag, 'p4 add'
        determines filetype using its own logic and the name-to-type
        mapping table managed by 'p4 typemap', if configured.
        'p4 edit -t auto' will determine the filetype by the above mentioned
        logic.

        By default, 'p4 edit' and 'p4 reopen' reuse the current file
        filetype, and 'p4 add' determines the filetype by examining the
        file's contents and its execute permission bits.

        If a filetype is specified using only '+mods, then that filetype
        is combined with the default.  Most modifiers are simply added to
        the default type, but the +C, +D, and +F storage modifiers replace
        the storage method.  To remove a previously assigned modifier, the
        whole filetype must be specified.
        """
