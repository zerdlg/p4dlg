
from libdlg.dlgUtilities import bail
from libdlg.dlgStore import ZDict, Lst, objectify
from libdlg.dlgControl import DLGControl

class NOFile(DLGControl):
    def __init__(self, *args, **kwargs):
        (args, kwargs) = (Lst(args), objectify(kwargs))
        self.locked = True
        self.closed = False
        self.buffer = Lst()
        self.content = None
        loglevel = kwargs.loglevel or 'DEBUG'
        loggername = Lst(__name__.split('.'))(-1)
        super(NOFile, self).__init__(loggername, loglevel, )

    def __call__(self):
        return self

    def close(self):
        self.locked = False
        self.closed = True

    def getsize(self):
        return self.__len__()

    def __len__(self):
        return sum(Lst(len(self.buffer[idx]) for idx in self.buffer), len(self.content))

    def write(self, data=None):
        bdata = data or self.buffer

        def line(i):
            return item \
                if (item.endswith('\n')) \
                else f'{item}\n'

        if (isinstance(bdata, list)):
            for item in bdata:
                self.buffer.mergeright(line(item))
        elif (isinstance(bdata, str)):
            self.buffer.mergeright(line(bdata))
        elif (isinstance(data, ZDict)):
            for key in bdata.getkeys():
                self.buffer.mergeright(line(bdata[key]))
        else:
            bail(
                f"arg must be one of string, list, dict, but not {data}"
            )

    def flush(self):
        if (len(self.buffer) > 0):
            self.write()

    def seek(self, line=0):
        return self.buffer[line]

    def _seeklines(self, line=0):
        return self.buffer.slice(line, self.buffer.last().key(), 1)

    def read(self, size=None):
        if (size is None):
            if (size > len(self.content)):
                self.content = ''.join((self.content, ''.join(self.buffer)))
        if (size > len(self.content)):
            return ""
        result = self.content[:size]
        self.content = self.content[len(result):]
        return result