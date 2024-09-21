import os, sys
import logging

from libdlg.dlgStore import Storage
from libdlg.dlgUtilities import bail

'''  [$File: //dev/p4dlg/libdlg/dlgLogger.py $] [$Change: 452 $] [$Revision: #4 $]
     [$DateTime: 2024/07/30 12:39:25 $]
     [$Author: mart $]
'''

__all__ = ['LogHandler']

def make_or_remove(logfile, logdir):
    try:
        os.makedirs(logdir)
    except: pass
    try:
        os.remove(logfile)
    except:pass

class LogHandler(object):
    '''
    %(asctime)s
    %(name)s
    %(levelname)
    %(pathname)s
    %(message)s
    %(filename)s
    %(module)s
    %(funcName)s
    %(lineno)d
    '''
    def __init__(
            self,
            loggername,
            loglevel=None,
            logfile=None,
            logger=None
    ):
        level={
           'DEBUG': logging.DEBUG,
           'INFO': logging.INFO,
           'WARNING': logging.WARNING,
           'ERROR': logging.ERROR,
           'CRITICAL': logging.CRITICAL
        }
        self.loglevel = loglevel or 'INFO'
        self.logger = logger or logging.getLogger(loggername)
        self.closed = False
        self.logger.setLevel(level[self.loglevel])
        self.formatter = logging.Formatter(
            fmt='\
%(asctime)s - \
%(name)s - \
%(levelname)s - \
%(module)s - \
%(filename)s - \
%(funcName)s\t- \
%(lineno)d - \
%(message)s',
            datefmt='%m-%d %H:%M:%S'
        )
        self.handlers = []

        if (logfile is not None):
            logfile = os.path.abspath(logfile)
            logdir = os.path.dirname(logfile)
            make_or_remove(logfile, logdir)
        self.logfile = logfile

    def get_filehandler(
            self,
            loglevel=logging.DEBUG,
            logfile=None,
            formatter=None,
            handlername=None
    ):
        logfile = logfile or self.logfile
        if (logfile is None):
            bail('A FileHandler requires a path to a logfile', True)
        log_formatter = formatter or self.formatter
        filehandler = logging.FileHandler(logfile)
        filehandler.setLevel(loglevel)
        filehandler.setFormatter(log_formatter)
        return filehandler

    def get_streamhandler(
            self,
            loglevel=logging.DEBUG,
            formatter=None
    ):
        log_formatter = formatter or self.formatter
        streamhandler = logging.StreamHandler(stream=sys.stdout)
        streamhandler.setLevel(loglevel)
        streamhandler.setFormatter(log_formatter)
        return streamhandler

    def get_nullhandler(
            self,
            loglevel=logging.DEBUG,
            formatter=None
    ):
        log_formatter = formatter or self.formatter
        nullhandler = logging.NullHandler()
        nullhandler.setLevel(loglevel)
        nullhandler.setFormatter(log_formatter)
        return nullhandler

    def get_dbhandler(self, *args, **kwargs):
        '''  NOT YET IMPLEMENTED
        '''

    def add_handlers(self, *handlers):
        if (self.logger.hasHandlers() is False):
            if (len(handlers) == 0):
                handlers = ['filehandler', 'streamhandler']
            ''' map handler name to logging handler
            '''
            kwhandler = Storage({'filehandler': self.get_filehandler,
                                 'streamhandler': self.get_streamhandler,
                                 'nullhandler': self.get_nullhandler,
                                 'dbhandler': self.get_dbhandler})
            '''  default handlers - filehandler & streamhandler
            '''
            [self.logger.addHandler(kwhandler[handler]()) for handler in handlers]
            [self.handlers.append(handler) for handler in handlers]
        return self


    def __call__(self, loggername, logfile=None, loglevel=None):
        #return LogHandler(loggername, logfile=self.logfile, loglevel=loglevel)
        return self

    def LOGINFO(self, msg):
        #if (self.loglevel == 'DEBUG'):
        self.logger.info(msg)

    def LOGWARNING(self, msg):
        #if (self.loglevel == 'DEBUG'):
        self.logger.warning(msg)

    def LOGERROR(self, msg):
        #if (self.loglevel == 'DEBUG'):
        self.logger.error(msg)

    def LOGCRITICAL(self, msg):
        #if (self.loglevel == 'DEBUG'):
        self.logger.critical(msg)


'''
def getLogger(*args):
    args = Lst(args)
    name = Lst(__name__.split('.'))(-1)
    oLogger = args(0) \
        if (type(args(0)) is LogHandler) \
        else LogHandler(name).add_handlers('streamhandler')
    logger = oLogger(name).add_handlers(*oLogger.handlers).logger \
        if (oLogger is not None) \
        else LogHandler(name).add_handlers('streamhandler').logger
    return logger

if (__name__ == '__main__'):
    oLogger = getLogger()
'''