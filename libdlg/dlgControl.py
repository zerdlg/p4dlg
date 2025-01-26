import datetime
import os, sys
import logging

from libdlg.dlgUtilities import bail
from libdlg.dlgStore import Lst, Storage
from libdlg.dlgLogger import LogHandler

'''  [$File: //dev/p4dlg/libdlg/dlgControl.py $] [$Change: 452 $] [$Revision: #4 $]
     [$DateTime: 2024/07/30 12:39:25 $]
     [$Author: mart $]
'''

__all__ = ['DLGControl']

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
            bail(
                'A FileHandler requires a path to a logfile'
            )
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
        return self

class DLGControl(object):
    #__shared__ = Storage()
    def __init__(
            self,
            loggername='P4DLG',
            loglevel='INFO',
            logfile=None,
            *args,
            **kwargs
    ):
        #self.__dict__ = self.__shared__
        (args, kwargs) = (Lst(args), Storage(kwargs))
        if (logfile is None):
            dateitems = datetime.datetime.now().ctime().split()
            dateitems.pop(3)
            logfilename = '_'.join(dateitems)
            logfile = os.path.abspath(f'../logs/p4log/py4_{logfilename}')
        self.handler = 'streamhandler'
        self.oLogger = LogHandler(
            loggername,
            loglevel=loglevel,
            logfile=logfile
        )
        self.loglevel = loglevel
        self.oLogger.add_handlers('streamhandler')
        self.logger = self.oLogger.logger
        ''' log levels
        '''

        self.loginfo = self.logger.info \
            if (self.loglevel in ('DEBUG', 'INFO')) \
            else self.logcollector
        self.logwarning = self.logger.warning \
            if (self.loglevel in ('DEBUG', 'WARNING')) \
            else self.logcollector
        self.logerror = self.logger.error \
            if (self.loglevel in ('DEBUG', 'ERROR')) \
            else self.logcollector
        self.logcritical = self.logger.critical \
            if (self.loglevel in ('DEBUG', 'CRITICAL')) \
            else self.logcollector

    def logcollector(self, msg):
        pass

    def __call__(self, *args, **kwargs):
        return self
