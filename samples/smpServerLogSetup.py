from libdlg.dlgUtilities import noneempty

'''  [$File: //dev/p4dlg/sample/smpServerLogSetup.py $] [$Change: 461 $] [$Revision: #4 $]
     [$DateTime: 2024/08/09 18:15:55 $]
     [$Author: zerdlg $]
'''

''' Server logs:
            
        There are different formats that can be used with perforce logging simply by
        applying any of the following file extensions to valid log file names:
        +---------+-----------------------------------+
        | '.csv'  | csv formatted logs                |
        +---------+-----------------------------------+
        | '.jnl'  | jnl formatted logs (@@ quotes)    |
        +---------+-----------------------------------+
        | '.json' | json formatted logs               |
        +---------+-----------------------------------+
        | '.txt'  | text output logs                  |
        +---------+-----------------------------------+
        | '.xml'  | xml formatted logs                |
        +---------+-----------------------------------+
    
        Server supported log file names (note the use of the `.jnl` extensions):
        +---------------+--------------------------------------------------+
        | all.jnl       | All loggable events (commands, errors, audit,    |
        |               | triggers, and more)                              |
        +---------------+--------------------------------------------------+ 
        | audit.jnl     | Audit events (audit, purge)                      |
        +---------------+--------------------------------------------------+
        | auth.jnl      | The results of p4 login attempts. If the login   |
        |               | failed, the reason for this is included in the   |
        |               | log. Additional information provided             |
        |               | by the authentication method is also included.   |
        +---------------+--------------------------------------------------+                                 
        | commands.jnl  | Command events (command start, compute, and end) |
        +---------------+--------------------------------------------------+
        | errors.jnl    | Error events (errors-failed, errors-fatal)       |
        +---------------+--------------------------------------------------+
        | events.jnl    | Server events (startup, shutdown, checkpoint,    |
        |               | journal rotation, etc.)                          |
        +---------------+--------------------------------------------------+
        | integrity.jnl | Major events that occur during replica integrity |
        |               | checking.                                        |
        +---------------+--------------------------------------------------+
        | ldapsync.jnl  | p4 ldapsync events, such as when:                |
        |               |    - a user is added, updated, or removed        |
        |               |    - a user is added or removed from a group     |
        +---------------+--------------------------------------------------+
        | route.jnl     | Log the full network route of authenticated      |
        |               | client connections.                              |
        |               | Errors related to net.mimcheck are also logged   |
        |               | against the related hop.                         |
        +---------------+--------------------------------------------------+
        | track.jnl     | Command tracking (track-usage, track-rpc,        |
        |               | track-db)                                        |
        +---------------+--------------------------------------------------+
        | triggers.jnl  | Trigger events.                                  |
        +---------------+--------------------------------------------------+
        | user.jnl      | User events: one record every time a user runs   |
        |               | p4 logappend.                                    |
        +---------------+--------------------------------------------------+
    
        USAGE:
        - setting up logging:
            >>> p4 configure set serverlog.file.1=audit.jnl
            >>> p4 configure set serverlog.file.2=auth.jnl
            >>> p4 configure set serverlog.file.3=commands.jnl
            >>> p4 configure set serverlog.file.4=errors.jnl
            etc.
    
        * Note: the order for log files doesn't matter but, the number associated
          to them do matter. The Type of logs (as well as their names) are referenced
          by that number.
    
        - set logging configurables to log files:
            I.e. - rotate log file when its max size is reached
            >>> p4 configure set servername#serverlog.maxmb.4=100
                                                            |
                                                      `.4` references `errors.jnl`, 
                                                      as specified above.
    
            I.e. - specify the number of logs you want to keep
            >>> p4 configure set servername#serverlog.retain.4=10
        
    USAGE:
    
        defaults:   type        -> 'jnl' (@journal quotes@)
                    logfiles    -> 'all'
                    maxlogsize  -> 100 
                    retain      -> 10 
    
        >>> oServerLog = Py4ServerLog(objp4, type='jnl')
        >>> oServerLog.setup(logfiles='all', maxlogsize=100, retain=10)
        '''

__all__ = ['Py4ServerLog']

class Py4ServerLog(object):
    def __init__(self, objp4, type='jnl'):
        self.objp4 = objp4
        self.types = [
            'csv',
            'jnl',
            'json',
            'txt',
            'xml'
        ]
        self.logtype = type \
            if (type in self.types) \
            else 'jnl'
        self.all_logfiles = [
                                'audit',
                                'auth',
                                'commands',
                                'errors',
                                'events',
                                'integrity',
                                'ldapsync',
                                'route',
                                'track',
                                'triggers',
                                'user'
                            ]

    def setup(self, logfiles=None, maxlogsize=100, retain=10):
        if (
                (noneempty(logfiles) is True) |
                (logfiles == 'all')
        ):
            logfiles = self.all_logfiles
        N = 1
        for log in logfiles:
            try:
                logname = f'serverlog.file.{N}={log}.{self.logtype}'
                maxsize = f'servername#serverlog.maxmb.{N}={maxlogsize}'
                logretain = f'servername#serverlog.retain.{N}={retain}'
                for config in (
                                logname,
                                maxsize,
                                logretain
                ):
                    self.objp4.configure('set', config)

                for logitem in (
                                logname,
                                maxsize,
                                logretain
                ):
                    print(f'logname: {log} configurable: {logitem}')
            except Exception as err:
                print(err)
            N += 1
        return self