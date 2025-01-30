## ``p4dlg``
*this readme is under construction*
#### P4dlg is a set of *abstractions* written in Python that lets you take full advantage of SQL features and functionality to access - & interact with - your Perforce server instances and their resources. 

``Note: Though I don't know how/if this runs on Windows, it does however work as expected on both MacOS and Linux. ``

### Abstractions:
+ P4Jnl - Interact with all of your metadata, without having to rely on a Perforce client.
+ Py4   - You can put away the p4 cmd line, P4Python & p4v. Try P4dlg to nteract with your Perforce instance!

### Intsallation:
+ drop p4dlg on your system somewhere.
+ requires IPython / qtconsole
+ I generally install Anaconda as my defualt distribution - if you don't and p4dlg complains about missing dependencies, please do let me know.

### Where do we use *p4dlg*?
``Import P4dlg and use it in script or broader programs or, use it interactively in an IPython QT shell (included in this package). It's OK, P4dlg is fully baked into it. More on this below``

### Create a connection to a Perforce Journal or checkpoint.
```Python
>>> from libjnl.jnlIO import jnlconnector

>>> jnlfile = 'Users/gc/anastasia/dev/p4dlg/resc/journals/checkpoint.14'
>>> version = 'r15.2' 
>>> jnl = jnlconnector(jnlfile, version=version)

>>> pprint(jnl.tables)
['config', 'counters', 'nameval', 'logger', 'ldap', 'server', 'svrview', 'remote', 'rmtview', 'stash', 'userrp', 'user', 'group', 'groupx', 'depot', 'stream', 'domain', 'template', 'templatesx', 'templatewx', 'viewrp', 'view', 'review', 'integed', 'integtx', 'resolve', 'resolvex', 'haverp', 'havept', 'have', 'label', 'locks', 'excl', 'archmap', 'rev', 'revtx', 'revcx', 'revdx', 'revhx', 'revpx', 'revsx', 'revsh', 'revbx', 'revux', 'working', 'workingx', 'traits', 'trigger', 'change', 'changex', 'changeidx', 'desc', 'job', 'fix', 'fixrev', 'bodresolve', 'bodtext', 'bodtextcx', 'bodtexthx', 'bodtextsx', 'bodtextwx', 'ixtext', 'ixtexthx', 'uxtext', 'protect', 'property', 'message', 'sendq', 'jnlack', 'monitor', 'rdblbr', 'tiny']

>>> jnl.rev
<libjnl.jnlSqltypes.JNLTable at 0x1161b92d0>

>>> jnl.rev.fieldnames
['idx', 'db_action', 'table_revision', 'table_name', 'depotFile', 'depotRev', 'type', 'action', 'change', 'date', 'modTime', 'digest', 'size', 'traitLot', 'lbrIsLazy', 'lbrFile', 'lbrRev', 'lbrType']

>>> jnl.rev.depotfile
<JNLField depotFile>

>>> query = (jnl.rev.depotFile.contains('test'))
>>> testfiles = jnl(query).select()
>>> testfiles
<DLGRecords (18013)>

>>> testfiles.first()
<DLGRecord {'action': '0',
 'change': '3',
 'date': '2021/03/12 03:00:42',
 'db_action': 'pv',
 'depotFile': '//depot/anyschema_2db/lib/sql/pymysql/tests/__init__.py',
 'depotRev': '1',
 'digest': '5BC0C5616DFAAE172F0495510B7BB41E',
 'idx': 115,
 'lbrFile': '//depot/anyschema_2db/lib/sql/pymysql/tests/__init__.py',
 'lbrIsLazy': '0',
 'lbrRev': '1.3',
 'lbrType': '0',
 'modTime': '2016/02/12 21:14:40',
 'size': '363',
 'table_name': 'db.rev',
 'table_revision': '9',
 'traitLot': '0',
 'type': '0'}>
```

### Create a connection to a Perforce Server instance.
```Python
>>> from libpy4.py4IO import p4connector
>>> p4globals = {'user': 'bigbird', 'port': 'anastasia.local:1777', 'client': 'bigbird_workspace'}
>>> p4 = p4connector(**p4globals)

>>> print(p4.tables)
['where', 'change', 'fixes', 'jobspec', 'have', 'status', 'renameuser', 'unshelve', 'delete', 'counter', 'clients', 'jobs', 'users', 'resolve', 'dbstat', 'key', 'protects', 'verify', 'streams', 'workspace', 'logtail', 'dbschema', 'rename', 'add', 'ldap', 'filelog', 'labels', 'stream', 'login', 'copy', 'client', 'archive', 'groups', 'sizes', 'user', 'flush', 'diff', 'integrate', 'sync', 'dbverify', 'changelists', 'attribute', 'zip', 'branches', 'help', 'populate', 'export', 'branch', 'logschema', 'edit', 'unzip', 'merge', 'typemap', 'tickets', 'clean', 'dirs', 'changelist', 'passwd', 'property', 'logparse', 'rec', 'obliterate', 'annotate', 'workspaces', 'admin', 'interchanges', 'unlock', 'unload', 'counters', 'list', 'depot', 'prune', 'review', 'journals', 'diff2', 'logger', 'changes', 'reopen', 'diskspace', 'opened', 'logappend', 'license', 'files', 'set', 'fstat', 'ldapsync', 'keys', 'logstat', 'print', 'lockstat', 'restore', 'tag', 'group', 'istat', 'submit', 'logrotate', 'describe', 'cachepurge', 'integrated', 'label', 'reviews', 'resolved', 'revert', 'depots', 'grep', 'logout', 'ping', 'protect', 'labelsync', 'info', 'triggers', 'ldaps', 'update', 'lock', 'reconcile', 'cstat', 'reload', 'job', 'fix', 'move', 'configure', 'shelve', 'monitor']

>>> p4.files
<libpy4.py4Sqltypes.Py4Table at 0x130398890>

>>> p4.files.fieldnames
['code', 'depotFile', 'rev', 'change', 'action', 'type', 'time']

>>> p4.files.depotfile
<Py4Field depotFile>

>>> query = (p4.files.depotfile.contains('test'))
>>> testfiles = p4(query).select()
>>> testfiles
<DLGRecords (8)>

>>> testfiles.last()
<DLGRecord {'action': 'edit',
 'change': '538',
 'depotFile': '//dev/p4dlg/unittests/unittesting_py4.py',
 'idx': 584,
 'rev': '3',
 'time': '2025/01/07 12:31:54',
 'type': 'text'}>
```

## As you can see, P4Jnl and Py4 share the same API. As for Py4, you just need to modify your mindset a little bit... Instead of commands (```%> p4 command arg1 arg2```), think of them as tables.

# SQL features (queries, aggregators, operators, expressions...)

## Queries (the where clause)

### Building a query.
```Python
>>> qry = (jnl.domain.type == 'client')          # A simple query.
>>> qry 
<DLGQuery {'inversion': False,                   # A query is a reference to class `DLGQuery`.
 'left': <JNLField type>,
 'objp4': <P4Jnl ./resc/journals/journal.8>,     
 'op': <function EQ at 0x104d32700>,
 'right': 'client'}>
```

### Connector + query = recordset
```Python
>>> my_recordset = jnl(qry)                    # Connection objects are callable, take queries and expose useful attributes such as `select`, `fetch`, update, etc.
>>> my_recordset
<DLGRecordSet (<class 'libjnl.jnlFile.JNLFile'>)>

>>> clients = recordset.select()               # equivalent to `SELECT * FROM domain WHERE type = client`
>>> clients
<DLGRecords (188)>                             # The target journal defines 188 `clientspec` records.
```

### P4dlg supports:
+ groupby
+ orderby
+ sortby
+ distint
+ having
+ limitby
+ exclude
+ find
+ etc.

eg. just some simple list of requirements:

+ Retrieve all clientspec records. 
+ Fields should be imited to 'name', 'extra', 'owner' & ''accessDate'. 
+ Group client records by 'Host" ("extra") & order them by "accessDate".
+ Let's limit the number of records to 25.

```Python
>>> clients = jnl(jnl.domain.type == 'client').select('name', 'extra', 'owner', 'accessDate')
>>> client_groups = clients.groupby('extra', orderby='accessDate')

# you can output results as a grid.
>>> client_groups.as_grid()

+---------------------+-----------------+-------+---------------------+
| name                | extra           | owner | accessDate          |
+---------------------+-----------------+-------+---------------------+
| anyschema           |                 | mart  | 2021/03/12 03:05:36 |
| client.protodev     |                 | mart  | 2021/04/14 08:14:43 |
| lxcharlotte.pycharm |                 | mart  | 2021/11/28 22:30:23 |
| mscharlotte         |                 | mart  | 2021/12/06 17:57:19 |
| upycharmclient      |                 | mart  | 2021/12/08 18:56:31 |
| linuxclient         |                 | mart  | 2022/02/23 12:24:56 |
| pycharmclient       |                 | mart  | 2022/07/19 13:54:21 |
| anastasia           |                 | mart  | 2023/07/17 21:19:43 |
| gc.charlotte        | computer.local  | mart  | 2022/02/15 12:36:11 |
| gitfusion           | computer.local  | mart  | 2022/06/01 15:21:32 |
| gc.fred             | computer.local  | fred  | 2022/07/19 13:58:41 |
| pyforce             | computer.local  | mart  | 2022/07/28 02:50:54 |
| dummy               | computer.local  | mart  | 2023/01/10 18:25:03 |
| swerve.client       | computer.local  | mart  | 2023/07/17 20:52:04 |
| gc.computer         | computer.local  | mart  | 2023/07/19 16:05:38 |
| stagingclient       | computer.local  | mart  | 2023/12/05 00:29:01 |
| gc.pycharm          | computer.local  | mart  | 2024/01/11 07:24:23 |
| p4query_dev_mac     | computer.local  | mart  | 2024/01/11 08:06:20 |
| anastasia_dev       | computer.local  | mart  | 2024/01/22 00:12:11 |
| gcrelease           | computer.local  | mart  | 2024/01/22 06:29:48 |
| gcstable            | computer.local  | mart  | 2024/02/07 03:48:57 |
| cvs_conversion      | computer.local  | mart  | 2024/03/03 00:38:18 |
| databraid_main      | computer.local  | mart  | 2024/06/13 04:27:54 |
| p4qgit              | computer.local  | mart  | 2024/06/27 04:09:08 |
| computer_dev        | computer.local  | mart  | 2024/07/27 20:09:06 |
| computer_scea       | computer.local  | mart  | 2024/07/27 20:17:11 |
| computer_py4        | computer.local  | mart  | 2024/08/30 18:25:46 |
| bla                 | computer.local  | mart  | 2024/09/08 07:17:57 |
| computer_p4q        | computer.local  | mart  | 2024/09/20 23:51:52 |
| computer_git_p4dlg  | computer.local  | mart  | 2024/09/29 16:17:58 |
| computer.depot      | computer.local  | mart  | 2024/11/18 09:00:18 |
| computer.bck        | computer.local  | mart  | 2024/11/27 16:29:54 |
| gcdev               | computer.local  | mart  | 2024/12/21 06:48:11 |
| computer_p4dlg      | computer.local  | mart  | 2025/01/07 12:31:47 |
| p4source            | uxcharlotte     | mart  | 2021/12/01 01:01:08 |
| bsclient            | uxcharlotte     | mart  | 2021/12/14 02:55:44 |
| lpycharmclient      | uxcharlotte     | mart  | 2022/02/02 17:42:11 |
| uxcharlotte.p4src   | uxcharlotte     | mart  | 2022/02/23 12:29:50 |
| uxcharlotte.pycharm | uxcharlotte     | mart  | 2022/02/24 12:01:12 |
| uxcharlotte.mart    | uxcharlotte     | mart  | 2022/04/07 15:25:57 |
| gcclient            | gc              | mart  | 2024/01/16 05:40:31 |
| gc_p4dlg            | gc              | mart  | 2024/10/20 05:17:28 |
| gcp4src             | minou           | mart  | 2024/01/16 05:39:42 |
| gcdev_minou         | minou           | mart  | 2024/05/14 01:29:33 |
| localclient         | raspberrypi     | mart  | 2021/04/14 08:05:57 |
| mart.win            | LAPTOP-I424S433 | mart  | 2021/11/18 03:03:09 |
| p4vtest             | LAPTOP-I424S433 | mart  | 2021/11/18 11:40:11 |
| p4client            | gareth.local    | mart  | 2021/10/25 15:00:50 |
| martclient          | gareth.local    | mart  | 2022/01/05 12:33:57 |
| miscclient          | gareth.local    | mart  | 2022/02/02 17:46:43 |
+---------------------+-----------------+-------+---------------------+
```

+ Please see working samples & example in /p4q/libsample.

## p4dlg in an interactive shell

```Python
%> python dlg.py shell
```

![run_shell](https://github.com/user-attachments/assets/14825c81-ada0-48d4-a0e6-834f9b8090c1)


