# ``p4dlg``

#### P4dlg is a set of *abstractions* written in Python that lets you take full advantage of SQL features and functionality to access - & interact with - your Perforce server instances and their resources. 

``Note: Though I don't know how/if this runs on Windows, it does however work as expected on both MacOS and Linux. ``

### Abstractions:
+ P4Jnl - Interact with all of your metadata, without having to rely on a perforce client or admin program.
+ Py4   - You can put away the p4 cmd line, P4Python & p4v. Try P4dlg to interact with your Perforce instance!

### Intsallation:
+ drop p4dlg on your system somewhere.
+ requires IPython / qtconsole
+ I generally install Anaconda as my defualt distribution - if you don't and p4dlg complains about missing dependencies, please do let me know.

### Where do we use *p4dlg*?
``Import P4dlg and use it in script or broader programs or, use it interactively in an IPython QT shell (included in this package). It's OK, P4dlg is fully baked into it. More on this below``

### Create a connection to a Perforce Journal (or checkpoint) and a quick example.
```Python
>>> from libjnl.jnlIO import jnlconnector

>>> jnlfile = 'Users/gc/anastasia/dev/p4dlg/resc/journals/checkpoint.14'
>>> version = 'r15.2' 
>>> jnl = jnlconnector(jnlfile, version=version)

>>> jnl.tables
['config', 'counters', 'nameval', 'logger', 'ldap', 'server', 'svrview', 'remote', 'rmtview', 'stash', 'userrp', 'user', 'group', 'groupx', 'depot', 'stream', 'domain', 'template', 'templatesx', 'templatewx', 'viewrp', 'view', 'review', 'integed', 'integtx', 'resolve', 'resolvex', 'haverp', 'havept', 'have', 'label', 'locks', 'excl', 'archmap', 'rev', 'revtx', 'revcx', 'revdx', 'revhx', 'revpx', 'revsx', 'revsh', 'revbx', 'revux', 'working', 'workingx', 'traits', 'trigger', 'change', 'changex', 'changeidx', 'desc', 'job', 'fix', 'fixrev', 'bodresolve', 'bodtext', 'bodtextcx', 'bodtexthx', 'bodtextsx', 'bodtextwx', 'ixtext', 'ixtexthx', 'uxtext', 'protect', 'property', 'message', 'sendq', 'jnlack', 'monitor', 'rdblbr', 'tiny']

>>> jnl.rev
<libjnl.jnlSqltypes.JNLTable at 0x1161b92d0>

# fields vs. fieldnames:
# They are both attributes of a table object, but the `fields` attribute retrieves field objects, while the `fieldnames` attribute retrieves strings.
>>> jnl.rev.fieldnames
['idx', 'db_action', 'table_revision', 'table_name', 'depotFile', 'depotRev', 'type', 'action', 'change', 'date', 'modTime', 'digest', 'size', 'traitLot', 'lbrIsLazy', 'lbrFile', 'lbrRev', 'lbrType']

>>> jnl.rev.fields
[<JNLField db_action>,
 <JNLField table_revision>,
 <JNLField table_name>,
 <JNLField depotFile>,
 <JNLField depotRev>,
 <JNLField type>,
 <JNLField action>,
 <JNLField change>,
 <JNLField date>,
 <JNLField modTime>,
 <JNLField digest>,
 <JNLField size>,
 <JNLField traitLot>,
 <JNLField lbrIsLazy>,
 <JNLField lbrFile>,
 <JNLField lbrRev>,
 <JNLField lbrType>]

>>> jnl.rev.depotfile
<JNLField depotFile>

>>> query = (jnl.rev.depotFile.contains('test'))       # A simple query. -> <connector>.<table>.<field> <operator> <value>
>>> query                                              # A query is a reference to class DLGQuery.
<DLGQuery {'objp4': <P4Jnl ./resc/journals/checkpoint.14>,
'op': <function CONTAINS at 0x10593f1a0>,
'left': <JNLField depotFile>,
'right': 'test',
'inversion': False}>

# jnl is a callable and takes a query.
>>> my_recordset = jnl(query)                          # Connector + query = recordset
>>> my_recordset                                       # It returns a set of records (DLGRecordSet()).
<DLGRecordSet (<class 'libjnl.jnlFile.JNLFile'>) >     

>>> testfiles = jnl(query).select()                    # A recordset has usefull attributes needed to complete a SQL statement.
                                                       # Attributes like: ```select```, ```fetch```, etc.
>>> testfiles                                          # The selected records (DLGRecords).
<DLGRecords (18013)>                                   # found 18013 records

>>> testfiles.first()                                  # the very first record
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

### Create a connection to a Perforce Server instance and a quick usage example.
```Python
>>> from libpy4.py4IO import p4connector
>>> p4globals = {'user': 'bigbird', 'port': 'anastasia.local:1777', 'client': 'bigbird_workspace'}
>>> p4 = p4connector(**p4globals)

>>> p4.tables
['where', 'change', 'fixes', 'jobspec', 'have', 'status', 'renameuser', 'unshelve', 'delete', 'counter', 'clients', 'jobs', 'users', 'resolve', 'dbstat', 'key', 'protects', 'verify', 'streams', 'workspace', 'logtail', 'dbschema', 'rename', 'add', 'ldap', 'filelog', 'labels', 'stream', 'login', 'copy', 'client', 'archive', 'groups', 'sizes', 'user', 'flush', 'diff', 'integrate', 'sync', 'dbverify', 'changelists', 'attribute', 'zip', 'branches', 'help', 'populate', 'export', 'branch', 'logschema', 'edit', 'unzip', 'merge', 'typemap', 'tickets', 'clean', 'dirs', 'changelist', 'passwd', 'property', 'logparse', 'rec', 'obliterate', 'annotate', 'workspaces', 'admin', 'interchanges', 'unlock', 'unload', 'counters', 'list', 'depot', 'prune', 'review', 'journals', 'diff2', 'logger', 'changes', 'reopen', 'diskspace', 'opened', 'logappend', 'license', 'files', 'set', 'fstat', 'ldapsync', 'keys', 'logstat', 'print', 'lockstat', 'restore', 'tag', 'group', 'istat', 'submit', 'logrotate', 'describe', 'cachepurge', 'integrated', 'label', 'reviews', 'resolved', 'revert', 'depots', 'grep', 'logout', 'ping', 'protect', 'labelsync', 'info', 'triggers', 'ldaps', 'update', 'lock', 'reconcile', 'cstat', 'reload', 'job', 'fix', 'move', 'configure', 'shelve', 'monitor']

>>> p4.files
<libpy4.py4Sqltypes.Py4Table at 0x130398890>

# fields vs. fieldnames:
# They are both attributes of a table object, but the `fields` attribute retrieves field objects, while the `fieldnames` attribute retrieves strings.
>>> p4.files.fieldnames
['code', 'depotFile', 'rev', 'change', 'action', 'type', 'time']

>>> p4.files.fields
[<Py4Field code>,
 <Py4Field depotFile>,
 <Py4Field rev>,
 <Py4Field change>,
 <Py4Field action>,
 <Py4Field type>,
 <Py4Field time>]

>>> p4.files.depotfile
<Py4Field depotFile>

>>> query = (p4.files.depotfile.contains('test'))      # A simple query. -> <connector>.<table>.<field> <operator> <value>
>>> query
<DLGQuery {'objp4': <Py4 anastasia.local:1777 >,
'op': <function CONTAINS at 0x10593f1a0>,
'left': <Py4Field depotFile>,
'right': 'test',
'inversion': False}>

#jnl is a callable and takes a query.
>>> my_recordset = p4(query)                           # Connector + query = recordset
>>> my_recordset                                       # It returns a set of records (DLGRecordSet()).
<DLGRecordSet (<class 'libpy4.py4Run.Py4Run'>) >

>>> testfiles = p4(query).select()                     # A recordset has usefull attributes needed to complete a SQL statement.
                                                       # Attributes like: ```select```, ```fetch```, etc.
>>> testfiles                                          # The selected records (DLGRecords).
<DLGRecords (8)>                                       # found 8 records

>>> testfiles.last()                                   # the very last record
<DLGRecord {'action': 'edit',
 'change': '538',
 'depotFile': '//dev/p4dlg/unittests/unittesting_py4.py',
 'idx': 584,
 'rev': '3',
 'time': '2025/01/07 12:31:54',
 'type': 'text'}>
```

Both P4Jnl and Py4 share the same SQL features, functionality, and syntax. However, in the case of Py4, you need to modify your mindset a little bit... Instead of thinking `p4 commands` (```%> p4 command arg1 arg2```), think of them as tables. In other words, the same syntax over the same mechanics. So, going forward in this README, I will use either P4Jnl or Py4 in examples, but not both (unless stated otherwise).

# SQL features (aggregators, operators, expressions, inner join, outer join, etc.)

### P4dlg supports aggregators:
+ groupby
+ orderby
+ sortby
+ distint
+ having
+ limitby
+ exclude
+ find
+ etc.

eg. just some list of imaginary requirements:

+ Retrieve all clientspec records. 
+ Fields should be limited to 'name', 'extra', 'owner' & ''accessDate'. 
+ Group client records by 'Host" (aka. "extra") & order them by "accessDate".
+ Let's limit the number of records to 25.

```Python
>>> clients = jnl(jnl.domain.type == 'client').select('name', 'extra', 'owner', 'accessDate')
>>> client_groups = clients.groupby('extra', orderby='accessDate', limitby=(1,25))

# you can output results as a grid.
>>> client_groups.as_grid()

+--------------------+----------------+-------+---------------------+
| name               | extra          | owner | accessDate          |
+--------------------+----------------+-------+---------------------+
| gc.charlotte       | computer.local | mart  | 2022/02/15 12:36:11 |
| gc.fred            | computer.local | fred  | 2022/07/19 13:58:41 |
| dummy              | computer.local | mart  | 2023/01/10 18:25:03 |
| gc.computer        | computer.local | mart  | 2023/07/19 16:05:38 |
| gc.pycharm         | computer.local | mart  | 2024/01/11 07:24:23 |
| anastasia_dev      | computer.local | mart  | 2024/01/22 00:12:11 |
| cvs_conversion     | computer.local | mart  | 2024/03/03 00:38:18 |
| databraid_main     | computer.local | mart  | 2024/06/13 04:27:54 |
| computer_dev       | computer.local | mart  | 2024/07/27 20:09:06 |
| computer_scea      | computer.local | mart  | 2024/07/27 20:17:11 |
| computer_py4       | computer.local | mart  | 2024/08/30 18:25:46 |
| bla                | computer.local | mart  | 2024/09/08 07:17:57 |
| computer_p4q       | computer.local | mart  | 2024/09/20 23:51:52 |
| computer_git_p4dlg | computer.local | mart  | 2024/09/29 16:17:58 |
| computer.depot     | computer.local | mart  | 2024/11/18 09:00:18 |
| computer.bck       | computer.local | mart  | 2024/11/27 16:29:54 |
| gcdev              | computer.local | mart  | 2024/12/21 06:48:11 |
| computer_p4dlg     | computer.local | mart  | 2025/01/07 12:31:47 |
| anyschema          |                | mart  | 2021/03/12 03:05:36 |
| client.protodev    |                | mart  | 2021/04/14 08:14:43 |
| bsclient           | uxcharlotte    | mart  | 2021/12/14 02:55:44 |
| gcclient           | gc             | mart  | 2024/01/16 05:40:31 |
| gc_p4dlg           | gc             | mart  | 2024/10/20 05:17:28 |
+--------------------+----------------+-------+---------------------+
```
Though such things as aggregators can be set as keyword arguments, they can just as well be accessed as DLGRecords object attributes.
Or do both...

I.e.:
```Python
# Instead of passing in a query, specify a table object which begets all available records.
# .select() also takes aggregators.

>>> change_records = jnl(jnl.change).select(find=lambda rec: ('test' not in rec.description))
>>> grouped_changes = change_records.groupby(jnl.change.client, limitby=(1, 250), sortby=jnl.change.date)

# which is equivalent to
>>> grouped_changes = change_records.limitby=(1, 250).groupby('client').sortby('date')

# or all together in a oneliner ?
>>> grouped_changes = jnl(jnl.change).select(find=lambda rec: ('test' not in rec.description)).limitby=(1, 250).groupby('client').sortby('date')
```

### `belongs` equivalent to a SQL `in` 

### P4dlg supports expressions:
+ belongs
+ on

### P4dlg supports inner & outer joins:


   
+ Please see more working samples & examples in /p4dlg/libsample. 

# Inner joins, outer joins and merging records (braiding)
`` ``
`` ``
`` ``
`` ``



## p4dlg has an an interactive shell where we can muck around, test stuff, or generate some those awesome matplotlib graphes with your results.

```Python
%> python dlg.py shell
```

![run_shell](https://github.com/user-attachments/assets/14825c81-ada0-48d4-a0e6-834f9b8090c1)


