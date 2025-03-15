*This readme is still very much a draft*

# ``p4dlg``

#### P4dlg is a set of *abstractions* written in Python that lets you take full advantage of SQL features and functionality to access - & interact with - your Perforce server instances and their resources. 

``Note: Though I don't know how/if this runs on Windows, it does however work as expected on both MacOS and Linux. There are plans, when time permits, to test and enable compatibility on Windows.``

### Abstractions:
+ P4Jnl - Interact with all of your metadata, without having to rely on a perforce client or admin program.
+ Py4   - You can put away the p4 cmd line, P4Python & p4v. Try P4dlg to interact with your Perforce instance!

### Intsallation:
+ drop p4dlg on your system somewhere.
+ requires IPython / qtconsole
+ I generally install Anaconda as my defualt distribution - if you don't and p4dlg complains about missing dependencies, please do let me know.

### Where do we use *p4dlg*?
``Import P4dlg and use it in script or broader programs or, use it interactively in an IPython QT shell (included in this package). It's OK, P4dlg is fully baked into it. More on this below``

### Create a connection to a Perforce Journal (or checkpoint).
```Python
>>> from libjnl.jnlIO import jnlconnector

>>> jnlfile = 'Users/gc/anastasia/dev/p4dlg/resc/journals/checkpoint.14'
>>> version = 'r15.2' 
>>> jnl = jnlconnector(jnlfile, version=version)
```

### Create a connection to a Perforce Server instance.
```Python
>>> from libpy4.py4IO import p4connector
>>> p4globals = {'user': 'bigbird', 'port': 'anastasia.local:1777', 'client': 'bigbird_workspace'}
>>> p4 = p4connector(**p4globals)
```

### A few examples on tables & fields.
Table names lookup
```Python
>>> jnl.tables
['config', 'counters', 'nameval', 'logger', 'ldap', 'server', 'svrview', 'remote', 'rmtview', 'stash', 'userrp', 'user', 'group', 'groupx', 'depot', 'stream', 'domain', 'template', 'templatesx', 'templatewx', 'viewrp', 'view', 'review', 'integed', 'integtx', 'resolve', 'resolvex', 'haverp', 'havept', 'have', 'label', 'locks', 'excl', 'archmap', 'rev', 'revtx', 'revcx', 'revdx', 'revhx', 'revpx', 'revsx', 'revsh', 'revbx', 'revux', 'working', 'workingx', 'traits', 'trigger', 'change', 'changex', 'changeidx', 'desc', 'job', 'fix', 'fixrev', 'bodresolve', 'bodtext', 'bodtextcx', 'bodtexthx', 'bodtextsx', 'bodtextwx', 'ixtext', 'ixtexthx', 'uxtext', 'protect', 'property', 'message', 'sendq', 'jnlack', 'monitor', 'rdblbr', 'tiny']

>>> p4.tables
['where', 'change', 'fixes', 'jobspec', 'have', 'status', 'renameuser', 'unshelve', 'delete', 'counter', 'clients', 'jobs', 'users', 'resolve', 'dbstat', 'key', 'protects', 'verify', 'streams', 'workspace', 'logtail', 'dbschema', 'rename', 'add', 'ldap', 'filelog', 'labels', 'stream', 'login', 'copy', 'client', 'archive', 'groups', 'sizes', 'user', 'flush', 'diff', 'integrate', 'sync', 'dbverify', 'changelists', 'attribute', 'zip', 'branches', 'help', 'populate', 'export', 'branch', 'logschema', 'edit', 'unzip', 'merge', 'typemap', 'tickets', 'clean', 'dirs', 'changelist', 'passwd', 'property', 'logparse', 'rec', 'obliterate', 'annotate', 'workspaces', 'admin', 'interchanges', 'unlock', 'unload', 'counters', 'list', 'depot', 'prune', 'review', 'journals', 'diff2', 'logger', 'changes', 'reopen', 'diskspace', 'opened', 'logappend', 'license', 'files', 'set', 'fstat', 'ldapsync', 'keys', 'logstat', 'print', 'lockstat', 'restore', 'tag', 'group', 'istat', 'submit', 'logrotate', 'describe', 'cachepurge', 'integrated', 'label', 'reviews', 'resolved', 'revert', 'depots', 'grep', 'logout', 'ping', 'protect', 'labelsync', 'info', 'triggers', 'ldaps', 'update', 'lock', 'reconcile', 'cstat', 'reload', 'job', 'fix', 'move', 'configure', 'shelve', 'monitor']
```

P4Jnl Table & Field objects
```Python
>>> jnl.rev
<libjnl.jnlSqltypes.JNLTable at 0x1161b92d0>

>>> jnl.rev.fields
[<JNLField db_action>, <JNLField table_revision>, <JNLField table_name>, <JNLField depotFile>, <JNLField depotRev>, <JNLField type>, <JNLField action>, <JNLField change>, <JNLField date>, <JNLField modTime>, <JNLField digest>, <JNLField size>, <JNLField traitLot>, <JNLField lbrIsLazy>, <JNLField lbrFile>, <JNLField lbrRev>, <JNLField lbrType>]

>>> jnl.rev.depotfile
<JNLField depotFile>
```

Py4 Table & Field object
```Python
>>> p4.files
<libpy4.py4Sqltypes.Py4Table at 0x130398890>

>>> p4.files.fields
[<Py4Field code>, <Py4Field depotFile>, <Py4Field rev>, <Py4Field change>, <Py4Field action>, <Py4Field type>, <Py4Field time>]

>>> p4.files.depotfile
<Py4Field depotFile>
```

## A few examples of SQL features and functionality.
### Queries
```Python
# P4JNl queries
>>> jnlquery = (jnl.rev.depotFile.contains('test'))       # A simple query
>>> jnlquery                                              # A query is a reference to class DLGQuery
<DLGQuery {'objp4': <P4Jnl ./resc/journals/checkpoint.14>,
'op': <function CONTAINS at 0x10593f1a0>,
'left': <JNLField depotFile>,
'right': 'test',
'inversion': False}>

>>> jnlfiles = jnl(jnlquery).select()                   # A recordset has usefull attributes needed to complete a SQL statement.
                                                     # Attributes like: select, fetch, etc.
>>> jnlfiles                                         # The selected records (DLGRecords).
<DLGRecords (18013)>                                 # found 18013 records

>>> jnlfiles.first()                                 # the very first record
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

# p4 Py4 queries
>>> p4query = (p4.files.depotfile.contains('test'))      # A simple query
>>> p4query                                              # A query is a reference to class DLGQuery
<DLGQuery {'objp4': <Py4 anastasia.local:1777 >,
'op': <function CONTAINS at 0x10593f1a0>,
'left': <Py4Field depotFile>,
'right': 'test',
'inversion': False}>

>>> p4files = p4(p4query).select()                     # A recordset has usefull attributes needed to complete a SQL statement.
                                                     # Attributes like: select, fetch, etc.
>>> p4files                                          # The selected records (DLGRecords).
<DLGRecords (8)>                                     # found 8 records

>>> p4files.last()                                   # the very last record
<DLGRecord {'action': 'edit',
 'change': '538',
 'depotFile': '//dev/p4dlg/unittests/unittesting_py4.py',
 'idx': 584,
 'rev': '3',
 'time': '2025/01/07 12:31:54',
 'type': 'text'}>
```
Both P4Jnl and Py4 share the same SQL features, functionality, and syntax. However, in the case of Py4, you need to modify your mindset a little bit... Instead of thinking `p4 commands` (```%> p4 command arg1 arg2```), think of them as tables. In other words, the same syntax over the same mechanics. So, going forward in this README, I will use either P4Jnl or Py4 in examples, but not both (unless stated otherwise).

### SQL features (aggregators, operators, expressions, inner join, outer join, etc.)

#### Aggregators:
+ groupby
+ orderby
+ sortby
+ distint
+ having
+ limitby
+ exclude
+ find
+ inner & outer joins
+ etc.


### Example using a list of simple & imaginary requirements:
+ Retrieve all clientspec records. 
+ Fields should be restricted to 'name', 'extra' (aka. 'Host:'), 'owner' & 'accessDate'. 
+ Group client records by 'Host" (aka. "extra") & order them by "accessDate".
+ Let's limit the number of records to 25.

```Python
>>> clients = jnl(jnl.domain.type == 'client').select('name', 'extra', 'owner', 'accessDate')
>>> client_groups = clients.groupby('extra', orderby='accessDate', limitby=(100,125))

# you can output results as a grid.
>>> client_groups.as_grid()

+--------------------+----------------+----------+---------------------+
| name               | extra          | owner    | accessDate          |
+--------------------+----------------+----------+---------------------+
| gc.charlotte       | computer.local | bigbird  | 2022/02/15 12:36:11 |
| gc.fred            | computer.local | fred     | 2022/07/19 13:58:41 |
| dummy              | computer.local | bigbird  | 2023/01/10 18:25:03 |
| gc.computer        | computer.local | bigbird  | 2023/07/19 16:05:38 |
| gc.pycharm         | computer.local | bigbird  | 2024/01/11 07:24:23 |
| anastasia_dev      | computer.local | bigbird  | 2024/01/22 00:12:11 |
| cvs_conversion     | computer.local | bigbird  | 2024/03/03 00:38:18 |
| databraid_main     | computer.local | bigbird  | 2024/06/13 04:27:54 |
| computer_dev       | computer.local | bigbird  | 2024/07/27 20:09:06 |
| computer_scea      | computer.local | bigbird  | 2024/07/27 20:17:11 |
| computer_py4       | computer.local | bigbird  | 2024/08/30 18:25:46 |
| bla                | computer.local | bigbird  | 2024/09/08 07:17:57 |
| computer_p4q       | computer.local | bigbird  | 2024/09/20 23:51:52 |
| computer_git_p4dlg | computer.local | bigbird  | 2024/09/29 16:17:58 |
| computer.depot     | computer.local | bigbird  | 2024/11/18 09:00:18 |
| computer.bck       | computer.local | bigbird  | 2024/11/27 16:29:54 |
| gcdev              | computer.local | bigbird  | 2024/12/21 06:48:11 |
| computer_p4dlg     | computer.local | bigbird  | 2025/01/07 12:31:47 |
| anyschema          |                | bigbird  | 2021/03/12 03:05:36 |
| client.protodev    |                | bigbird  | 2021/04/14 08:14:43 |
| bsclient           | uxcharlotte    | bigbird  | 2021/12/14 02:55:44 |
| gcclient           | gc             | bigbird  | 2024/01/16 05:40:31 |
| gc_p4dlg           | gc             | bigbird  | 2024/10/20 05:17:28 |
+--------------------+----------------+----------+---------------------+
```

Aggregators can be set as keyword arguments to select(). Note they can just as well be accessed after the records have been selected & retrieved as attributes to the DLGRecords objects.
I.e.:
```Python
>>> changes = jnl(jnl.change).select(find=lambda rec: ('test' not in rec.description))
>>> grouped_changes = changes.groupby(jnl.change.client, limitby=(1, 250), sortby=jnl.change.date)

# which is equivalent to
>>> grouped_changes = changes.limitby=(1, 250).groupby('client').sortby('date')

# or all together in a oneliner?
>>> grouped_changes = jnl(jnl.change).select(find=lambda rec: ('test' not in rec.description)).limitby=(1, 250).groupby('client').sortby('date')
```

Note that there is a ```groupdict``` keyword option. If set to ```True```, the groupby aggregator will return a single dictionary, instead of a set of records, where the key is the field value (which served to group these records) and the values are the records that corolate to each, respectively.
```Python
>>> grouped_changes = changes.groupby(jnl.change.client, limitby=(1, 250), sortby=jnl.change.date, groupdict=True)

>>> grouped_changes.keys()
dict_keys(['computer_p4dlg', 'computer_p4q', 'computer_py4'])

>>> grouped_changes
{'computer_p4dlg': <DLGRecords (55)>,
 'computer_p4q': <DLGRecords (190)>,
 'computer_py4': <DLGRecords (0)>}

>>> grouped_changes.computer_p4dlg
<DLGRecords (55)>
>>> grouped_changes.computer_p4dlg.first()
<DLGRecord {'access': '',
 'change': '480',
 'client': 'computer_p4dlg',
 'date': '2024/09/21 00:03:40',
 'db_action': 'pv',
 'descKey': '480',
 'description': 'copy of p4q, outside of a p4 co',
 'identify': '',
 'idx': 58,
 'importer': '',
 'root': '//dev/p4dlg/...',
 'status': '1',
 'table_name': 'db.change',
 'table_revision': '3',
 'user': 'bigbird'}>
```

#### Operators
+ You can use bitwise operators to combine query statements, though each statement must be parenthesized.
+ You can use any relational operators within a query statement.
+ You can use `contains`, `startswith` or `endswith` to check Field values.

eg.
```Python
qry = (jnl.domain.type == 'client') & (jnl.domain.description.contains('some_token')
```


#### SQL `IN` (```in``` is already a Python keyword, therefor renamed to ```belongs```)

An example of a simple & straightforward ```belongs``` 
```Python
>>> clientnames = ('p4client', 'computer_p4dlg', 'computer_dev')
>>> clientrecords = jnl(jnl.domain.name.belongs(clientnames)).select()
>>> clientrecords
<DLGRecords (3)>
```

An example of a nested belongs
```Python
>>> qry = ((jnl.domain.type == 'client') & (jnl.domain.extra == 'uxcharlotte'))
>>> targetclients = jnl(qry)._select(jnl.domain.name)
>>> targetclients
('bsclient',
 'uxcharlotte.pycharm',
 'uxcharlotte.p4src',Change 569 submitted.
 'lpycharmclient',
 'p4source',
 'uxcharlotte.bigbird')

>>> qry2 = (jnl.domain.name.belongs(targetclients))
>>> clientrecords = jnl(qry2).select()
>>> clientrecords
<DLGRecords (6)>

>>> clientrecords.first()
<DLGRecord {'accessDate': '2021/12/14 02:55:44',
 'db_action': 'pv',
 'description': 'Created by bigbird.\n',
 'extra': 'uxcharlotte',
 'idx': 5,
 'mount': '/home/gc',
 'mount2': '',
 'mount3': '',
 'name': 'bsclient',
 'options': '0',
 'owner': 'bigbird',
 'partition': '0',
 'serverid': '',
 'stream': '',
 'table_name': 'db.domain',
 'table_revision': '6',
 'type': '99',
 'updateDate': '2021/12/14 02:50:36'}>
```

#### Join (inner)
A merging of 2 records into one. 
however, the table must be included in the syntax (I.e.: rec.rev.depotFile & rec.change.user) as the default behaviour is to contain both records (flat=False). 

* Note that records are skipped where inner fields are non-matching.
```Python
>>> reference = (jnl.rev.change == jnl.change.change)
>>> recs = jnl(jnl.rev).select(join=jnl.change.on(reference), flat=False)
# alternatively, this syntax is equivalent:
>>> recs = jnl(reference).select()
>>> recs
<DLGRecords (73285)>

>>> rec = recs.first()
>>> rec
<DLGRecord {'change': <DLGRecord {'access': '',
                                  'change': '142',
                                  'client': 'bigbird.pycharm',
                                  'date': '2021/11/25',
                                  'db_action': 'pv',
                                  'descKey': '142',
                                  'description': 'renaming for case consistency',
                                  'identify': '',
                                  'idx': 1,
                                  'importer': '',
                                  'root': '',
                                  'status': '0',
                                  'table_name': 'db.change',
                                  'table_revision': '3',
                                  'user': 'bigbird'}>,
            'rev': <DLGRecord {'action': '8',
                               'change': '142',
                               'date': '2021/11/25',
                               'db_action': 'pv',
                               'depotFile': '//depot/pycharmprojects/sQuery/lib/sqFileIO.py',
                               'depotRev': '1',
                               'digest': '45C82D6A13E755DEBDE0BD32EA4B7961',
                               'idx': 1,
                               'lbrFile': '//depot/pycharmprojects/sQuery/lib/sqfileUtils.py',
                               'lbrIsLazy': '1',
                               'lbrRev': '1.121',
                               'lbrType': '0',
                               'modTime': '1630482775',
                               'size': '18420',
                               'table_name': 'db.rev',
                               'table_revision': '9',
                               'traitLot': '0',
                               'type': '0'}>
            }>

>>> print(f"Change `{rec.rev.change}` on depotFile `{rec.rev.depotFile}` by user `{rec.change.user}`")
Change `142` on depotFile `//depot/pycharmprojects/sQuery/lib/sqFileIO.py` by user `bigbird`
```

#### left (outer)
like join but records with non-matching fields are included in the recosoverall outrecords.
In this example, let's set ```flat=True```
```Python

>>> reference = (jnl.rev.change == jnl.change.change)
>>> recs = jnl(jnl.rev).select(left=jnl.change.on(reference), flat=False)
>>> rec = recs(0)
>>> rec
<DLGRecord {'action': '0',
 'change': '3',
 'client': 'anyschema',
 'date': '2021/03/12 03:00:42',
 'db_action': 'pv',
 'depotFile': '//depot/anyschema_2db/.DS_Store',
 'depotRev': '1',
 'descKey': '3',
 'description': 'initital checking of anyschema_',
 'digest': '59DD11B3B8804F6AA7A9BBCE1B583430',
 'identify': '',
 'idx': 1,
 'importer': '',
 'lbrFile': '//depot/anyschema_2db/.DS_Store',
 'lbrIsLazy': '0',
 'lbrRev': '1.3',
 'lbrType': '65539',
 'modTime': '2021/02/09 05:50:14',
 'root': '//depot/anyschema_2db/...',
 'size': '6148',
 'table_name': 'db.rev',
 'table_revision': '9',
 'traitLot': '0',
 'type': '65539',
 'user': 'bigbird'}>

>>> print(f"Change `{rec.change}` on depotFile `{rec.depotFile}` by user `{rec.user}`")
Change `3` on depotFile `//depot/anyschema_2db/.DS_Store` by user `bigbird`
```

#### Merging records (braiding) - right side over left where common fields are overwrittern
```Python
>>> reference = (jnl.rev.change == jnl.change.change)
>>> recs = jnl(jnl.rev).select(merge_records=jnl.change.on(reference))

# which is equivalent to:
# jnl(jnl.rev).select(join=jnl.change.on(reference))

>>> rec = recs(0)
>>> rec
<DLGRecord {'action': '0',
 'change': '3',
 'client': 'anyschema',
 'date': '2021/03/12 03:00:42',
 'db_action': 'pv',
 'depotFile': '//depot/anyschema_2db/.DS_Store',
 'depotRev': '1',
 'descKey': '3',
 'description': 'initital checking of anyschema_',
 'digest': '59DD11B3B8804F6AA7A9BBCE1B583430',
 'identify': '',
 'idx': 1,
 'importer': '',
 'lbrFile': '//depot/anyschema_2db/.DS_Store',
 'lbrIsLazy': '0',
 'lbrRev': '1.3',
 'lbrType': '65539',
 'modTime': '2021/02/09 05:50:14',
 'root': '//depot/anyschema_2db/...',
 'size': '6148',
 'table_name': 'db.rev',
 'table_revision': '9',
 'traitLot': '0',
 'type': '65539',
 'user': 'bigbird'}>

print(f"Change `{rec.change}` on depotFile `{rec.depotFile}` by user `{rec.user}`")
Change `3` on depotFile `//depot/anyschema_2db/.DS_Store` by user `bigbird`
```


#### p4dlg has an an interactive shell where we can muck around, test stuff, or generate some those awesome matplotlib graphes with your results.

```Python
%> python dlg.py shell
```

![run_shell](https://github.com/user-attachments/assets/14825c81-ada0-48d4-a0e6-834f9b8090c1)


+ Please see more working samples & examples in /p4dlg/libsample. 
