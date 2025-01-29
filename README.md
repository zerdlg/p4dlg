## ``p4dlg``
*this readme is under construction*
#### P4dlg is a set of *abstractions* written in Python that lets you take full advantage of SQL features and functionality to access & interact with your Perforce instances and their resources. 

I am making 2 Perforce Abstractions available for public use, though, depending on interest, more may be released. My thinking is that a more complete toolset would not only extend the versionning capabilities and experience that SQL brings to this exercise, but that it would also provide some exposure, even if just a little, to the fun that can be had when applying a new (ish) perspective & mindset on a versioning engine's relatively unchanging set of practices and uses. I don't just mean mechanically, though in its self, there is definitely some awsomeness in pushing the limitations of a technology's proprietory design (eg. the mighty P4 DB), but also philosophically so as to elevate or broaden the perception we may have of a versionning engine's traditional role in application design. 

### Abstractions:
+ P4Jnl (Interact with all of your metadata, without without having to rely on a Perforce client)
+ Py4 (you can put away the p4 cmd line, P4Python & p4v. Interact with your Perforce instance using SQL)

### How do we use it and what SQL functionality does it support?

[a few details and examples ... add here]


[more stuff here ...]


``Note: Though I don't know how this runs on Windows, it does however work as expected on both MacOS and Linux. ``

### Intsallation:
+ drop p4dlg on your system somewhere.
+ requires IPython / qtconsole
+ I generally install Anaconda as my defualt distribution - if you don't and p4dlg complains about missing dependencies, please do let me know.

### Where do we use *p4dlg*?
``Though *p4dlg* can be imported and used in script or broader programs, it can also be used interactively in an IPython QT shell (included in this package) where p4dlg is fully baked into it. Mor on this below``

### Create a new or load an existing connection to a Perforce Journal (or checkpoint).
```Python
from libjnl.jnlIO import jnlconnector

jnlfile = 'Users/gc/anastasia/dev/p4dlg/resc/journals/checkpoint.14'
version = 'r15.2' 

jnl = jnlconnector(jnlfile, version=version)
```

### Create a new or load an existing connection to a Perforce Server instance .
```Python
from libpy4.py4IO import p4connector

p4globals = {'user': 'bigbird',
             'port': 'anastasia.local:1777',
             'client': 'bigbird_workspace'}     # any valid p4global

p4 = p4connector(**p4globals)
```

## SQL features

### Query syntax
#### P4Jnl
 ```Python
#                     table     op    value
#                      ^        ^      ^
In [19]: qry = (jnl.domain.type == 'client')
#                v           v
#             connector    column
```


#### Py4
```Python
#                     table       op    value
#                      ^          ^      ^
In [19]: qry = (p4.clients.client == 'client')
#                v           v
#             connector    column
```


### Building a query.
```Python
In [20]: qry = (jnl.domain.type == 'client')     # A simple query.
In [21]: qry
Out[21]: 
<DLGQuery {'inversion': False,                   # A query is a reference to class `DLGQuery`.
 'left': <JNLField type>,
 'objp4': <P4Jnl ./resc/journals/journal.8>,     
 'op': <function EQ at 0x104d32700>,
 'right': 'client'}>
```


### Connector + query = recordset
```Python
In [22]: recordste = jnl(qry)                    # Connection objects are callable, take queries and expose
                                                 # useful attributes such as  `select`, `fetch`, update, etc.
In [23]: RECORDSET
Out[23]: <DLGRecordSet (<class 'libjnl.jnlFile.JNLFile'>)>

In [24]: clients = recordset.select()            # equivalent to `SELECT * FROM domain WHERE type = client`
In [25]: clients
Out[25]: <DLGRecords (188)>                      # The target journal defines 188 `clientspec` records.
```


### Usage and Examples
####  Example 1. Selecting records (P4Jnl)
```Python
In [12]: my_files = jnl(jnl.rev.change == 142).select()

In [13]: my_files
Out[13]: <DLGRecords (2)>

In [14]: my_files.first()
Out[14]: 
<DLGRecord {'action': '8',
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
```

####  Example 2. Selecting records (Py4)
```Python
In [15]: my_files = p4(p4.files.change == 510).select()

In [16]: my_files
Out[16]: <DLGRecords (46)>

In [17]: my_files(0)
Out[17]:
<DLGRecord {'action': 'edit',
 'change': '510',
 'depotFile': '//dev/p4dlg/libdlg/dlgExtract.py',
 'idx': 129,
 'rev': '2',
 'time': '1731949279',
 'type': 'text'}> 
```

#### Selecting records
```Python
In [26]: jnl(jnl.domain.type == 'client').select()
Out[26]: <DLGRecords (188)>
```

### Break it down to its syntactical parts.
```
In [27]: client_recordset = jnl(jnl.domain.type == 'client')
```

eg.
```Python

In [27]: qry = (jnl.domain.type == 'client')

```

```Python
In [28]: clients.first()                         # `clients` is a reference to class `DLGRecords`, and
                                                 # exposes interesting SQL attributes.
Out[28]: 
<DLGRecord {'accessDate': '2021/11/14',          # `DLGRecords` contains references to class `DLGRecord`
 'db_action': 'pv',
 'description': 'Created by bert.\n',
 'extra': 'desktop_bert',
 'idx': 1,
 'mount': 'c:/Users/goodbert/depot',
 'mount2': '',
 'mount3': '',
 'name': 'bert_workspace',
 'options': '0',
 'owner': 'bert',
 'partition': '0',
 'serverid': '',
 'stream': '',
 'table_name': 'db.domain',
 'table_revision': '6',
 'type': '99',
 'updateDate': '2021/11/14'}>
```

### A connnector has useful attributes
eg.
```Python
# List all available table for this version
jnl.tables
Out[29]: 
['config',
 'counters',
 'nameval',
 'logger',
 'ldap',
 'server',
 'svrview',
 'remote',
 'rmtview',
 'stash',
 'userrp',
 'user',
 'group',
...
]

# Once referenced, a table object becomes acceesible as an attribute of said connection object.

In [30]: jnl.domain
Out[30]: <libjnl.jnlSqltypes.JNLTable at 0x16099e210>      # A table is a reference to class JNLTable (in this case).
In [31]: jnl.domain.fields                                 # A table object exposes its fields and other usefull attributes.
Out[31]: 
[<JNLField idx>,
 <JNLField db_action>,
 <JNLField table_revision>,
 <JNLField table_name>,
 <JNLField name>,
 <JNLField type>,
 <JNLField extra>,
 <JNLField mount>,
 <JNLField mount2>,
 <JNLField mount3>,
 <JNLField owner>,
 <JNLField updateDate>,
 <JNLField accessDate>,
 <JNLField options>,
 <JNLField description>,
 <JNLField stream>,
 <JNLField serverid>,
 <JNLField partition>]

In [32]: jnl.domain.desc                                   # The tables description attribute, as per the referenced schema.
Out[33]: 'Domains: depots, clients, labels, branches, streams, and typemap'

In [34]: jnl.domain.type                                   # The table's field objects are exposed.
Out[34]: <JNLField type>

In [35]: jnl.domain.type.desc                              # A table's field attributes can be accessed.
Out[35]: 'Type of domain'

```

## Agregators Operators, Expressions, Queries etc.

eg. Group client (domain) records by `name` & order them by `accessDate`.
```Python
In [36]: client_groups = clients.groupby('name', orderby=jnl.domain.accessdate, groupdict=True)

# The `groupdict` attribute modifies groupby's return type. When set to True (False is the default),
# it will not return the set of records, as we would expect, but instead, a dict where the keys are the 
# field values that make up the tagret group names. The values are the records belonging to the field 
# that groups the the records.

In [37]: client_groups
Out[37]: 
{'bert_workspace': <DLGRecords (2)>,
 'p4vtest': <DLGRecords (23)>,
 'pycharm_client': <DLGRecords (1)>,
 'lpycharm_client': <DLGRecords (32)>,
 'linux_client': <DLGRecords (4)>,
 'lx_ernie_pycharm': <DLGRecords (10)>,
 'p4grover': <DLGRecords (1)>,
 'thecount_workspace': <DLGRecords (3)>,
 'upycharm_client': <DLGRecords (13)>,
 'test': <DLGRecords (3)>,
 'bsclient': <DLGRecords (2)>,
 'bigbirdclient': <DLGRecords (1)>,
 'anastasia': <DLGRecords (9)>,
 'bert_pycharm': <DLGRecords (36)>,
 'computer.depot': <DLGRecords (29)>,
 'miscclient': <DLGRecords (1)>,
 'projclient': <DLGRecords (8)>,
 'gc_local': <DLGRecords (4)>,
 'gc_bob': <DLGRecords (1)>,
 'gc.fred': <DLGRecords (3)>,
 'ux_pycharm': <DLGRecords (1)>,
 'computer_bck': <DLGRecords (1)>}

In [38]: for client in client_groups.linuxclient:
    print(client.name, client.accessdate)
    
linuxclient 2021/11/24                # Though perforce stores datetime values in linux time, p4dlg converts them 
linuxclient 2021/11/27                # to the more readable ISO format, apparently favoured by their own client 
linuxclient 2021/12/01                # programs.
linuxclient 2021/12/01
```
### More examples below on the use of aggregators, operators, queries, expressions, etc., as well as p4dlg's take on using SQL mechanics to drive interactions with a Perforce instance.

#### Other samples & examples
+ Please see working samples & example in /p4q/libsample.

## p4dlg in an interactive shell

```Python
%> python dlg.py shell
```

![run_shell](https://github.com/user-attachments/assets/14825c81-ada0-48d4-a0e6-834f9b8090c1)


## RCS <under_construction>
## P4DB <under_construction>


