# p4dlg 
#### A set of *abstractions* that use SQL to access and interact with your Perforce instance and its resources.

### Abstractions:
+ P4Jnl (Interact with your metadata, without Perforce client or admin programs)
+ Py4RCS (Interact with your versioned files) - under construction
+ Py4 (Interact with your server using SQL)

### Intsallation:
+ drop p4dlg on your system somewhere.
+ qtconsole
+ I generally install Anaconda for 3rd party dependencies

### In a nutshell...
All abstractions basically work the same way.
1. reate a new connection (or load a previously created connection) as per the target abstraction requirements.
2. Start using.

### How & where do we use *p4dlg*?
Though *p4dlg* can be imported and used in script or broader programs, it can also be used interactively in an IPython QT shell (included in this package) where p4dlg is fully baked into it. Just type the following cmdline to start it up!

```Python
%> python dlg.py shell
```

![run_shell](https://github.com/user-attachments/assets/14825c81-ada0-48d4-a0e6-834f9b8090c1)

### Create a new or load an existing connection to a Perforce Journal (or checkpoint).
```Python
# Use `jnlconnect` to manage connections to journals and checkpoints.
# methods:    create - load - update - unload - destroy - purge - show
# `jnlconnect` attributes: create    create and store a connection to a journal
#                          load      load an existing connector
#                          update    update a connector's values
#                          unload    unload a connectior from the current scope
#                          destroy   destroy an existing connector
#                          purge     combines unload/destroy
#                          show      display the the data that defined the object
#                          
# Create a connection to a journal with jnlconnect.create
# parameters: args[0]  -> name,
#             keyword  -> journal = journal_path
#             keyword  -> version = release of the p4d instance
#                                   that created the journal
#             keyword  -> oSchema = the schema that that defines the p4db
#
#             Note that keywords `version` & `oSchema` are mutually exclusive.
#             Pass in one or the orther.
#             * a bit more on schemas further down. 
In [17]: jnlconnect.create('jnl',
                        **{
                           'journal': './resc/journals/checkpoint.rasp',
                           'version': 'r16.2'
                           }
                       )

# the connectors are persistent & p4dlg keeps track to make them available for reuse. Once created, they can be loaded at any time.
In [18]: jnlcon.load('jnl')
Reference (jnl) loaded
Out[18]: <P4Jnl ./resc/journals/journal.8>
```

### Create a new or load an existing connection to a Perforce Server instance .
```Python
# Use `p4connect` to manage connections to a Perforce Server Instance.
# methods:    create - load - update - unload - destroy - purge - show
# `p4connect` attributes:  create    create and store a connection to a Server instance
#                          load      load an existing connector
#                          update    update a connector's values
#                          unload    unload a connectior from the current scope
#                          destroy   destroy an existing connector
#                          purge     combines unload/destroy
#                          show      display the the data that defined the object
#                          
# Create a connection to a Server instance with p4connect.create
In [17]: p4connect.create('p4',                            # a name for your connection
                        **{
                           oSchema=oSchema,                # a perforce schema (don't worry
                                                           # about this, they are already
                                                           # included in the package)              
                           user='bigbird',                 # a p4 user to access P4D
                           port='anastasia.local:1777',    # the port to a p4d instance
                           client='my_client'              # the clientspec that defines your workspace
    )
                           }
                       )
# As above, once created you can easily reload it
In [18]: p4connect.load('p4')
Reference (oP4) loaded & connected to anastasia.local:1777
<Py4 anastasia.local:1777 >
```

## SQL features

### Query syntax
#### P4Jnl
```Python
#                     table     op    value
#                      |        |      |
In [19]: qry = (jnl.domain.type == 'client')
#                |           |
#             connector    column
```
#### Py4
```Python
#                     table       op    value
#                      |          |      |
In [19]: qry = (p4.clients.client == 'client')
#                |           |
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

## Py4
### A client program, A wrapper, an abstraction layer over the p4 cmd line client API probably describes it better. If you're like me, you have favoured the more robust and never failing cmd line client when interacting with your Perforce server. So using this connector might be counter intiative, at first. If we are to think SQL instead of `executable cmd *args`, we must alter our mind set, a little bit. Please, bare with me & let's just rip off the bandaid.

Let's forget the executable, the command and everything else that comes after that. I propose instead that we accept a new Perforce where the executable no longer an executable but rather a name. That is a name of a connection to a DB (after all the P4DB is just that, a DB). ALong the same lines, a command is no longer a command, but rather a table. Think of Postgres or mysql. Like that. Of course if we think of tables, we will start seeing fields with values. Here's what I mean:

```c++

>>> p4 client computer_p4dlg
Client:▷computer_p4dlg
 
 Update:▷2024/09/20 23:53:18
 
 Access:▷2024/10/04 19:14:53
  
 Owner:▷⋅bigbird
  
 Host:▷⋅⋅computer.local
  
 Description:
 ▷⋅⋅⋅Created by bigbird.
  
 Root:▷⋅⋅/Users/gc/anastasia/dev/p4dlg
  
 Options:▷⋅⋅⋅noallwrite noclobber nocompress unlocked nomodtime normdir
  
 SubmitOptions:▷⋅submitunchanged
 
 LineEnd:▷⋅⋅⋅local
  
 View:
 ▷⋅⋅⋅//dev/p4dlg/... //computer_p4dlg/... 
```


#### Other samples & examples
+ Please see working samples & example in /p4q/libsample.

## RCS <under_construction>
## P4DB <under_construction>


