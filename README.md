# p4dlg

### A set of *abstractions* that use SQL, supported by a common set of APIs, to interact with your Perforce instance and its resources. 

While I work on formal documentation, this README should at least give you an idea of what *p4dlg* is. There are also working samples and examples in /p4q/libsample.

### All abstractions, whatever they support, adhere to the same syntax, conventions and functionality. The following examples can be applied, conceptually, to any abstraction.

## How do we use it?
Though *p4dlg* can be imported and used in script or broader programs, it can also be used interactively in an IPython QT shell where p4dlg is fully baked into it. Just type the following cmdline:


```Python
%> python dlg.py shell
```

![run_shell](https://github.com/user-attachments/assets/14825c81-ada0-48d4-a0e6-834f9b8090c1)

* Please look in the /p4dlg/libsample directory for a more examples & samples.

# Connectors.
## P4Jnl
### Access all your metadata without being hindered by proprietary hurdles and without any perforce client program or admin program.

A checkpoint is a snapshot, a textual representation of your Perforce DB. As records are created, the server outputs the data to an ongoing journal (until truncated). Conversely, they can be used to rebuild your database by replaying a checkpoint (or set of journals), or a modified checkpoint (or fragment thereof) to modify existing records (checkpoint surgery). In opther words, they can be used to insert, update and delete records. 

### A sample journal fragment.
```Python
@pv@ 7 @db.user@ @bigbird@ @bigbird@@pycharmclient@ @@ 1624456923 1624456923 @bigbird@ @@ 2 @@ 0 0 0 0 0 0
@pv@ 6 @db.domain@ @localclient@ 99 @raspberrypi@ @/home/pi@ @@ @@ @mart@ 1615718544 1618412757 0 @Created by bigbird.\n@
@pv@ 9 @db.rev@ @//depot/codesamples/squery/squery.py@ 2 131072 1 13 1618413823 1618413704 47D355EA47D55FAEC8C92A2ABDA980BC 40271 0 0 @//depot/codesamples/squery/squery.py@ @1.13@ 131072 
@pv@ 9 @db.rev@ @//depot/codesamples/squery/squery.py@ 1 131072 0 11 1615560173 1614326814 B72B933FC28A76969DB23DA8A219091A 46151 0 0 @//depot/codesamples/squery/squery.py@ @1.11@ 131072
```

It kind of looks like a .CSV file, but without column headers. P4D does not discriminate, each line is a record of some transaction, regardless of table, in order and as it occures. Therefore a proficient knowledge of the p4 schema is nice t o have. Luckily, the know-how is naked-in to p4dlg, for any server/schema release.

### Create or load an existing connection.
```Python
# Use `jnlconnect` to manage connections.
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

# the connectors are persistent & p4dlg keeps track to make them available for reuse. The can be loaded at any time.
In [18]: jnlcon.load('jnl')
Reference (jnl) loaded
Out[18]: <P4Jnl ./resc/journals/journal.8>
```

### Query syntax
```Python
#                     table     op    value
#                      |        |      |
In [19]: qry = (jnl.domain.type == 'client')
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

Passing in a query to the connector's __call__ method, forces it to return a set of records, *DLGRecordSet*.
The set of records are not the same as the selected records. It is basically a generator with useful methods
and attributes.
### Conector + query = record set
```Python
In [22]: jnl(qry)                                # Connection objects are callable. Among other things,
                                                 # you can pass in queries forces it to return a set of
                                                 # records
Out[23]: <DLGRecordSet (<class 'libjnl.jnlFile.JNLFile'>) >

In [24]: clients = recordset.select()            # equivalent to `SELECT * FROM domain WHERE type = client`
In [25]: clients
Out[25]: <DLGRecords (188)>                      # The target journal contains 188 `clientspec` records.
```

## Selecting records / syntax
### Typing the thing as a one liner, say in an interactive QT Console, flows from the end of your finger tips.
```Python
In [26]: clients = jnl(jnl.domain.type == 'client').select()
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
 'description': 'Created by mart.\n',
 'extra': 'LAPTOP-I424S433',
 'idx': 1,
 'mount': 'c:Usersgoodcdepot',
 'mount2': '',
 'mount3': '',
 'name': 'mart.win',
 'options': '0',
 'owner': 'mart',
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
{'mart.win': <DLGRecords (2)>,
 'p4vtest': <DLGRecords (23)>,
 'pycharmclient': <DLGRecords (1)>,
 'lpycharmclient': <DLGRecords (32)>,
 'linuxclient': <DLGRecords (4)>,
 'lxcharlotte.pycharm': <DLGRecords (10)>,
 'p4source': <DLGRecords (1)>,
 'mscharlotte': <DLGRecords (3)>,
 'upycharmclient': <DLGRecords (13)>,
 'test': <DLGRecords (3)>,
 'bsclient': <DLGRecords (2)>,
 'martclient': <DLGRecords (1)>,
 'anastasia': <DLGRecords (9)>,
 'gc.pycharm': <DLGRecords (36)>,
 'computer.depot': <DLGRecords (29)>,
 'miscclient': <DLGRecords (1)>,
 'projclient': <DLGRecords (8)>,
 'gc.local': <DLGRecords (4)>,
 'gc.charlotte': <DLGRecords (1)>,
 'gc.fred': <DLGRecords (3)>,
 'uxcharlotte.pycharm': <DLGRecords (1)>,
 'computer.bck': <DLGRecords (1)>}

In [38]: for client in client_groups.linuxclient:
    print(client.name, client.accessdate)
    
linuxclient 2021/11/24                # Though perforce stores datetime values in linux time, p4dlg converts them 
linuxclient 2021/11/27                # to the more readable ISO format, apparently favoured by their own client 
linuxclient 2021/12/01                # programs.
linuxclient 2021/12/01
```
### More examples below on the use of aggregators, operators, queries, expressions, etc., as well as p4dlg's take on using SQL mechanics to drive interactions with a Perforce instance.

Moving on to the next coonector...
## Py4
### A client

## 3. P4db
## 4. Rcs


