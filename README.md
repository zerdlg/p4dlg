# p4dlg

### A set of *abstractions* that use SQL, supported by a common set of APIs, to interact with your Perforce instance and its resources. 

While I work on formal documentation, this README should at least give you an idea of what *p4dlg* is. There are also working samples and examples in /p4q/libsample.

### All abstractions, whatever they support, adhere to the same syntax, conventions and functionality. The following examples can be applied, conceptually, to any abstraction.

## Where?
Though *p4dlg* can be imported and used in script or broader programs, it can also be used interactively in an IPython QT shell where p4dlg is fully baked into it. Just type the following cmdline:


```Python
%> python dlg.py shell
```

![run_shell](https://github.com/user-attachments/assets/14825c81-ada0-48d4-a0e6-834f9b8090c1)

* Please look into the /p4dlg/libsample directory for a more detailed examples.

# P4Jnl
### Access all your metadata without being hindered by proprietary hurdles and without any perforce client program or admin program.

A checkpoint is a snapshot, a textual representation of your Perforce DB. As records are created, the server outputs the data to an ongoing journal (until truncated). Conversely, they can be used to rebuild your database by replaying a checkpoint (or set of journals), or a modified checkpoint (or fragment thereof) to modify existing records (checkpoint surgery). In opther words, they can be used to insert, update and delete records. 

### A sample journal fragment.

```Python
@pv@ 7 @db.user@ @bigbird@ @bigbird@@pycharmclient@ @@ 1624456923 1624456923 @bigbird@ @@ 2 @@ 0 0 0 0 0 0
@pv@ 6 @db.domain@ @localclient@ 99 @raspberrypi@ @/home/pi@ @@ @@ @mart@ 1615718544 1618412757 0 @Created by bigbird.\n@
@pv@ 9 @db.rev@ @//depot/codesamples/squery/squery.py@ 2 131072 1 13 1618413823 1618413704 47D355EA47D55FAEC8C92A2ABDA980BC 40271 0 0 @//depot/codesamples/squery/squery.py@ @1.13@ 131072 
@pv@ 9 @db.rev@ @//depot/codesamples/squery/squery.py@ 1 131072 0 11 1615560173 1614326814 B72B933FC28A76969DB23DA8A219091A 46151 0 0 @//depot/codesamples/squery/squery.py@ @1.11@ 131072
```

It kind of looks like a .CSV file, but without column headers. P4D does not discriminate, each line is a record of some transaction, regardless of table, in order and as it occures. Therefore a proficient knowledge of the p4 schema is needed. Luckely, the knowledge is built-in to p4dlg, for any server/schema release.

## Create or load an existing connection.
```Python
# Use `jnlconnect` to manage connections.
# methods:    create - load - update - unload - destroy - purge
# `jnlconnect` attributes: create    create and store a connection to a journal
#                          load      load an existing connector
#                          update    update a connector's values
#                          unload    unload a connectior from the current scope
#                          destroy   destroy an existing connector
#                          purge     combines unload/destroy
#                          
# Create a connection to a journal with jnlconnect.create
# parameters: args[0]  -> name,
#             keyword  -> journal = journal_path
#             keyword  -> version = release of the p4d instance
#                                   that created the journal
#             keyword  -> oSchema = the schema that that defines the p4db
#
#             Note that keywords `version` & ` oSchema` are mutually exclusive.
#             Pass in one or the orther.

jnlconnect.create('jnl',
                  **{
                     'journal': './resc/journals/checkpoint.rasp',
                     'version': 'r16.2'
                     }
                 )
```
![shell_create_connection](https://github.com/user-attachments/assets/3e09eb1c-a933-496a-ba70-312535a693c0)

## Query syntax
```Python
#             table     op    value
#              |        |      |
#              V        V      V
qry = (oJnl.domain.type == 'client')
#       ^           ^
#       |           |
#    connector    column
```

## Building a query.
```Python
In [24]: qry = (jnl.domain.type == 'client')     # A simple query, the WHERE clause.
In [25]: qry
Out[25]: 
<DLGQuery {'inversion': False,                   # A query is a reference to class `DLGQuery`.
 'left': <JNLField type>,
 'objp4': <P4Jnl ./resc/journals/journal.8>,     
 'op': <function EQ at 0x104d32700>,
 'right': 'client'}>

In [26]: clients = jnl(qry).select()             # SELECT * FROM domain WHERE type = client
In [27]: clients
Out[27]: <DLGRecords (188)>                      # The target journal contains 188 `clientspec` records.

In [28]: clients.first()                         # `clients` is a reference to class `DLGRecords`, and
                                                 # exposes interesting SQL attributes.
Out[28]: 
<DLGRecord {'accessDate': '2021/11/14',          # `DLGRecords contains references to class `DLGRecord`
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

![shell_qry_clients](https://github.com/user-attachments/assets/20862973-bf37-46cd-92ca-b0805904c6bb)

## A connnector has useful attributes

![shell_jnl_tablenames](https://github.com/user-attachments/assets/bd8ceb9f-858b-4ab3-be9b-c416884bb880)
![shell_table_objects_and_fields](https://github.com/user-attachments/assets/519f8ea8-5b84-416c-97cf-5d95b94f3a9f)

## Agregators like *groupby*, *orderby*, *sortby*, *exclude*, *limitby*, *find*, etc.
![shell_records_groupby](https://github.com/user-attachments/assets/b6164493-1728-425a-ab7d-c0eb1e8c6c8c)

## 2. Py4
## 3. P4db
## 4. Rcs
#### A fun use case... create a list of records equivalent to the result of running  ```>>> p4 clients```


