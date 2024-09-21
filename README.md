# dlg

### A set of *abstractions* that use SQL, supported by a common set of APIs, to interact with your Perforce instance and its resources. 

While I work on formal documentation, this README should at least give you an idea of what *P4q* is. There are also working samples and examples in /p4q/libsample.

### All abstractions, whatever they support, adhere to the same syntax, conventions and functionality, generally. The following example can be applied, conceptually, to any abstraction.

## Where?
Though P4Q can be imported and used in script or broader programs (which was the initial intention), you can also use it interactively by launching an IPython QTConsole where P4Q is baked into it. To open the interactive shell, simply open a command prompt and type the following command line:

```Python
%> python p4qmain.py shell
```

![image](https://github.com/user-attachments/assets/940c69f3-77cd-4282-99d0-3d95c9fbcea7)

* Please look into the /p4q/libsample directory for a more detailed examples.

# P4Jnl
### Access all your metadata without being hindered by proprietary hurdles and without any perforce client program or admin program.

A checkpoint is a snapshot, a textual representation of your Perforce DB. As records are created, the server outputs the data to an ongoing journal (which generally gets truncated based on the requirements set by a admin). In other words, your collection of journals should be pretty much equivalent to a freshly outputted checkpoint (apart from the delta between the 2). Conversely, you can rebuild your database by inputting (importing) the same checkpoint, or a modified checkpoint (and/or journals). They can be used to insert, update and delete records. Throughout this README, ```checkpoint``` and ```journal``` are used interchangeably. Enough said. 

### A sample journal fragment.

```Python
@pv@ 7 @db.user@ @bigbird@ @bigbird@@pycharmclient@ @@ 1624456923 1624456923 @bigbird@ @@ 2 @@ 0 0 0 0 0 0
@pv@ 6 @db.domain@ @localclient@ 99 @raspberrypi@ @/home/pi@ @@ @@ @mart@ 1615718544 1618412757 0 @Created by bigbird.\n@
@pv@ 9 @db.rev@ @//depot/codesamples/squery/squery.py@ 2 131072 1 13 1618413823 1618413704 47D355EA47D55FAEC8C92A2ABDA980BC 40271 0 0 @//depot/codesamples/squery/squery.py@ @1.13@ 131072 
@pv@ 9 @db.rev@ @//depot/codesamples/squery/squery.py@ 1 131072 0 11 1615560173 1614326814 B72B933FC28A76969DB23DA8A219091A 46151 0 0 @//depot/codesamples/squery/squery.py@ @1.11@ 131072
```

It kind of looks like a .CSV file, but without column headers. P4D does not discriminate, each line is a record of some transaction, in order, as it occures and belonging to any of P4D's supported tables. So, to successfuly query journal records, we must rely on close familiarity to the p4 schema. As you might have guessed, P4Q has intimate knowledge of p4 schemas, belonging to any release.

## Connect
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
# 3 parameters: args[0]  -> name, journal_path, release

In [1]: jnlconnect.create('oJnl', './resc/journals/checkpoint.rasp', version='r16.2')
Reference (jnl3) created

# once created, a connector can be reloaded at anytime with `jnlconnect.load`
# 1 paramter: name of the connection

In [1]: jnlconnect.load('oJnl')
Out[1]: <P4Jnl /Users/gc/anastasia/dev/p4q/resc/journals/checkpoint.rasp>
```
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
In [2]: qry = (oJnl.domain.type == 'client')           # select all columns from table `db.domain` where column
                                                       # `type` is 'client' (or 99)

In [3]: qry                                            # a query is an object
Out[3]:
<P4Query {'inversion': False,
 'left': <JNLField type>,
 'objp4': <P4Jnl /Users/gc/anastasia/dev/p4q/resc/journals/checkpoint.rasp>,
 'op': <function EQ at 0x107fa1da0>,
 'right': 'client'}>

In [3]: clients = oJnl(qry).select()                   # the journal connector is callable, and takes a query
In [4]: clients                                        # select() returns a P4QRecords object
Out[4]: <P4QRecords (6)>                               

In [5]: clients.first()                                # `first` record
Out[5]: 
<P4QRecord {'accessDate': '2021/03/12',                # each record in a P4QRecords object is a P4QRecord
 'db_action': 'pv',
 'description': 'Created by mart.\n',
 'extra': '',
 'idx': 59,
 'mount': '/Users/mart/anyschema_2db',
 'mount2': '',
 'mount3': '',
 'name': 'anyschema',
 'options': '4096',
 'owner': 'mart',
 'partition': '0',
 'serverid': '',
 'stream': '',
 'table_name': 'db.domain',
 'table_revision': '6',
 'type': '99',
 'updateDate': '2021/03/12'}>
```


## A connnector has useful attributes
```Python
# get a list of tablenames available in this version of the schema
In  [5]: oJnl.tablenames
Out [5]:
['archmap',
 'change',
 'config',
 'counters',
 'depot',
..., ]

# Each table is an object
In  [6]: oJnl.rev
Out [6]: <libjnl.jnlSqltypes.JNLTable at 0x1659d8fd0>

# each table has interesting attributes and knows everything about the fields it supports.
In  [7]: oJnl.rev.fields
Out [7]:
{'idx': <JNLField idx>,
 'db_action': <JNLField db_action>,
 'table_revision': <JNLField table_revision>,
 'table_name': <JNLField table_name>,
 'depotFile': <JNLField depotFile>,
 'depotRev': <JNLField depotRev>,
 'type': <JNLField type>,
 'action': <JNLField action>,
 'change': <JNLField change>,
 'date': <JNLField date>,
 'modTime': <JNLField modTime>,
 'digest': <JNLField digest>,
 'size': <JNLField size>,
 'traitLot': <JNLField traitLot>,
 'lbrIsLazy': <JNLField lbrIsLazy>,
 'lbrFile': <JNLField lbrFile>,
 'lbrRev': <JNLField lbrRev>,
 'lbrType': <JNLField lbrType>}

# get the p4 type of a field
In  [8]: oJnl.rev.depotFile.type
Out [8]: 'File'
```

## Agregators like *groupby*, *orderby*, *sortby*, *exclude*, *limitby*, *find*, etc.
```Python
In [9]: clients = oJnl(qry).select()

# group clients by client name and orderby db_action (pv, dv, ...)
In  [10]: clients_by_name = clients.groupby('name', orderby='db_action', as_recordlist=False)
In  [11]: clients_by_name
{'mart_macos': <P4QRecords (2)>,
 'p4vtest': <P4QRecords (23)>,
 'pycharmclient': <P4QRecords (1)>,
 'lpycharmclient': <P4QRecords (32)>,
 'linuxclient': <P4QRecords (4)>,
 'lxcharlotte_pycharm': <P4QRecords (10)>,
 'source': <P4QRecords (1)>,
 'mscharlotte': <P4QRecords (3)>,
 'upycharmclient': <P4QRecords (13)>,
 'test': <P4QRecords (3)>,
 'bsclient': <P4QRecords (2)>,
 'martclient': <P4QRecords (1)>,
 'anastasia': <P4QRecords (9)>,
 'gc_pycharm': <P4QRecords (36)>,
 'computer_depot': <P4QRecords (29)>,
 'miscclient': <P4QRecords (1)>,
 'projclient': <P4QRecords (8)>,
 'gc_local': <P4QRecords (4)>,
 'gc_charlotte': <P4QRecords (1)>,
 'gc_fred': <P4QRecords (3)>,
 'uxcharlotte_pycharm': <P4QRecords (1)>}

In  [12]: clients_by_name.gc_fred
<P4QRecords (3)>

In  [13]: for record in clients_by_name.gc_fred:
          ... print(record.db_action, record.accessDate)
pv 2021/11/17
rv 2021/11/17
rv 2021/11/17

# NOTE: setting the `as_recordlist' argument to True (the default) will return a P4QRecords object containg the grouped records, otherwise as above (a dict).


```

## 2. Py4
## 3. P4db
## 4. Rcs


#### A fun use case... create a list of records equivalent to the result of running  ```>>> p4 clients```
```Python
def clients():
    qry = (jnl.domain.type == 'client')         # a qry to select all client domain records
    
    clients = jnl(qry).select()                 # select the target records
    
    clientgroups = clients.groupby(             # group records by client name
        'name',                                 # domain field to groupby is 'name'
        orderby='accessDate',                   # order each group of records by 'accessDate'
        groupdict=True                          # instructs the groupby() method to return a dict, instead of a set of records,
                                                # where each key/value pair consists of clientname/relevent_domain_records
                                                
                                                #     i.e.: 
                                                #           {'my_client': [P4QRecords],
                                                #            'other_client': [P4QRecords],
                                                #            ... } 
    )
    
    return [                                    # Everytime a user accesses a client spec (i.e. 
                                                # >>> p4 client -o my_client) p4d creates a 
                                                # record, so the very last record of each group will do.
                                                
            records.last() for (name, records) in clientgroups.items()
        ]
```

```
>>> clients()

```

## Py4

```Python
In [5]: p4connect.load('p4')
Reference (p4) loaded & connected to anastasia.local:1777
Out[6]: <Py4 anastasia.local:1777 >
```
... (TODO)


