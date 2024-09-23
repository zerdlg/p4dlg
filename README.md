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
# 3 parameters: args[0]  -> name, journal_path, release
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

![shell_qry_clients](https://github.com/user-attachments/assets/20862973-bf37-46cd-92ca-b0805904c6bb)


## A connnector has useful attributes

![shell_jnl_tablenames](https://github.com/user-attachments/assets/bd8ceb9f-858b-4ab3-be9b-c416884bb880)


![shell_table_objects_and_fields](https://github.com/user-attachments/assets/519f8ea8-5b84-416c-97cf-5d95b94f3a9f)

## Agregators like *groupby*, *orderby*, *sortby*, *exclude*, *limitby*, *find*, etc.

```Python
In [9]: clients = oJnl(qry).select()

# group clients by client name and orderby db_action (pv, dv, ...)
In  [10]: clients_by_name = clients.groupby('name', orderby='db_action', as_recordlist=False)
In  [11]: clients_by_name
{'mart_macos': <DLGRecords (2)>,
 'p4vtest': <DLGRecords (23)>,
 'pycharmclient': <DLGRecords (1)>,
 'lpycharmclient': <DLGRecords (32)>,
 'linuxclient': <DLGRecords (4)>,
 'lxcharlotte_pycharm': <DLGRecords (10)>,
 'source': <DLGRecords (1)>,
 'mscharlotte': <DLGRecords (3)>,
 'upycharmclient': <DLGRecords (13)>,
 'test': <DLGRecords (3)>,
 'bsclient': <DLGRecords (2)>,
 'martclient': <DLGRecords (1)>,
 'anastasia': <DLGRecords (9)>,
 'gc_pycharm': <DLGRecords (36)>,
 'computer_depot': <DLGRecords (29)>,
 'miscclient': <DLGRecords (1)>,
 'projclient': <DLGRecords (8)>,
 'gc_local': <DLGRecords (4)>,
 'gc_charlotte': <DLGRecords (1)>,
 'gc_fred': <DLGRecords (3)>,
 'uxcharlotte_pycharm': <DLGRecords (1)>}

In  [12]: clients_by_name.gc_fred
<DLGRecords (3)>

In  [13]: for record in clients_by_name.gc_fred:
          ... print(record.db_action, record.accessDate)
pv 2021/11/17
rv 2021/11/17
rv 2021/11/17

# NOTE: setting the `as_recordlist' argument to True (the default) will return a DLGRecords object containg the grouped records, otherwise as above (a dict).


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
                                                #           {'my_client': [DLGRecords],
                                                #            'other_client': [DLGRecords],
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


