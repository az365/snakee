# Connectors

### What is Connector

Connector is a wrapper-like object with quite similar interface 
for unified access to any stored data objects 
(tables in databases, files on disk or in object storage)
in [Snakee](https://github.com/az365/snakee/blob/main/README.md).

#### What does Connector object contain
- connection settings:
    - addresses,
    - credentials,
    - parameters,
- state information:
    - opened or closed,
    - existing or estimated,
- meta information:
    - structure of table (field names and types),
    - count of rows (items).

#### What it can to do
- store link to data source and its connection settings
- manage connection and its state (open, close, reset)
- provide iterator and 
  [Stream](https://github.com/az365/snakee/blob/main/streams/readme.md) object 
  with data from source

## Hierarchy
Unlike flat set of [streams](https://github.com/az365/snakee/blob/main/streams/readme.md) 
connectors form hierarchic structure. 
E.g. storage.folder.file or database.table.

It should be distinguished hierarchy of objects (storage.folder.file) 
and hierarchy of subclasses 
(AbstractConnector>HierarchicConnector>AbstractStorage>LocalStorage). 

#### Hierarchy of objects in applied project 
- [Context](https://github.com/az365/snakee/blob/main/base/context.md) 
  (common root singleton object for any applied project)
    - Database 
        - Table
    - S3Storage
        - S3Bucket
            - S3Object
    - LocalStorage
        - LocalFolder 
            - LocalFile

#### Hierarchy of Connector (sub)classes
- ConnectorInterface
    - AbstractConnector
        - HierarchicConnector 
        is a branch in tree of objects hierarchy
        (hierarchic connector can contain some connectors as child items)
            - AbstractDatabase
              - PostgresDatabase
              - ClickhouseDatabase
            - AbstractStorage
              - S3Storage (AWS or YandexCloud S3 object storage)
              - LocalStorage (local filesystem)
            - AbstractFolder
              - S3Bucket
              - LocalFolder
        - LeafConnector 
        is a leaf node of objects hierarchy 
            - Table
            - S3Object
            - File
                - TextFile
                    - JsonFile
                    - ColumnFile
                        - TsvFile
                        - CsvFile

## General connector methods
Common for HierarchicConnector and LeafConnector:
- get_path()
- get_data() 

#### HierarchicConnector
- get_child(name)
- get_children()
- list_existing_names()

#### LeafConnector
- is_existing()
- is_opened()
- open(), close()
- get_count()
- get_struct()
- write_stream()
- to_stream()

Also LeafConnector inherits StreamInterface, so it has appropriate methods:
- map(mapper)
- flat_map(mapper)
- select(*fields, **expressions)
- filter(*fields, *expr, **kv)
- sort(*fields, **options)
- group_by(*keys, values, **op)
- join(stream, by, how)

and so on.

Therefore any instance of LeafConnector can be used as initial 
[Stream](https://github.com/az365/snakee/blob/main/streams/readme.md) object
