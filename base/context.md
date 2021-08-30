# Context

### What is Context

Context is a common root singleton object 
for any applied project 
using [Snakee](https://github.com/az365/snakee/blob/main/README.md).

SnakeeContext object is a connecting link for classes in library and objects in project.

It's quite similar to Spark Context.

However SnakeeContext is not mandatory object for your project,  
you can use available classes directly without context.

### What it doing

- remember links to all 
[streams](https://github.com/az365/snakee/blob/main/streams/readme.md), 
[connectors](https://github.com/az365/snakee/blob/main/connectors/readme.md), 
loggers, 
defined in current session;
- let them contact with each other;
- implement Singleton pattern
(there is only one accessible instance of Context within the applied session).

### Usage

Usually in applied code it's enough to import Context only,
but it's indispensably to initialize it
('cx' is a common recommended name for SnakeeContext instance):

    from snakee.context import SnakeeContext
    
    cx = SnakeeContext()

After initializing it you can extract (almost) either required object from Context instance (cx):

    my_tsv = cx.get_job_folder().file(
        'my_file.tsv',
    )
    
    my_db = cx.ct.PostgresDatabase(
        name='my_db',
        host='localhost',
        port=8123,
        db='public',
    )
    
    logger = cx.get_logger()
