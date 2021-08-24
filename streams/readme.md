## Streams

### What is Stream

Data stream is the main abstraction in Snakee project, it's like Spark RDD.

Stream classes provides unified interface for realize applied code of data-transformations in functional paradigm.
Most methods of stream-class return an instance of same or other stream-class.

Inside any stream instance can be iterator over arbitrary items (Lines, Rows, Records, other objects) 
or any kind of array in memory (including Pandas dataframe or link to expected result of generated SQL-query).

### Stream classes hierarchy

* AbstractStream (
[interface](https://github.com/az365/snakee/blob/main/streams/interfaces/abstract_stream_interface.py), 
[implementation](https://github.com/az365/snakee/blob/main/streams/abstract/abstract_stream.py)
)
    * IterableStream (
    [interface](https://github.com/az365/snakee/blob/main/streams/interfaces/iterable_stream_interface.py), 
    [implementation](https://github.com/az365/snakee/blob/main/streams/abstract/iterable_stream.py)
    )
        * LocalStream (
        [interface](https://github.com/az365/snakee/blob/main/streams/interfaces/local_stream_interface.py), 
        [implementation](https://github.com/az365/snakee/blob/main/streams/abstract/local_stream.py)
        )
            * AnyStream
                * LineStream
                * RecordStream
                * RowStream
                * StructStream
                * KeyValueStream
    * WrapperStream
        * PandasStream
        * SqlStream

### General stream methods

* SQL-like methods
    * select(*fields, **expressions)
    * filter(*fields, *expr, **kv)
    * sort(*fields, **options)
    * group_by(*keys, values, **op)
    * join(stream, by, how)
    * map_side_join(stream, by, h)
* functional methods 
    * map(mapper)
    * flat_map(mapper)
    * apply_to_data(function)
    * apply_to_stream(function)
* structure methods 
    * concat(*streams)
    * split(field)
* extract methods
    * collect()
    * to_*()
    * get_*()

### Example

    my_stream = my_stream.filter(
        category='A',
        weight=fs.at_least(100),
    ).select(
        '*',
        density=('volume', 'weight', fs.div()),
    )
 
### See also 

* [functions](https://github.com/az365/snakee/blob/main/functions/readme.md)
* [general introduction](https://github.com/az365/snakee/blob/main/README.md)
