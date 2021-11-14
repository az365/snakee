# Snakee

### What is it 

Snakee is a free framework for unified data processing in analytical and scientific tasks.

This python library was created by combining 
[flux.streams](https://github.com/az365/flux/tree/master/streams) 
and [kagdata.series](https://github.com/az365/kagdata/tree/master/series) 
subpackages of [Flux](https://github.com/az365/flux) 
and [KagData](https://github.com/az365/kagdata) projects 
started at September 2019.

Snakee project code distributed [under MIT licence](https://github.com/az365/snakee/blob/main/LICENSE).

### Basic ideas

* Do not keep full dataset(s) in memory (by default);
* iteration over datasets on local disk, cloud storages, other sources;
* sort big arrays on disk, limit array consumption;
* allow custom data transformations (mappers, reducers);
* use SQL-like definition of operations over all data sources;
* uniformly manage connections for databases and other data sources.
 
### Generic abstractions 
 
* [context](base/context.md)
* [connectors](connectors/readme.md)
* [streams](streams/readme.md)
* items 
* fields 
* [functions](functions/readme.md)
