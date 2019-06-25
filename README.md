# Simple-DB

Simple experiment with a [parquet](https://parquet.apache.org/) based data-store.

## Build:

I've checked in a compiled binary for OSX which should work on most macs but a simple
```bash
go build
```
should yield you a functioning executable on whatever system you're using

## Usage:
Import the sample data from the assets directory:
```bash
./simple-db import 
```
Query to filter by title:
```bash
./simple-db -f title="the matrix"
```
Query to filter by title and order by view_time:
```bash
./simple-db -f title="the matrix" -o view_time
```

## Considerations
* I chose parquet because I wanted to learn more about it, but realized probably too far into this challenge that parquet is an immutable file-format. This means that any time we need to update/upsert a row - we are rewriting the entire database. We are not reading the entire file in at once when replicating, so we shouldn't blow the stack, but I think that the [ORC file format](https://orc.apache.org/) would probably be a much better choice here and would come with some [better guarantees](https://orc.apache.org/docs/acid.html) (this implementation would be a nightmare in a distributed system)

* I used a simple [lmdb inspired k/v store](https://github.com/boltdb/bolt) for indexing parquet row offsets - this is used for keeping track of data and upserting the file. This is not the best solution here, but it was a quick way to get things up and running. 

* I am not taking advantage of parquets column based access here and that is purely because I ran out of time. I would have loved to utilize it to implement the SELECT queries but I just didn't get there.

* I would have liked to further develop the query layer. I typically would approach something like this using an AST to define the nodes and used a recursive pattern to crawl the query tree and build out the result set in stages. Because I ran out of time and the challenge suggested not worrying about memory here I just read the entire data-set and do in-memory sorting and filtering. I think using a combination of an intelligent AST (applying filters first etc.) and indexing and would yield much better performance. 
