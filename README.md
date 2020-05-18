# RustDB
Experimental db written in rust, based on example of [Chapter 7](https://livebook.manning.com/book/rust-in-action/chapter-7/) of the book Rust in Action.

Also, added ideas from Chapter 3 of the book [Designing Data-Intensive Applications](https://www.amazon.com.br/Designing-Data-Intensive-Applications-Reliable-Maintainable-ebook/dp/B06XPJML5D/ref=sr_1_1?__mk_pt_BR=%C3%85M%C3%85%C5%BD%C3%95%C3%91&crid=18FFFVLEM7FEH&keywords=design+data+intensive+applications&qid=1589769724&sprefix=design+data%2Caps%2C299&sr=8-1).

## Usage
For now, it exposes a rest api accepting json as input.

It accepts objects for put/post. It needs to have an id and at least one more field. An error 400 will be return with information regarding the problem.

For get/delete, it accepts strings, numbers or an object with and id.

After cloning this repo, you can run:

<pre><font color="#C3D82C"><b>➜  </b></font><font color="#00ACC1"><b>rustdb</b></font> <font color="#42A5F5"><b>git:(</b></font><font color="#FF5252"><b>master</b></font><font color="#42A5F5"><b>)</b></font> cargo run
<font color="#C3D82C"><b>    Finished</b></font> dev [unoptimized + debuginfo] target(s) in 0.01s
<font color="#C3D82C"><b>     Running</b></font> `target/debug/rustdb_rest`
Loading database...
Database ready at 7887
</pre>

As explicited here, the server will start listening to por 7887. It also creates a directory called `storage` inside application's folder. You can test the database inserting some data:

<pre><font color="#C3D82C"><b>➜  </b></font><font color="#00ACC1"><b>rustdb</b></font> <font color="#42A5F5"><b>git:(</b></font><font color="#FF5252"><b>master</b></font><font color="#42A5F5"><b>)</b></font> curl --request POST \
  --url http://localhost:7887/ \
  --header &apos;content-type: application/json&apos; \
  --data &apos;{
        &quot;id&quot;: &quot;1237&quot;,
        &quot;name&quot;: &quot;Lucas&quot;,
        &quot;email&quot;: &quot;lucas@test.com&quot;
}&apos;
</pre>

And you can request data with a GET request:

<pre><font color="#C3D82C"><b>➜  </b></font><font color="#00ACC1"><b>rustdb</b></font> <font color="#42A5F5"><b>git:(</b></font><font color="#FF5252"><b>master</b></font><font color="#42A5F5"><b>)</b></font> curl --request GET \
  --url http://localhost:7887/ \
  --header &apos;content-type: application/json&apos; \
  --data 1237
{&quot;email&quot;:&quot;lucas@test.com&quot;,&quot;id&quot;:&quot;1237&quot;,&quot;name&quot;:&quot;Lucas&quot;}<span style="background-color:#A1B0B8"><font color="#263238"><b>%</b></font></span>  </pre>

When you start the server, it creates a separate thread to compress log. It will garantee that database files will occupy the lowest possible number of log files that represents all data. This process runs each 5 seconds and will create and delete log files from storage folder.

# Understand db's structure

RustDB is a simple key/value storage with single collection and persisted data. The keys are kept in memory in a hash map. The value is stored in log files splited into data segments. Each time you request a key/value, it gets the file position from the hash map and load the value to return it.

The log file contains, for each register:
 - Checksum
 - key lenght
 - value length
 - key data
 - value data

By this way, we can garantee that the database will not delivery corrputed data. The data segments are filled in a append only way, allwing very fast inserts. When you update an registry, it creates a new entry in the end of the log file and the hash map value index is updated in memory.

Due to the nature of writes, log files grows fast with lots of old versions of each key. We break each file in 3MB chuncks in a struct called DataSegment. Besides of the record strucuture, each data segment log file contains its name in the first 8 bytes and a reference to the next segment in the following 8 bytes.

The storage directory contains a file called `initial_segment` that contains 8 bytes poiting to the first data segment. The name is a u64 value and is parsed into a `{:016x}` hex value to express the file names.

To deal with the always growing log files, we have a struct called `LogCompressor` that takes a list of segments and recreates a db without duplications. By this way, we can remove the old segments and change reference on `initial_segment` file. In thre rest_api implementation, we run this compression funciton each 5 seconds.

## Tests
RustDB has just few acceptance tests covering DataSegments, LogCompression and basic database opreations. All tests are executed using I/O, creating and deleting storage folders.

### Load tests
If you want to try some volume, you can use jmeter tests configured inside `jmeter` folder. It uses a csv inside `load_test`. Currently exists 2 tests. One is for writing and reading and a second one only reading data from the database.

<pre><font color="#C3D82C"><b>➜  </b></font><font color="#00ACC1"><b>jmeter</b></font> <font color="#42A5F5"><b>git:(</b></font><font color="#FF5252"><b>master</b></font><font color="#42A5F5"><b>)</b></font> jmeter -n -t test_read_write.jmx -l result01.jtl -e -o ./result01</pre>

<pre><font color="#FF5252"><b>➜  </b></font><font color="#00ACC1"><b>jmeter</b></font> <font color="#42A5F5"><b>git:(</b></font><font color="#FF5252"><b>master</b></font><font color="#42A5F5"><b>)</b></font> jmeter -n -t test_read.jmx -l result02.jtl -e -o ./result02</pre>
