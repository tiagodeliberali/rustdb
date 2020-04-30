# rustdb
Experimental db written in rust, based on example of [Chapter 7](https://livebook.manning.com/book/rust-in-action/chapter-7/) of the book Rust in Action.

## Usage
For now, it exposes a rest api accepting json as input.

For fun, it accepts objects for put/post. It needs to have an id and at least one more field. An error 400 will be return with information regarding the problem.

For get/delete, it accepts strings, numbres or an object with and id.

<pre><font color="#A6E22E"><b>➜  </b></font><font color="#A1EFE4"><b>~</b></font> curl -X POST http://localhost:7887 --data &apos;{&quot;id&quot;:1345, &quot;data&quot;: 123}&apos;
</pre>
