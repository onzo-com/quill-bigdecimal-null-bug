# Test cases for https://github.com/getquill/quill/issues/1665

- **Version**: 3.4.9
- **Module**: quill-cassandra
- **Database**: Cassandra (I have version 3.11.4 locally, but I don't believe the error is on Cassandra's side)
- **See also**:
    - https://github.com/getquill/quill/issues/535 talks about `NullPointerException`s for custom types
        (this issue concerns `IllegalArgumentException`s (albeit basically `NullPointerException`s) for a built-in type)
    - https://github.com/onzo-com/quill-bigdecimal-null-bug contains test cases that trigger this bug

[repo]: https://github.com/onzo-com/quill-bigdecimal-null-bug

## Expected Behaviour

All test cases in [this repo][repo] pass.

```
[info] BugReport:
[info] a Udt containing compulsory fields
[info] - should read a non-null value correctly
[info] - should read a null text value as null, without throwing
[info] - should read a null decimal value as null, without throwing
[info] a Udt containing optional fields
[info] - should read a non-null value correctly
[info] - should read a null text value as None, without throwing
[info] - should read a null decimal value as None, without throwing
```

## Actual Behaviour

Two test cases in [this repo][repo] fail.

```
[info] BugReport:
[info] a Udt containing compulsory fields
[info] - should read a non-null value correctly
[info] - should read a null text value as null, without throwing
[info] - should read a null decimal value as null, without throwing *** FAILED ***
[info]   The future returned an exception of type: java.lang.IllegalArgumentException, with message: null value for BigDecimal. (BugReport.scala:58)
[info] a Udt containing optional fields
[info] - should read a non-null value correctly
[info] - should read a null text value as None, without throwing
[info] - should read a null decimal value as None, without throwing *** FAILED ***
[info]   The future returned an exception of type: java.lang.IllegalArgumentException, with message: null value for BigDecimal. (BugReport.scala:109)
```

## Running the tests

Set up environment variables:

- `QUILL_CASSANDRA_VERSION` — version of `quill-cassandra` to test against, defaults to `3.4.9`
- `CASSANDRA_HOST` — host to connect to Cassandra on, defaults to `localhost`
- `CASSANDRA_PORT` — post to connect to Cassandra on, defaults to `9042`
- `CASSANDRA_KEYSPACE` — keyspace to run the tests in, defaults to `comonzobugreport`; you will lose all data in this keyspace

Then run `sbt test`.

## Workaround

There is no general workaround.

## Fix

Applying this diff to the quill repository, publishing locally, then referencing the snapshot version when running the tests,
is enough to make the tests in [this repository][repo] pass.

```
--- a/quill-cassandra/src/main/scala/io/getquill/context/cassandra/encoding/CassandraTypes.scala
+++ b/quill-cassandra/src/main/scala/io/getquill/context/cassandra/encoding/CassandraTypes.scala
@@ -56,7 +56,10 @@ trait CassandraMappedTypes {
   implicit val decodeBoolean: CassandraMapper[JBoolean, Boolean] = CassandraMapper(Boolean2boolean)

   implicit val encodeBigDecimal: CassandraMapper[BigDecimal, JBigDecimal] = CassandraMapper(_.bigDecimal)
-  implicit val decodeBigDecimal: CassandraMapper[JBigDecimal, BigDecimal] = CassandraMapper(BigDecimal.apply)
+  implicit val decodeBigDecimal: CassandraMapper[JBigDecimal, BigDecimal] = CassandraMapper {
+    case null        => null
+    case jBigDecimal => BigDecimal(jBigDecimal)
+  }

   implicit val encodeByteArray: CassandraMapper[Array[Byte], ByteBuffer] = CassandraMapper(ByteBuffer.wrap)
   implicit val decodeByteArray: CassandraMapper[ByteBuffer, Array[Byte]] = CassandraMapper(bb => {
```

Although this is enough for my immediate use case, a complete fix may also need to consider
- null-checking in decoders for other types
- null-checking in encoders
