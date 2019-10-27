package com.onzo.bugreport

import com.datastax.driver.core.{Cluster, Session}
import io.getquill.context.cassandra.Udt
import io.getquill.{CassandraAsyncContext, SnakeCase}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec}

import scala.collection.JavaConverters._

class BugReport extends FreeSpec with BeforeAndAfterAll with ScalaFutures {

  implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val host = sys.env.getOrElse("CASSANDRA_HOST", "localhost")
  val port = sys.env.getOrElse("CASSANDRA_PORT", "9042").toInt
  val keyspace = sys.env.getOrElse("CASSANDRA_KEYSPACE", "comonzobugreport")

  lazy val cluster: Cluster = Cluster.builder().addContactPoint(host).withPort(port).build()
  lazy val session: Session = cluster.connect()
  lazy val context = new CassandraAsyncContext(SnakeCase, cluster, keyspace, preparedStatementCacheSize = 10)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    Seq(
      s"DROP KEYSPACE IF EXISTS $keyspace",
      s"CREATE KEYSPACE $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
      s"USE $keyspace",
      "CREATE TYPE a_type (s text, n decimal)",
      "CREATE TYPE b_type (s text, n decimal)",
      "CREATE TABLE a_table (id int, field a_type, PRIMARY KEY (id))",
      "CREATE TABLE b_table (id int, field b_type, PRIMARY KEY (id))",
    ).foreach(session.execute)
  }

  override protected def afterAll(): Unit = {
    try {
      context.close()
      session.close()
      cluster.close()
    }
    finally {
      super.afterAll()
    }
  }

  "a Udt containing compulsory fields" - {
    case class ATable(id: Int, field: AType)
    case class AType(s: String, n: BigDecimal) extends Udt

    val schema: context.Quoted[context.EntityQuery[ATable]] = {
      import context._
      quote(query[ATable])
    }

    "should read" - {
      def queryATable(id: Int) = {
        import context._
        context.run {
          quote {
            for {
              row <- schema
              if row.id == lift(id)
            } yield row
          }
        }.futureValue
      }

      "a non-null value correctly" in {
        // given
        session.execute("INSERT INTO a_table (id, field) VALUES (0, {s: 'cake', n: 1.25})")

        // when
        val result = queryATable(0)

        // then
        assert(result == List(ATable(0, AType("cake", BigDecimal(1.25)))))
      }

      "a null text value as null, without throwing" in {
        // given
        session.execute("INSERT INTO a_table (id, field) VALUES (1, {s: null, n: 1.25})")

        // when
        val result = queryATable(1)

        // then
        assert(result == List(ATable(1, AType(null, BigDecimal(1.25)))))
      }

      "a null decimal value as null, without throwing" in {
        // given
        session.execute("INSERT INTO a_table (id, field) VALUES (2, {s: 'cake', n: null})")

        // when
        val result = queryATable(2) // fails:
        // “The future returned an exception of type: java.lang.IllegalArgumentException, with message: null value for BigDecimal.”

        // then
        assert(result == List(ATable(2, AType("cake", null))))
      }
    }

    "should write" - {
      def insertIntoATable(row: ATable) = {
        import context._
        context.run {
          schema.insert(lift(row))
        }.futureValue
      }

      "a non-null value correctly" in {
        // when
        insertIntoATable(ATable(100, AType("cake", BigDecimal(1.25))))

        // then
        val resultSet = session.execute("SELECT * FROM a_table WHERE id = 100")
        val rows = resultSet.all().asScala
        assert(rows.length == 1, "one row should be returned")
        val row = rows.head
        val field = row.getUDTValue("field")
        assert(field.getString("s") == "cake")
        assert(BigDecimal(field.getDecimal("n")) == BigDecimal(1.25))
      }

      "a null text value as null, without throwing" in {
        // when
        insertIntoATable(ATable(101, AType(null, BigDecimal(1.25))))

        // then
        val resultSet = session.execute("SELECT * FROM a_table WHERE id = 101")
        val rows = resultSet.all().asScala
        assert(rows.length == 1, "one row should be returned")
        val row = rows.head
        val field = row.getUDTValue("field")
        assert(field.getString("s") == null)
        assert(BigDecimal(field.getDecimal("n")) == BigDecimal(1.25))
      }

      "a null decimal value as null, without throwing" in {
        // when
        insertIntoATable(ATable(102, AType("cake", null)))
        // The future returned an exception of type: java.lang.NullPointerException.

        // then
        val resultSet = session.execute("SELECT * FROM a_table WHERE id = 102")
        val rows = resultSet.all().asScala
        assert(rows.length == 1, "one row should be returned")
        val row = rows.head
        val field = row.getUDTValue("field")
        assert(field.getString("s") == "cake")
        assert(field.getDecimal("n") == null)
      }
    }
  }

  "a Udt containing optional fields" - {
    case class BTable(id: Int, field: BType)
    case class BType(s: Option[String], n: Option[BigDecimal]) extends Udt

    val schema: context.Quoted[context.EntityQuery[BTable]] = {
      import context._
      quote(query[BTable])
    }

    "should read" - {
      def queryBTable(id: Int) = {
        import context._
        context.run {
          quote {
            for {
              row <- schema
              if row.id == lift(id)
            } yield row
          }
        }.futureValue
      }

      "a non-null value correctly" in {
        // given
        session.execute("INSERT INTO b_table (id, field) VALUES (0, {s: 'cake', n: 1.25})")

        // when
        val result = queryBTable(0)

        // then
        assert(result == List(BTable(0, BType(Some("cake"), Some(BigDecimal(1.25))))))
      }

      "a null text value as None, without throwing" in {
        // given
        session.execute("INSERT INTO b_table (id, field) VALUES (1, {s: null, n: 1.25})")

        // when
        val result = queryBTable(1)

        // then
        assert(result == List(BTable(1, BType(None, Some(BigDecimal(1.25))))))
      }

      "a null decimal value as None, without throwing" in {
        // given
        session.execute("INSERT INTO b_table (id, field) VALUES (2, {s: 'cake', n: null})")

        // when
        val result = queryBTable(2) // fails:
        // “The future returned an exception of type: java.lang.IllegalArgumentException, with message: null value for BigDecimal.”

        // then
        assert(result == List(BTable(2, BType(Some("cake"), None))))
      }
    }

    "should write" - {
      def insertIntoBTable(row: BTable) = {
        import context._
        context.run {
          schema.insert(lift(row))
        }.futureValue
      }

      "a non-None value correctly" in {
        // when
        insertIntoBTable(BTable(100, BType(Some("cake"), Some(BigDecimal(1.25)))))

        // then
        val resultSet = session.execute("SELECT * FROM b_table WHERE id = 100")
        val rows = resultSet.all().asScala
        assert(rows.length == 1, "one row should be returned")
        val row = rows.head
        val field = row.getUDTValue("field")
        assert(field.getString("s") == "cake")
        assert(BigDecimal(field.getDecimal("n")) == BigDecimal(1.25))
      }

      "a None text value as null, without throwing" in {
        // when
        insertIntoBTable(BTable(101, BType(None, Some(BigDecimal(1.25)))))

        // then
        val resultSet = session.execute("SELECT * FROM b_table WHERE id = 101")
        val rows = resultSet.all().asScala
        assert(rows.length == 1, "one row should be returned")
        val row = rows.head
        val field = row.getUDTValue("field")
        assert(field.getString("s") == null)
        assert(BigDecimal(field.getDecimal("n")) == BigDecimal(1.25))
      }

      "a None decimal value as null, without throwing" in {
        // when
        insertIntoBTable(BTable(102, BType(Some("cake"), None)))
        // The future returned an exception of type: java.lang.NullPointerException.

        // then
        val resultSet = session.execute("SELECT * FROM b_table WHERE id = 102")
        val rows = resultSet.all().asScala
        assert(rows.length == 1, "one row should be returned")
        val row = rows.head
        val field = row.getUDTValue("field")
        assert(field.getString("s") == "cake")
        assert(field.getDecimal("n") == null)
      }
    }
  }
}
