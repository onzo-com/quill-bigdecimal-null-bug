package com.onzo.bugreport

import com.datastax.driver.core.{Cluster, Session}
import io.getquill.context.cassandra.Udt
import io.getquill.{CassandraAsyncContext, CassandraSyncContext, SnakeCase}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, FreeSpec}

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

    def queryATable(id: Int) = {
      import context._
      context.run {
        quote {
          for {
            row <- query[ATable]
            if row.id == lift(id)
          } yield row
        }
      }.futureValue
    }

    "should read a non-null value correctly" in {
      // given
      session.execute("INSERT INTO a_table (id, field) VALUES (0, {s: 'cake', n: 1.25})")

      // when
      val result = queryATable(0)

      // then
      assert(result == List(ATable(0, AType("cake", BigDecimal(1.25)))))
    }

    "should read a null text value as null, without throwing" in {
      // given
      session.execute("INSERT INTO a_table (id, field) VALUES (1, {s: null, n: 1.25})")

      // when
      val result = queryATable(1)

      // then
      assert(result == List(ATable(1, AType(null, BigDecimal(1.25)))))
    }

    "should read a null decimal value as null, without throwing" in {
      // given
      session.execute("INSERT INTO a_table (id, field) VALUES (2, {s: 'cake', n: null})")

      // when
      val result = queryATable(2) // fails:
      // “The future returned an exception of type: java.lang.IllegalArgumentException, with message: null value for BigDecimal.”

      // then
      assert(result == List(ATable(2, AType("cake", null))))
    }
  }

  "a Udt containing optional fields" - {
    case class BTable(id: Int, field: BType)
    case class BType(s: Option[String], n: Option[BigDecimal]) extends Udt

    def queryBTable(id: Int) = {
      import context._
      context.run {
        quote {
          for {
            row <- query[BTable]
            if row.id == lift(id)
          } yield row
        }
      }.futureValue
    }

    "should read a non-null value correctly" in {
      // given
      session.execute("INSERT INTO b_table (id, field) VALUES (0, {s: 'cake', n: 1.25})")

      // when
      val result = queryBTable(0)

      // then
      assert(result == List(BTable(0, BType(Some("cake"), Some(BigDecimal(1.25))))))
    }

    "should read a null text value as None, without throwing" in {
      // given
      session.execute("INSERT INTO b_table (id, field) VALUES (1, {s: null, n: 1.25})")

      // when
      val result = queryBTable(1)

      // then
      assert(result == List(BTable(1, BType(None, Some(BigDecimal(1.25))))))
    }

    "should read a null decimal value as None, without throwing" in {
      // given
      session.execute("INSERT INTO b_table (id, field) VALUES (2, {s: 'cake', n: null})")

      // when
      val result = queryBTable(2) // fails:
      // “The future returned an exception of type: java.lang.IllegalArgumentException, with message: null value for BigDecimal.”

      // then
      assert(result == List(BTable(2, BType(Some("cake"), None))))
    }
  }
}
