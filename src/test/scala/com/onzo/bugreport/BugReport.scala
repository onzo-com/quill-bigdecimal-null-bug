package com.onzo.bugreport

import com.datastax.driver.core.{Cluster, Session}
import io.getquill.{CassandraSyncContext, SnakeCase}
import org.scalatest.{BeforeAndAfterAll, FreeSpec}

class BugReport extends FreeSpec with BeforeAndAfterAll {

  val host = sys.env.getOrElse("CASSANDRA_HOST", "localhost")
  val port = sys.env.getOrElse("CASSANDRA_PORT", "9042").toInt
  val keyspace = sys.env.getOrElse("CASSANDRA_KEYSPACE", "comonzobugreport")

  lazy val cluster: Cluster = Cluster.builder().addContactPoint(host).withPort(port).build()
  lazy val session: Session = cluster.connect()
  lazy val context = new CassandraSyncContext(SnakeCase, cluster, keyspace, preparedStatementCacheSize = 10)

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
}
