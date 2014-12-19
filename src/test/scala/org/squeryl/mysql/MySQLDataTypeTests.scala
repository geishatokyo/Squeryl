package org.squeryl.mysql

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import org.squeryl.Schema
import org.squeryl.PrimitiveTypeMode._
import org.squeryl.SessionFactory
import org.squeryl._
import org.squeryl.adapters.MySQLAdapter
import org.squeryl.framework.DBConnector
import java.io._

object MySQLTestScheme extends Schema{

  val basicTypes = table[BasicTypes]

}

class BasicTypes extends KeyedEntity[Long]{
  var id : Long = 0
  var str : String = ""
  var i : Int = 0
  var time : java.util.Date = new java.util.Date
}

class MySQLDataTypeTests extends FlatSpec with ShouldMatchers with DBConnector {

  override def sessionCreator() : Option[() => AbstractSession] = {
    if(config.hasProps("mysql.connectionString")){
      Class.forName("com.mysql.jdbc.Driver")
      Some(() => {
        val c = java.sql.DriverManager.getConnection(
          config.getProp("mysql.connectionString"),
          config.getProp("mysql.user"),
          config.getProp("mysql.password")
        )
        c.setAutoCommit(false)
        Session.create(c, new MySQLAdapter)
      })
    }else{
      None
    }
  }

  SessionFactory.concreteFactory = sessionCreator()
  if(SessionFactory.concreteFactory.isDefined){
    transaction{
      MySQLTestScheme.drop
      MySQLTestScheme.create
    }

    "MySQL type" should "generate such types" in {
      transaction{
        val out = new ByteArrayOutputStream()
        val printer = new PrintWriter(out)
        MySQLTestScheme.printDdl(printer)
        printer.flush()
        val s = new String(out.toByteArray,"utf-8")
        println(s)

        s should include ("i int")
        s should include ("str varchar(128)")
        s should include ("time datetime")

      }
    }

    "Types" should "reserver type accuracy" in {
      val data = new BasicTypes()
      transaction{
        MySQLTestScheme.basicTypes.insert(data)
      }


      transaction{
        val read = MySQLTestScheme.basicTypes.lookup(data.id).get

        println(read)
        (data.time.getTime / 1000) should equal(read.time.getTime / 1000)


      }

    }
  }

}
