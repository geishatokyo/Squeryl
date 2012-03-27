package org.squeryl.sql

import org.squeryl.Session
import com.mysql.jdbc.PreparedStatement
import java.sql.{SQLException, ResultSet}
import org.squeryl.sharding._

/**
 * 
 * User: takeshita
 * Create: 11/09/22 13:22
 */

trait RawSQLSupport{

  def shardedSessionCache : ShardedSessionCache

  def execute[T](shardName : String)(func : DAO => T) : T = {
    val s = shardedSessionCache.getSession(shardName,ShardMode.Write)
    
    s.use()
    s.beginTransaction()

    var txOk = false
    try{
      val r = _using[T](s, () => {
        val connection = s.connection
        val dao = new DAO(connection)
        func(dao)
      })
      s.commitTransaction()
      if(s.safeClose){
        shardedSessionCache.removeSession(s)
      }
      txOk = true
      r
    }catch{
      case e : Exception => {
        _ignoreException(s.rollback())
        _ignoreException(s.forceClose())
        _ignoreException(shardedSessionCache.removeSession(s))
        throw e
      }
    }
  }


  @inline
  private def _ignoreException(func : => Unit) = {
    try{
      func
    }catch{
      case e : Exception => e.printStackTrace()
    }
  }

  private def _using[A](session: ShardedSession, a: ()=>A): A = {
    val s = Session.currentSessionOption
    try {
      if(s != None) s.get.unbindFromCurrentThread
      try {
        session.bindToCurrentThread
        val r = a()
        r
      }
      finally {
        session.unbindFromCurrentThread
        session.cleanup
      }
    }
    finally {
      if(s != None) s.get.bindToCurrentThread
    }
  }



  /**
   * Data Access Object
   */
  class DAO( con : java.sql.Connection){

    def exec(sql : String , params : Any*) : Int = {

      val ps = con.prepareStatement(sql)
      var index = 1
      for( p <- params){
        ps.setObject(index,p)
        index += 1
      }
      ps.executeUpdate()
    }

    def execBatch( sqls : String*) : Array[Int] = {
      val st = con.createStatement()

      sqls.foreach( sql => {
        st.addBatch( sql)
      })

      st.executeBatch()
    }

    def execQuery(sql : String, params : Any*) : ResultSet = {
      val ps = con.prepareStatement(sql)
      var index = 1
      for( p <- params){
        ps.setObject(index,p)
        index += 1
      }
      ps.executeQuery()
    }

    def execQuery[T](sql : String , rp : ResultSetProcessor[T] , prams : Any*) : List[T] = {
      var results = new scala.collection.mutable.LinkedList[T]
      val rs = execQuery(sql,prams:_*)
      rp.init(rs)
      while(rs.next){
        val o = rp.eachResult(rs)
        if(o.isDefined){
          results :+= o.get
        }
      }
      rp.done(rs)
      results.toList
    }

    def execQuery[T](sql : String, proc : ResultSet => T , params : Any*) : List[T] = {
      var results = new scala.collection.mutable.LinkedList[T]

      val rs = execQuery(sql,params:_*)
      while(rs.next){
        results :+= proc(rs)
      }
      results.toList
    }
  }


}

trait ResultSetProcessor[T]{

  def init(resultSet : ResultSet) : Unit = {}

  def eachResult(resultSet : ResultSet) : Option[T]

  def done(resultSet : ResultSet) : Unit = {}
}