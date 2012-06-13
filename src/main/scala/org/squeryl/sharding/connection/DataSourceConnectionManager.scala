package org.squeryl.sharding.connection

import java.sql.Connection
import javax.sql.DataSource
import org.squeryl.sharding.{ShardMode, DatabaseConfig, ConnectionManager}
import scala.collection.mutable.{HashMap,ConcurrentMap}
import collection.JavaConversions._


/**
 *
 * User: takeshita
 * Create: 12/06/13 16:11
 */

trait DataSourceConnectionManager extends ConnectionManager {
  val dataSource : ConcurrentMap[DatabaseConfig,DataSource] =
    new java.util.concurrent.ConcurrentHashMap[DatabaseConfig,DataSource]()

  def connection(shardName: String, mode: ShardMode.Value, config: DatabaseConfig): Connection = {
    val ds = dataSource.get(config) match{
      case Some(dataSource) => dataSource
      case None => {
        config.synchronized{
          if(!dataSource.containsKey(config)){
            dataSource += config -> createDataSource(config)
          }
        }
        dataSource(config)
      }
    }
    ds.getConnection()
  }

  def createDataSource(config : DatabaseConfig) : DataSource
}
