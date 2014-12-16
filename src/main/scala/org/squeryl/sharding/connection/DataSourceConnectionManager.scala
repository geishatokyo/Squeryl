package org.squeryl.sharding.connection

import java.sql.Connection
import javax.sql.DataSource

import org.squeryl.sharding.{ConnectionManager, DatabaseConfig, ShardMode}

import scala.collection.JavaConversions._
import scala.collection.concurrent.{Map => ConcurrentMap}


/**
 *
 * User: takeshita
 * Create: 12/06/13 16:11
 */

trait DataSourceConnectionManager extends ConnectionManager {
  val dataSource : ConcurrentMap[DatabaseConfig,DataSource] =
    new java.util.concurrent.ConcurrentHashMap[DatabaseConfig,DataSource]()

  def connection(shardName: String, mode: ShardMode.Value, config: DatabaseConfig): Connection = {
    val ds = dataSource.getOrElse(config, {
      config.synchronized {
        if (!dataSource.containsKey(config)) {
          dataSource += config -> createDataSource(config)
        }
      }
      dataSource(config)
    })
    ds.getConnection
  }

  def createDataSource(config : DatabaseConfig) : DataSource
}
