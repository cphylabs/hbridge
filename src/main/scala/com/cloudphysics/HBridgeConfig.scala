package com.cloudphysics.data

import org.apache.hadoop.hbase.client.HTablePool
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

object HDataType extends Enumeration {
  type DataType = Value
  val dByteSeq, dByte, dInt, dLong, dFloat, dDouble, dString, dBoolean, dTime = Value
}

class HBridgeConfig(
  val configuration: Option[Configuration],
  val htablePool: Option[HTablePool])

object HBridgeConfig {
  private val POOL_SIZE: Int = 100
  private var htablePool: Option[HTablePool] = None

  def apply(conf : Configuration) = {
    htablePool = Option(new HTablePool(conf, POOL_SIZE))
    new HBridgeConfig(
          Option(conf), htablePool)
  }

  def apply(hbaseZookeeperQuorum: String,
    hbaseZookeeperClientPort: String,
    hbaseWriteBufferSize: String,
    hbaseMaster: String) = {

    val conf: Configuration = HBaseConfiguration.create()
    conf.clear()
    conf.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum)
    conf.set("hbase.zookeeper.property.clientPort", hbaseZookeeperClientPort)
    conf.set("hbase.master", hbaseMaster)
    conf.set("hbase.client.write.buffer", hbaseWriteBufferSize)
    htablePool = Option(new HTablePool(conf, POOL_SIZE))
    new HBridgeConfig(
      Option(conf), htablePool)
  }
}

