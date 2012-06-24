package com.cloudphysics

import reflect.BeanProperty

object DataType extends Enumeration {
  type DataType = Value
  val dByteSeq, dByte, dInt, dLong, dFloat, dDouble, dString, dBoolean, dTime = Value
}

case class HBridgeConfig(hbaseZookeeperQuorum: String,
  hbaseZookeeperClientPort: String,
  hbaseWriteBufferSize: String,
  hbaseMaster: String,
  hbasetable: String,
  hbaseColumFamily: String)


class InventoryConfig {

  @BeanProperty var hbasetable: String = null
  @BeanProperty var hbaseColumFamily: String = null
  @BeanProperty var hbaseZookeeperQuorum: String = null
  @BeanProperty var hbaseZookeeperClientPort: String = null
  @BeanProperty var hbaseWriteBufferSize: String = null
  @BeanProperty var hbaseMaster: String = null

  def getHbaseConfiguration: com.cloudphysics.HBridgeConfig = {
    val conf = HBridgeConfig(hbaseZookeeperQuorum,
      hbaseZookeeperClientPort,
      hbaseWriteBufferSize,
      hbaseMaster,
      hbasetable, hbaseColumFamily)
    conf
  }

  override def toString: String = {
    return String.format("hbasetable (%s), hbaseconfig (%s)", hbasetable, getHbaseConfiguration)
  }
}