package com.cloudphysics.hbridge

object DataType extends Enumeration {
  type DataType = Value
  val dByteSeq, dByte, dInt, dLong, dFloat, dDouble, dString, dBoolean, dTime = Value
}

case class HBridgeConfig(hbaseZookeeperQuorum: String,
  hbaseZookeeperClientPort: String,
  hbaseWriteBufferSize: String,
  hbaseMaster: String
  )

