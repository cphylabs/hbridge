package com.cloudphysics.data.test

import com.cloudphysics.data.HBridge
import com.cloudphysics.data.HBridgeConfig

import org.specs._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.HTable
import org.specs.SpecificationWithJUnit
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._

/**
 * Unit test for Get operations.
 */
class HBridgeSpec extends SpecificationWithJUnit with HBaseTestCluster {
  val TABLE_NAME = "Table"
  val FAMILY_NAME = "jvmheap"


  doBeforeSpec {
    startMiniCluster
  }

  "Scala HBase Operations " should {
    doFirst {
      if (!hBaseAdmin.tableExists(TABLE_NAME)) {
        hBaseTestingUtility.createTable(Bytes.toBytes(TABLE_NAME), Bytes.toBytes(FAMILY_NAME))
      }
      else {
        hBaseTestingUtility.truncateTable(Bytes.toBytes(TABLE_NAME))
      }
    }

    "perform standard Put Operation" in {
      val hbridgeConfig = HBridgeConfig(hBaseTestingUtility.getConfiguration)
      val hbridge = HBridge(hbridgeConfig,TABLE_NAME)
      hbridge.put("machine1", "jvmheap", "min", "300")
      hbridge.put("machine1", "jvmheap", "max", "10000")
      val min = hbridge.getString("machine1", "jvmheap", "min").get
      min mustEqual "300"
      hbridge.returnToPool
    }


  }

  // Stop HBase
  doAfterSpec {

    shutdownMiniCluster
  }
}
