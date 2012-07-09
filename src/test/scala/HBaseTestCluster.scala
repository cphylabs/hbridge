package com.cloudphysics.data.test

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.HBaseTestingUtility

/**
 * Trait used to mix in an HBase test cluster into unit tests.
 */
trait HBaseTestCluster {


  val hBaseTestingUtility = new HBaseTestingUtility()
  var hBaseAdmin: HBaseAdmin = null

  /**
   * Gets the HBase configuration.
   * @return the HBase configuration.
   */
  def configuration = hBaseTestingUtility.getConfiguration

  /**
   * Starts a mini HBase cluster.
   */
  def startMiniCluster = {
    hBaseTestingUtility.startMiniCluster()
    hBaseAdmin = new HBaseAdmin(configuration)

  }

  /**
   * Shuts down a previously-started mini HBase cluster.
   */
  def shutdownMiniCluster = {
    hBaseAdmin.close()
    hBaseTestingUtility.shutdownMiniCluster()
  }
}