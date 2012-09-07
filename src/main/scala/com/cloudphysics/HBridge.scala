package com.cloudphysics.data

import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.joda.time.{ DateTime, DateTimeZone }
import org.joda.time.format.ISODateTimeFormat
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor }
import org.apache.hadoop.hbase.filter._
import grizzled.slf4j.Logging
import org.apache.hadoop.hbase.KeyValue

object Benchmark extends Logging {
  def time(description: String)(f: => Unit) = {
    val s = System.currentTimeMillis
    f
    logger.info(System.currentTimeMillis - s)
  }
  import java.util.Date
  def profile[T](body: => T) = {
    val start = new Date()
    val result = body
    val end = new Date()
    logger.info("Execution took " + (end.getTime() - start.getTime()) / 1000 + " seconds")
    result
  }
}

object HBridge extends Logging {

  private def CHUNK_SIZE = 3

  def apply(hbridgeConfig: HBridgeConfig, tableName: String) = {
    new HBridge(hbridgeConfig.htablePool, tableName)
  }

  def deleteAllConnections = HBridge.terminateClient()

  def terminateClient() { HConnectionManager.deleteAllConnections(true) }

  def toBytes(value: Any): Array[Byte] = {
    value match {
      case null => null
      case i: Int => Bytes.toBytes(i.toLong)
      case l: Long => Bytes.toBytes(l)
      case f: Float => Bytes.toBytes(f.toDouble)
      case d: Double => Bytes.toBytes(d)
      case b: Boolean => Bytes.toBytes(b)
      case bytes: Array[Byte] => bytes
      case _ => Bytes.toBytes(value.toString)
    }
  }

  def withHadmin(configObject: HBridgeConfig, tableName: String = null)(f: (HBaseAdmin, String) => Any) {
    val conf: Configuration = configObject.configuration.get
    val hAdmin = new HBaseAdmin(conf)
    try {
      f(hAdmin, tableName)
    } finally {
      hAdmin.close()
    }
  }

  def create(tableName: String, families: List[String], configObject: HBridgeConfig) {
    withHadmin(configObject, tableName) {
      (hAdmin, tableName) =>
        val descriptor = new HTableDescriptor(tableName)
        for (family <- families)
          descriptor.addFamily(new HColumnDescriptor(Bytes.toBytes(family)))
        hAdmin.createTable(descriptor)
    }
  }

  def drop(tableName: String, configObject: HBridgeConfig) {
    withHadmin(configObject, tableName) {
      (hAdmin, tableName) =>
        hAdmin.disableTable(tableName)
        hAdmin.deleteTable(tableName)
    }
  }

  def recreate(tableName: String, configObject: HBridgeConfig) {
    withHadmin(configObject, tableName) {
      (hAdmin, tableName) =>
        val descriptor = hAdmin.getTableDescriptor(Bytes.toBytes(tableName))
        drop(tableName, configObject)
        hAdmin.createTable(descriptor)
    }
  }

  def exists(tableName: String, configObject: HBridgeConfig): Boolean = {
    var isExists: Boolean = false
    withHadmin(configObject, tableName) {
      (hAdmin, tableName) =>
        isExists = hAdmin.tableExists(tableName)
    }
    isExists
  }

  def disable(tableName: String, configObject: HBridgeConfig) {
    withHadmin(configObject, tableName) {
      (hAdmin, tableName) =>
        hAdmin.disableTable(tableName)
    }
  }

  def enable(tableName: String, configObject: HBridgeConfig) {
    withHadmin(configObject, tableName) {
      (hAdmin, tableName) =>
        hAdmin.enableTable(tableName)
    }
  }

  def disableWork(tableName: String, func: () => Unit, configObject: HBridgeConfig) {
    withHadmin(configObject, tableName) {
      (hAdmin, tableName) =>
        hAdmin.disableTable(tableName)
        func.apply
        hAdmin.enableTable(tableName)
    }
  }

  def list(configObject: HBridgeConfig): List[String] = {
    var nameList = List[String]()
    withHadmin(configObject) {
      (hAdmin, tableName) =>
        hAdmin.listTables.foreach(table => { nameList +:= table.getNameAsString })
    }
    nameList
  }

  def addColumn(tableName: String, familyName: String, configObject: HBridgeConfig) {
    withHadmin(configObject) {
      (hAdmin, tableName) =>
        val column = new HColumnDescriptor(familyName)
        hAdmin.disableTable(tableName)
        hAdmin.addColumn(tableName, column)
        hAdmin.enableTable(tableName)
    }
  }

  def deleteColumn(tableName: String, familyName: String, configObject: HBridgeConfig) {
    withHadmin(configObject) {
      (hAdmin, tableName) =>
        hAdmin.disableTable(tableName)
        hAdmin.deleteColumn(tableName, familyName)
        hAdmin.enableTable(tableName)
    }
  }

  def withHbasePut(hbridge: HBridge, rowKey: Any, family: Any, qualifier: Any, value: Any)(f: (HBridge, Any, Any, Any, Any) => Unit) {
    hbridge.setAutoFlush(false)
    try {
      f(hbridge, rowKey, family, qualifier, value)
    } finally {
      hbridge.commit
      hbridge.returnToPool
    }
  }

  def withHbasePutCollection(hbridge: HBridge, rowKey: String, family: String, dataMap: List[(String, Any)], timeStamp: Long)(f: (HBridge, String, String, List[(String, Any)], Long) => Unit) {
    hbridge.setAutoFlush(false)
    try {
      f(hbridge, rowKey, family, dataMap, timeStamp)
    } finally {
      hbridge.commit
      hbridge.returnToPool
    }
  }

  def withHbasePutCollectionRegex(hbridge: HBridge, rowKey: String, family: String, dataMap: List[(String, String)], timeStamp: Long)(f: (HBridge, String, String, List[(String, String)], Long) => Unit) {
    hbridge.setAutoFlush(false)
    try {
      f(hbridge, rowKey, family, dataMap, timeStamp)
    } finally {
      hbridge.commit
      hbridge.returnToPool
    }
  }

  def batchInsertIntoHbase(hbridge: HBridge)(rowKey: String, family: String, timeStamp: Long, dataMap: List[(String, Any)]) {
    withHbasePutCollection(hbridge, rowKey, family, dataMap, timeStamp) {
      (hbridge, rowKey, columnFamily, dataMap, timeStamp) =>
        hbridge.putBufferingWithType(rowKey, columnFamily, dataMap, timeStamp)
    }
  }

  def batchInsertIntoHbaseRegex(hbridge: HBridge)(rowKey: String, family: String, timeStamp: Long, dataMap: List[(String, String)]) {
    withHbasePutCollectionRegex(hbridge, rowKey, family, dataMap, timeStamp) {
      (hbridge, rowKey, columnFamily, dataMap, timeStamp) =>
        hbridge.putBuffering(rowKey, columnFamily, dataMap, timeStamp)
    }
  }

  def insertIntoHbase(hbridge: HBridge)(rowKey: String, family: String, timeStamp: Long, dataMap: List[(String, Any)]) {
    withHbasePutCollection(hbridge, rowKey, family, dataMap, timeStamp) {
      (hbridge, rowKey, columnFamily, dataMap, timeStamp) =>
        hbridge.putDataMap(rowKey, columnFamily, dataMap, timeStamp)
    }
  }
}

class HBridge(htablePool: Option[HTablePool], tableName: String) extends Logging {

  val table: HTable = htablePool.get.getTable(tableName).asInstanceOf[HTable]
  def closeTablePool(tableName: String) = if (!htablePool.eq(None)) htablePool.get.closeTablePool(tableName)
  //def putTable = htablePool.get.putTable(table)
  def putTable = table.close
  def setAutoFlush(flushState: Boolean) = table.setAutoFlush(flushState)
  def isAutoFlush: Boolean = table.isAutoFlush
  def commit = table.flushCommits()
  def returnToPool = table.close();
  /*
  Regex Variables to type Inference based on Value in a Typed Map with values normalized to Strings a.k.a Map[String,String]
  */
  val TIMESTAMP = "timeStamp"
  val parser = ISODateTimeFormat.dateTime()
  //val DigitValue = """-?\d+""".r -- Triage
  val DigitValue = """-?[0-9][0-9]*""".r
  val DoubleValue = """-?[0-9]*[.]{1}[0-9]*""".r
  val StringValue = """\w+-?\w+""".r
  val AlphaNumValue = """[A-Za-z0-9\s?-?,;_\./\[\]]+""".r
  val BooleanValue = """[Tt]rue|[Ff]alse""".r
  val DateRegex = """([0-9]{4}-[0-9]{2}-[0-9]{2})"""
  val TimeRegex = """([T]+[0-9]{2}:[0-9]{2}:[0-9]{2}.?[0-9]*+[Z])"""
  val DateTime = (DateRegex + TimeRegex).r
  val NullValue = """[Nn]ull""".r
  val EmptyValue = """\[\]""".r
  val GuidValue = """[\w]+-[\w]+-[\w]+-[\w]+-[\w]+""".r

  
  
  def putBufferingWithTypeDebug(rowKey: String, columnFamily: String, dataMap: List[(String, Any)], timeStamp: Long) {
    import java.lang.NumberFormatException
    val putList = new java.util.ArrayList[Put]()
    val putData = putCache(rowKey, columnFamily, TIMESTAMP, timeStamp, timeStamp)
    putList.add(putData)
    dataMap foreach {
      case (columnKey, value) =>
        value match {
          case l: Long =>
            val putData =
              try {
                val columnKeyWithType = columnKey + ":" + DataType.dLong.id
                logger.info("LONG: RowKey " + rowKey + " , ColumnFamily : " + columnFamily + " , ColumnKeyWithType : " + " , TimeStamp " +  columnKeyWithType + " , Value " + l)
                
              } catch {
                case e: NumberFormatException => {
                  val columnKeyWithType = columnKey + ":" + DataType.dString.id
                  logger.info("LONG(as String): RowKey " + rowKey + " , ColumnFamily : " + columnFamily + " , ColumnKeyWithType : " + " , TimeStamp " +  columnKeyWithType + " , Value " + value.toString)
                }
              }
          case d: Double =>
            val putData =
              try {
                val columnKeyWithType = columnKey + ":" + DataType.dDouble.id
                logger.info("DOUBLE: RowKey " + rowKey + " , ColumnFamily : " + columnFamily + " , ColumnKeyWithType : " + " , TimeStamp " +  columnKeyWithType + " , Value " + d)
                
              } catch {
                case e: NumberFormatException => {
                  val columnKeyWithType = columnKey + ":" + DataType.dString.id
                  logger.info("DOUBLE(as String): RowKey " + rowKey + " , ColumnFamily : " + columnFamily + " , ColumnKeyWithType : " + " , TimeStamp " +  columnKeyWithType + " , Value " + value.toString)
                }
              }
          case i: Int =>
            val putData =
              try {
                val columnKeyWithType = columnKey + ":" + DataType.dLong.id
                logger.info("INTEGER: RowKey " + rowKey + " , ColumnFamily : " + columnFamily + " , ColumnKeyWithType : " + " , TimeStamp " +  columnKeyWithType + " , Value " + i)
                
              } catch {
                case e: NumberFormatException => {
                  val columnKeyWithType = columnKey + ":" + DataType.dString.id
                  logger.info("INTEGER(as String): RowKey " + rowKey + " , ColumnFamily : " + columnFamily + " , ColumnKeyWithType : " + " , TimeStamp " +  columnKeyWithType + " , Value " + value.toString)
                }
              }
          case b: Boolean =>
            val columnKeyWithType = columnKey + ":" + DataType.dBoolean.id
            logger.info("BOOLEAN: RowKey " + rowKey + " , ColumnFamily : " + columnFamily + " , ColumnKeyWithType : " + " , TimeStamp " +  columnKeyWithType + " , Value " + b)
          case s: String =>
            if (value != "" && value != null && value != "null" && value != "[]") {
              val columnKeyWithType = columnKey + ":" + DataType.dString.id
             logger.info("STRING: RowKey " + rowKey + " , ColumnFamily : " + columnFamily + " , ColumnKeyWithType : " + " , TimeStamp " +  columnKeyWithType + " , Value " + value.toString)
            }
        }
    }
    val size = putList.size
  }
  
  
  def putBufferingWithType(rowKey: String, columnFamily: String, dataMap: List[(String, Any)], timeStamp: Long) {
    import java.lang.NumberFormatException
    val putList = new java.util.ArrayList[Put]()
    val putData = putCache(rowKey, columnFamily, TIMESTAMP, timeStamp, timeStamp)
    putList.add(putData)
    dataMap foreach {
      case (columnKey, value) =>
        value match {
          case l: Long =>
            val putData =
              try {
                val columnKeyWithType = columnKey + ":" + DataType.dLong.id
                putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, l)
              } catch {
                case e: NumberFormatException => {
                  val columnKeyWithType = columnKey + ":" + DataType.dString.id
                  putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toString)
                }
              }
            putList.add(putData)
          case d: Double =>
            val putData =
              try {
                val columnKeyWithType = columnKey + ":" + DataType.dDouble.id
                putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, d)
              } catch {
                case e: NumberFormatException => {
                  val columnKeyWithType = columnKey + ":" + DataType.dString.id
                  putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toString)
                }
              }
            putList.add(putData)
          case i: Int =>
            val putData =
              try {
                val columnKeyWithType = columnKey + ":" + DataType.dLong.id
                putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, i)
              } catch {
                case e: NumberFormatException => {
                  val columnKeyWithType = columnKey + ":" + DataType.dString.id
                  putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toString)
                }
              }
            putList.add(putData)
          case b: Boolean =>
            val columnKeyWithType = columnKey + ":" + DataType.dBoolean.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, b)
            putList.add(putData)
          case s: String =>
            if (value != "" && value != null && value != "null" && value != "[]") {
              val columnKeyWithType = columnKey + ":" + DataType.dString.id
              val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, s)
              putList.add(putData)
            }
        }
    }
    val size = putList.size
    val putLists = putList.grouped(HBridge.CHUNK_SIZE).toList

    putLists foreach { list => table.put(list) }
  }

  def putBuffering(rowKey: String, columnFamily: String, dataMap: List[(String, String)], timeStamp: Long) {
    import java.lang.NumberFormatException
    val putList = new java.util.ArrayList[Put]()
    val putData = putCache(rowKey, columnFamily, TIMESTAMP, timeStamp, timeStamp)
    putList.add(putData)
    dataMap foreach {
      case (columnKey, value) =>
        value match {
          case DoubleValue() =>
            val putData =
              try {
                val columnKeyWithType = columnKey + ":" + DataType.dDouble.id
                putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toDouble)
              } catch {
                case e: NumberFormatException => {
                  val columnKeyWithType = columnKey + ":" + DataType.dString.id
                  putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toString)
                }
              }
            putList.add(putData)
          case DigitValue() =>
            val putData =
              try {
                val columnKeyWithType = columnKey + ":" + DataType.dLong.id
                putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toLong)
              } catch {
                case e: NumberFormatException => {
                  val columnKeyWithType = columnKey + ":" + DataType.dString.id
                  putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toString)
                }
              }
            putList.add(putData)
          case DateTime(d, t) =>
            val dateTime = new DateTime(value)
            val millSeconds: Long = dateTime.getMillis
            val columnKeyWithType = columnKey + ":" + DataType.dTime.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, millSeconds)
            putList.add(putData)
          case BooleanValue() =>
            val columnKeyWithType = columnKey + ":" + DataType.dBoolean.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toBoolean)
            putList.add(putData)
          case "null" | EmptyValue() | "" =>
            None
          case StringValue() | AlphaNumValue() | GuidValue() | _ =>
            if (value != "") {
              val columnKeyWithType = columnKey + ":" + DataType.dString.id
              val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toString)
              putList.add(putData)
            }
        }
    }
    val size = putList.size
    val putLists = putList.grouped(HBridge.CHUNK_SIZE).toList

    putLists foreach { list => table.put(list) }
  }

  def putDataMap(rowKey: String, columnFamily: String, dataMap: List[(String, Any)], timeStamp: Long) {
    val putData = putCache(rowKey, columnFamily, TIMESTAMP, timeStamp, timeStamp)
    table.put(putData)
    dataMap foreach {
      case (columnKey, value) =>
        value match {
          case i: Int =>
            val columnKeyWithType = columnKey + ":" + DataType.dLong.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, i)
            table.put(putData)
          case d: Double =>
            val columnKeyWithType = columnKey + ":" + DataType.dDouble.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, d)
            table.put(putData)
          case b: Boolean =>
            val columnKeyWithType = columnKey + ":" + DataType.dBoolean.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, b)
            table.put(putData)
          case s: String =>
            if (value != "") {
              val columnKeyWithType = columnKey + ":" + DataType.dString.id
              val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, s)
              table.put(putData)
            }
        }
    }

  }

  def putCache(row: Any, family: Any, qualifier: Any = Nil, ts: Long, value: Any): Put = {
    val put = new Put(HBridge.toBytes(row))
    put.add(HBridge.toBytes(family), HBridge.toBytes(qualifier), ts, HBridge.toBytes(value))
    put
  }

  def put(row: Any, family: Any, qualifier: Any, value: Any) {
    val put = new Put(HBridge.toBytes(row))
    put.add(HBridge.toBytes(family), HBridge.toBytes(qualifier), HBridge.toBytes(value))
    table.put(put)
  }

  def put(row1: Any, row2: Any, family: Any, qualifier: Any, value: Any) {
    val rowkey: Array[Byte] = Bytes.add(HBridge.toBytes(row1), HBridge.toBytes(row2))
    val put = new Put(rowkey)
    put.add(HBridge.toBytes(family), HBridge.toBytes(qualifier), HBridge.toBytes(value))
    table.put(put)
  }

  def put(row1: Any, row2: Any, row3: Any, family: Any, qualifier: Any, value: Any) {
    val rowkey: Array[Byte] = Bytes.add(HBridge.toBytes(row1), HBridge.toBytes(row2), HBridge.toBytes(row3))
    val put = new Put(rowkey)
    put.add(HBridge.toBytes(family), HBridge.toBytes(qualifier), HBridge.toBytes(value))
    table.put(put)
  }

  def incr(row: Any, family: Any, qualifier: Any, value: Long) {
    table.incrementColumnValue(HBridge.toBytes(row), HBridge.toBytes(family),
      HBridge.toBytes(qualifier), value)
  }

  def exists(row: Any): Boolean = {
    table.exists(new Get(HBridge.toBytes(row)))
  }

  def exists(row: Any, family: Any, qualifier: Any): Boolean = {
    val get = new Get(HBridge.toBytes(row))
    get.addColumn(HBridge.toBytes(family), HBridge.toBytes(qualifier))
    table.exists(get)
  }

  def getDetails(row: Any, family: Any, qualifier: Any) {
    val result = table.get(new Get(HBridge.toBytes(row)))
    val kvs: java.util.List[KeyValue] = result.list()
    val dataSet = kvs.filter(kv => Bytes.toString(kv.getQualifier).equals(qualifier) && Bytes.toString(kv.getFamily).equals(family))
    dataSet foreach { kv => logger.info(" TimeStamp Value for Qualifier " + qualifier + " is " + dateLongtoString(kv.getTimestamp)) }
  }

  def get(row: Any, family: Any, qualifier: Any): Array[Byte] = {
    val result = table.get(new Get(HBridge.toBytes(row)))
    result.getValue(HBridge.toBytes(family), HBridge.toBytes(qualifier))
  }

  def getWithTs(row: Any, family: Any, qualifier: Any): Option[(Array[Byte], Long)] = {
    val get = new Get(HBridge.toBytes(row))
    get.addColumn(HBridge.toBytes(family), HBridge.toBytes(qualifier))
    val result = table.get(get)
    val kv = Option(result.getColumnLatest(HBridge.toBytes(family), HBridge.toBytes(qualifier)))
    kv map {
      keyValue => (keyValue.getValue, keyValue.getTimestamp)
    }
  }

  def get(row: Any,
    func: (Array[Byte], Array[Byte], Array[Byte]) => Unit) {
    for (kv <- table.get(new Get(HBridge.toBytes(row))).raw) {
      func(kv.getFamily, kv.getQualifier, kv.getValue)
    }
  }

  def dateLongtoString(millis: Long): String = {
    val zoneUTC = DateTimeZone.UTC
    (new DateTime(millis, zoneUTC)).toString
  }

  def getDateString(row: Any, family: Any, qualifier: Any): Option[String] = {
    val result = get(row, family, qualifier)
    Option(result) map {
      valueData => dateLongtoString(Bytes.toLong(valueData))
    }
  }

  def getDateStringwithTs(row: Any, family: Any, qualifier: Any): Option[(String, Long)] = {
    getWithTs(row, family, qualifier) map {
      t => (dateLongtoString(Bytes.toLong(t._1)), t._2)
    }
  }

  def getLong(row: Any, family: Any, qualifier: Any): Option[Long] = {
    val result = get(row, family, qualifier)
    Option(result) map { valueData => Bytes.toLong(valueData) }
  }

  def getLongWithTs(row: Any, family: Any, qualifier: Any): Option[(Long, Long)] = {
    getWithTs(row, family, qualifier) map {
      t => (Bytes.toLong(t._1), t._2)
    }
  }

  def getDouble(row: Any, family: Any, qualifier: Any): Option[Double] = {
    val result = get(row, family, qualifier)
    Option(result) map { valueData => Bytes.toDouble(valueData) }
  }

  def getDoubleWithTs(row: Any, family: Any, qualifier: Any): Option[(Double, Long)] = {
    getWithTs(row, family, qualifier) map {
      t => (Bytes.toDouble(t._1), t._2)
    }
  }

  def getString(row: Any, family: Any, qualifier: Any): Option[String] = {
    val result = get(row, family, qualifier)
    Option(result) map { valueData => Bytes.toString(valueData) }
  }

  def getStringWithTs(row: Any, family: Any, qualifier: Any): Option[(String, Long)] = {
    getWithTs(row, family, qualifier) map {
      t => (Bytes.toString(t._1), t._2)
    }
  }

  def getBytes(row: Any,
    func: (String, String, Array[Byte]) => Unit) {
    for (kv <- table.get(new Get(HBridge.toBytes(row))).raw) {
      func(Bytes.toString(kv.getFamily), Bytes.toString(kv.getQualifier), kv.getValue)
    }
  }

  def getString(row: Any,
    func: (String, String, String) => Unit) {
    for (kv <- table.get(new Get(HBridge.toBytes(row))).raw) {
      func(Bytes.toString(kv.getFamily), Bytes.toString(kv.getQualifier),
        Bytes.toString(kv.getValue))
    }
  }

  def getLong(row: Any,
    func: (String, String, Long) => Unit) {
    for (kv <- table.get(new Get(HBridge.toBytes(row))).raw) {
      func(Bytes.toString(kv.getFamily), Bytes.toString(kv.getQualifier),
        Bytes.toLong(kv.getValue))
    }
  }

  def getDouble(row: Any,
    func: (String, String, Double) => Unit) {
    for (kv <- table.get(new Get(HBridge.toBytes(row))).raw) {
      func(Bytes.toString(kv.getFamily), Bytes.toString(kv.getQualifier),
        Bytes.toDouble(kv.getValue))
    }
  }

  def scan(startRow: Any, endRow: Any,
    func: (Array[Byte], Array[Byte], Array[Byte], Long, Array[Byte]) => Unit) {
    val rs = table.getScanner(new Scan(HBridge.toBytes(startRow), HBridge.toBytes(endRow)))
    try {
      for (item <- rs) {
        for (kv <- item.raw) {
          func(item.getRow, kv.getFamily, kv.getQualifier, kv.getTimestamp, kv.getValue)
        }
      }
    } finally {
      rs.close
    }
  }

  def scan(startRow: Any,
    func: (Array[Byte], Array[Byte], Array[Byte], Long, Array[Byte]) => Unit) {
    scan(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanBytes(startRow: Any, endRow: Any,
    func: (String, String, String, Long, Array[Byte]) => Unit) {
    scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
      qualifier: Array[Byte], timeStamp: Long, value: Array[Byte]) => {
      func(Bytes.toString(row), Bytes.toString(family),
        Bytes.toString(qualifier), timeStamp, value)
    })
  }

  def scanBytes(startRow: Any,
    func: (String, String, String, Long, Array[Byte]) => Unit) {
    scanBytes(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanString(startRow: Any, endRow: Any,
    func: (String, String, String, Long, String) => Unit) {
    scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
      qualifier: Array[Byte], timeStamp: Long, value: Array[Byte]) => {
      func(Bytes.toString(row), Bytes.toString(family),
        Bytes.toString(qualifier), timeStamp, Bytes.toString(value))
    })
  }

  def scanString(startRow: Any,
    func: (String, String, String, Long, String) => Unit) {
    scanString(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanLong(startRow: Any, endRow: Any,
    func: (String, String, String, Long, Long) => Unit) {
    scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
      qualifier: Array[Byte], timeStamp: Long, value: Array[Byte]) => {
      func(Bytes.toString(row), Bytes.toString(family),
        Bytes.toString(qualifier), timeStamp, Bytes.toLong(value))
    })
  }

  def scanLong(startRow: Any,
    func: (String, String, String, Long, Long) => Unit) {
    scanLong(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanDouble(startRow: Any, endRow: Any,
    func: (String, String, String, Long, Double) => Unit) {
    scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
      qualifier: Array[Byte], timeStamp: Long, value: Array[Byte]) => {
      func(Bytes.toString(row), Bytes.toString(family),
        Bytes.toString(qualifier), timeStamp, Bytes.toDouble(value))
    })
  }

  def scanDouble(startRow: Any,
    func: (String, String, String, Long, Double) => Unit) {
    scanDouble(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanRow(startRow: Any, endRow: Any, func: (Result) => Unit) {
    val rs = table.getScanner(new Scan(HBridge.toBytes(startRow), HBridge.toBytes(endRow)))
    try {
      for (item <- rs)
        func(item)
    } finally {
      rs.close
    }
  }

  def scanRowWithFilterCustom(filter: org.apache.hadoop.hbase.filter.Filter, func: (Result) => Unit) {
    val scan = new Scan()
    scan.setFilter(filter)
    val rs = table.getScanner(scan)
    try {
      for (item <- rs)
        func(item)
    } finally {
      rs.close()
    }
  }

  def scanRowWithFilterDynamic(filter: org.apache.hadoop.hbase.filter.Filter) = {
    val scan = new Scan()
    scan.setFilter(filter)
    val rs = table.getScanner(scan)
    try {

      Option(
        for (
          item <- rs;
          keyvalue <- item.raw()
        ) yield (Bytes.toString(keyvalue.getRow), Bytes.toString(keyvalue.getQualifier), Bytes.toLong(keyvalue.getValue)))

    } finally {
      rs.close()
    }
  }

  def getValueByType(valueType: String, valueRaw: Array[Byte]): Any = {
    import DataType._
    val typeBit: DataType = DataType(valueType.split(":").reverse.head.toInt)
    val valueResult =
      typeBit match {
        case DataType.dBoolean =>
          Bytes.toBoolean(valueRaw)
        case DataType.dDouble =>
          Bytes.toDouble(valueRaw)
        case DataType.dFloat =>
          Bytes.toFloat(valueRaw)
        case DataType.dInt =>
          Bytes.toInt(valueRaw)
        case DataType.dLong =>
          Bytes.toLong(valueRaw)
        case DataType.dString =>
          Bytes.toString(valueRaw)
        case DataType.dTime =>
          val ms = Bytes.toLong(valueRaw)
          val dateTime = new DateTime(ms)
          dateTime
        case _ => None
      }
    valueResult
  }

  def scanRowWithFilter(filter: org.apache.hadoop.hbase.filter.Filter) = {
    val scan = new Scan()
    scan.setFilter(filter)
    val rs = table.getScanner(scan)
    try {
      Option(
        for (
          item <- rs;
          keyvalue <- item.raw()
        ) yield (Bytes.toString(keyvalue.getRow),
          Bytes.toString(keyvalue.getQualifier),
          keyvalue.getTimestamp,
          getValueByType(Bytes.toString(keyvalue.getQualifier),
            keyvalue.getValue)))

    } finally {
      rs.close()
    }
  }

  def scanRowWithFilterLimit(startRow: String, endRow: String,filter: org.apache.hadoop.hbase.filter.Filter) = {
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes(startRow))
    scan.setStopRow(Bytes.toBytes(endRow))
    
    scan.setFilter(filter)
    val rs = table.getScanner(scan)
    try {
      Option(
        for (
          item <- rs;
          keyvalue <- item.raw()
        ) yield (Bytes.toString(keyvalue.getRow),
          Bytes.toString(keyvalue.getQualifier),
          keyvalue.getTimestamp,
          getValueByType(Bytes.toString(keyvalue.getQualifier),
            keyvalue.getValue)))

    } finally {
      rs.close()
    }
  }
  
  def scanRowColumnWithFilter(startRow: String, endRow: String, filter: org.apache.hadoop.hbase.filter.Filter) = {
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes(startRow))
    scan.setStopRow(Bytes.toBytes(endRow))

    scan.setFilter(filter)
    val rs = table.getScanner(scan)
    try {
      Option(
        for (
          item <- rs;
          keyvalue <- item.raw()
        ) yield (Bytes.toString(keyvalue.getRow),
          Bytes.toString(keyvalue.getQualifier),
          keyvalue.getTimestamp,
          getValueByType(Bytes.toString(keyvalue.getQualifier),
            keyvalue.getValue)))

    } finally {
      rs.close()
    }
  }

  def scanRowWithFilterSet(rowExp: String, qualifierExp: String) = {
    val filters = new java.util.ArrayList[Filter]()
    val typedQualifer = qualifierExp + ".*"
    val rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(rowExp))
    filters.add(rowFilter)
    val qualiferFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(typedQualifer))
    filters.add(qualiferFilter)
    val filterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters)
    scanRowWithFilter(filterList)
  }

  def scanRowWithQualifierList(rowExp: String, qualifierExp: List[String]) = {
    val filters = new java.util.ArrayList[Filter]()
    for (q <- qualifierExp) {
      val filterCriteria = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(q)))
      filters.add(filterCriteria)
    }
    val filterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters)
    val uberFilters = new java.util.ArrayList[Filter]()
    uberFilters.add(filterList)
    val rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rowExp)))
    uberFilters.add(rowFilter)
    val finalFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, uberFilters)
    scanRowWithFilter(finalFilterList)
  }

  
  def scanRowListWithQualifierList(startRow: String, endRow: String,rowExp: List[String], qualifierExp: List[String]) = {
    val qfilters = new java.util.ArrayList[Filter]()
    for (q <- qualifierExp) {
      val filterCriteria = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(q)))
      qfilters.add(filterCriteria)
    }
    val qFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, qfilters)
    val rfilters = new java.util.ArrayList[Filter]()
    for (r <- rowExp) {
      val rowCriteria = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(r)))
      rfilters.add(rowCriteria)
    }
    val rFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, rfilters)
    val uberFilters = new java.util.ArrayList[Filter]()
    uberFilters.add(qFilterList)
    uberFilters.add(rFilterList)
    val finalFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, uberFilters)
    scanRowWithFilterLimit(startRow, endRow, finalFilterList)
  }
  
  def scanRowListWithQualifierList(rowExp: List[String], qualifierExp: List[String]) = {
    val qfilters = new java.util.ArrayList[Filter]()
    for (q <- qualifierExp) {
      val filterCriteria = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(q)))
      qfilters.add(filterCriteria)
    }
    val qFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, qfilters)
    val rfilters = new java.util.ArrayList[Filter]()
    for (r <- rowExp) {
      val rowCriteria = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(r)))
      rfilters.add(rowCriteria)
    }
    val rFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, rfilters)
    val uberFilters = new java.util.ArrayList[Filter]()
    uberFilters.add(qFilterList)
    uberFilters.add(rFilterList)
    val finalFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, uberFilters)
    scanRowWithFilter(finalFilterList)
  }

  def scanRowWithQualifierListRegex(rowExp: String, qualifierExp: List[String]) = {
    val filters = new java.util.ArrayList[Filter]()
    val typedQualiferList = qualifierExp map (_ + ".*")
    for (q <- typedQualiferList) {
      val filterCriteria = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(q))
      filters.add(filterCriteria)
    }
    val filterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters)
    val uberFilters = new java.util.ArrayList[Filter]()
    uberFilters.add(filterList)
    val rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(rowExp))
    uberFilters.add(rowFilter)
    val finalFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, uberFilters)
    scanRowWithFilter(finalFilterList)
  }

  def scanRowListWithQualifierListRegex(rowExp: List[String], qualifierExp: List[String]) = {
    val qfilters = new java.util.ArrayList[Filter]()
    val typedQualiferList = qualifierExp map (_ + ".*")
    for (q <- typedQualiferList) {
      val filterCriteria = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(q))
      qfilters.add(filterCriteria)
    }
    val qFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, qfilters)
    val rfilters = new java.util.ArrayList[Filter]()
    for (r <- rowExp) {
      val rowCriteria = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(r))
      rfilters.add(rowCriteria)
    }
    val rFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, rfilters)
    val uberFilters = new java.util.ArrayList[Filter]()
    uberFilters.add(qFilterList)
    uberFilters.add(rFilterList)
    val finalFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, uberFilters)
    scanRowWithFilter(finalFilterList)
  }

  def scanRowListWithQualifierListRegex(startRow: String, endRow: String,rowExp: List[String], qualifierExp: List[String]) = {
    val qfilters = new java.util.ArrayList[Filter]()
    val typedQualiferList = qualifierExp map (_ + ".*")
    for (q <- typedQualiferList) {
      val filterCriteria = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(q))
      qfilters.add(filterCriteria)
    }
    val qFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, qfilters)
    val rfilters = new java.util.ArrayList[Filter]()
    for (r <- rowExp) {
      val rowCriteria = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(r))
      rfilters.add(rowCriteria)
    }
    val rFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE, rfilters)
    val uberFilters = new java.util.ArrayList[Filter]()
    uberFilters.add(qFilterList)
    uberFilters.add(rFilterList)
    val finalFilterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, uberFilters)
    scanRowWithFilterLimit(startRow, endRow, finalFilterList)
  }
  
  def scanRowKeyWithRegExCompareFilter(rowExp: String) = {
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(rowExp))
    scanRowWithFilter(filter)
  }

  def scanRowKeyWithSubStringCompareFilter(rowExp: String) = {
    val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(rowExp))
    scanRowWithFilter(filter)
  }

  def scanRowKeyWithBinaryCompareFilter(rowExp: String) = {
    val filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(rowExp)))
    scanRowWithFilter(filter)
  }

  def scanFamilyWithRegExCompareFilter(familyExp: String) = {
    val filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(familyExp))
    scanRowWithFilter(filter)
  }

  def scanFamilyWithSubStringCompareFilter(familyExp: String) = {
    val filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(familyExp))
    scanRowWithFilter(filter)
  }

  def scanFamilyWithBinaryCompareFilter(familyExp: String) = {
    val filter = new FamilyFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(familyExp)))
    scanRowWithFilter(filter)
  }

  def scanQualifierWithRegExCompareFilter(qualifierExp: String) = {
    val filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(qualifierExp))
    scanRowWithFilter(filter)
  }

  def scanQualifierWithSubStringCompareFilter(qualifierExp: String) = {
    val filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(qualifierExp))
    scanRowWithFilter(filter)
  }

  def scanQualifierWithBinaryCompareFilter(qualifierExp: String) = {
    val filter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(qualifierExp)))
    scanRowWithFilter(filter)
  }

  def scanValueWithRegExCompareFilter(valueExp: String) = {
    val filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(valueExp))
    scanRowWithFilter(filter)
  }

  def scanValueWithSubStringCompareFilter(valueExp: String) = {
    val filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(valueExp))
    scanRowWithFilter(filter)
  }

  def scanValueWithBinaryCompareFilter(valueExp: String) = {
    val filter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(valueExp)))
    scanRowWithFilter(filter)
  }

  def scanRow(startRow: Any, func: (Result) => Unit) {
    val startBytes = HBridge.toBytes(startRow)
    scanRow(startBytes, stepNextBytes(startBytes), func)
  }

  private def stepNextBytes(bytes: Array[Byte]): Array[Byte] = {
    val nextBytes = new Array[Byte](bytes.length)
    nextBytes(nextBytes.length - 1) = (nextBytes(nextBytes.length - 1) + 1).toByte
    nextBytes
  }

  def delete(row: Any) {
    val delete = new Delete(HBridge.toBytes(row))
    table.delete(delete)
  }

  def delete(row: Any, family: Any) {
    val delete = new Delete(HBridge.toBytes(row))
    delete.deleteFamily(HBridge.toBytes(family))
    table.delete(delete)
  }

  def delete(row: Any, family: Any, qualifier: Any) {
    val delete = new Delete(HBridge.toBytes(row))
    delete.deleteColumn(HBridge.toBytes(family), HBridge.toBytes(qualifier))
    table.delete(delete)
  }
}