package com.cloudphysics

import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.joda.time.{ DateTime, DateTimeZone }
import org.joda.time.format.ISODateTimeFormat
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor }
import org.apache.hadoop.hbase.filter._
import grizzled.slf4j.Logging
import com.cloudphysics.HBridgeConfig

object HBridge extends Logging {

  val DEFAULT_CONF_PATH = "resources/hbase-site.xml"

  def setHbaseConfig(hbaseConfig: HBridgeConfig): Configuration = {

    val conf: Configuration = HBaseConfiguration.create()
    conf.clear()
    conf.set("hbase.zookeeper.quorum", hbaseConfig.hbaseZookeeperQuorum)
    conf.set("hbase.zookeeper.property.clientPort", hbaseConfig.hbaseZookeeperClientPort)
    conf.set("hbase.master", hbaseConfig.hbaseMaster)
    conf.set("hbase.client.write.buffer", hbaseConfig.hbaseWriteBufferSize)
    conf
  }

  def terminateClient() { HConnectionManager.deleteAllConnections(true) }

  def toBytes(value: Any): Array[Byte] = {
    if (value == null)
      return null
    if (value.isInstanceOf[Int])
      return Bytes.toBytes(value.asInstanceOf[Int].toLong)
    if (value.isInstanceOf[Long])
      return Bytes.toBytes(value.asInstanceOf[Long])
    if (value.isInstanceOf[Float])
      return Bytes.toBytes(value.asInstanceOf[Float].toDouble)
    if (value.isInstanceOf[Double])
      return Bytes.toBytes(value.asInstanceOf[Double])
    if (value.isInstanceOf[Boolean])
      return Bytes.toBytes(value.asInstanceOf[Boolean])

    return Bytes.toBytes(value.toString)
  }

  def withHadmin(configObject: HBridgeConfig, tableName: String = null)(f: (HBaseAdmin, String) => Any) {
    val conf: Configuration = setHbaseConfig(configObject)
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


  def withHbasePut(hbaseConfig: HBridgeConfig, rowKey : Any, family: Any, qualifier: Any, value: Any)(f : (com.cloudphysics.HBridge, Any,Any,Any,Any,Any))
  {
    val hbridge = new com.cloudphysics.HBridge(hbaseConfig.hbasetable, HBridge.setHbaseConfig(hbaseConfig))
        hbridge.setAutoFlush(false)
        try {
          f(hbridge, rowKey, family , qualifier, value)
        } finally {
          hbridge.commit
          hbridge.close
        }
  }

  def withHbasePutCollection(hbaseConfig: HBridgeConfig, rowKey: String, dataMap: List[(String, String)], timeStamp: Long)(f: (com.cloudphysics.HBridge, String, String, List[(String, String)], Long) => Unit) {

    val hbridge = new com.cloudphysics.HBridge(hbaseConfig.hbasetable, HBridge.setHbaseConfig(hbaseConfig))
    hbridge.setAutoFlush(false)
    try {
      f(hbridge, rowKey, hbaseConfig.hbaseColumFamily, dataMap, timeStamp)
    } finally {
      hbridge.commit
      hbridge.close
    }
  }

  def batchInsertIntoHbase(hbaseConfig: HBridgeConfig)(rowKey: String, timeStamp: Long, dataMap: List[(String, String)]) {

    withHbasePutCollection(hbaseConfig, rowKey, dataMap, timeStamp) {
      (hbridge, rowKey, columnFamily, dataMap, timeStamp) =>
        hbridge.putBuffering(rowKey, columnFamily, dataMap, timeStamp)
    }

  }

  def insertIntoHbase(hbaseConfig: HBridgeConfig)(rowKey: String, timeStamp: Long, dataMap: List[(String, String)]) {

    withHbasePutCollection(hbaseConfig, rowKey, dataMap, timeStamp) {
      (hbridge, rowKey, columnFamily, dataMap, timeStamp) =>
        hbridge.putDataMap(rowKey, columnFamily, dataMap, timeStamp)
    }

  }
}

class HBridge(tableName: String, conf: Configuration = null) extends Logging {

  val table = new HTable(conf, tableName)

  /*
  Regex Variables to type Inference based on Value in a Typed Map with values normalized to Strings a.k.a Map[String,String]
  */

  val parser = ISODateTimeFormat.dateTime()
  val DigitValue = """-?\d+""".r
  val StringValue = """\w+-?\w+""".r
  val AlphaNumValue = """[A-Za-z0-9\s?-?,;_\./\[\]]+""".r
  val BooleanValue = """[Tt]rue|[Ff]alse""".r
  val DateRegex = """([0-9]{4}-[0-9]{2}-[0-9]{2})"""
  val TimeRegex = """([T]+[0-9]{2}:[0-9]{2}:[0-9]{2}.?[0-9]*+[Z])"""
  val DateTime = (DateRegex + TimeRegex).r
  val NullValue = """[Nn]ull""".r
  val EmptyValue = """\[\]""".r
  val GuidValue = """[\w]+-[\w]+-[\w]+-[\w]+-[\w]+""".r

  def putBuffering(rowKey: String, columnFamily: String, dataMap: List[(String, String)], timeStamp: Long) {

    var putList = new java.util.ArrayList[Put]()

    dataMap foreach {
      case (columnKey, value) =>
        value match {
          case DigitValue() =>
            val columnKeyWithType = columnKey + ":" + DataType.dLong.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toLong)
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

    table.put(putList)
  }

  def putDataMap(rowKey: String, columnFamily: String, dataMap: List[(String, String)], timeStamp: Long) {

    dataMap foreach {
      case (columnKey, value) =>
        value match {
          case DigitValue() =>
            val columnKeyWithType = columnKey + ":" + DataType.dLong.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toLong)
            table.put(putData)
          case DateTime(d, t) =>
            val dateTime = new DateTime(value)
            val millSeconds: Long = dateTime.getMillis
            val columnKeyWithType = columnKey + ":" + DataType.dTime.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, millSeconds)
            table.put(putData)
          case BooleanValue() =>
            val columnKeyWithType = columnKey + ":" + DataType.dBoolean.id
            val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toBoolean)
            table.put(putData)
          case "null" | EmptyValue() | "" =>
            None
          case StringValue() | AlphaNumValue() | GuidValue() | _ =>
            if (value != "") {
              val columnKeyWithType = columnKey + ":" + DataType.dString.id
              val putData = putCache(rowKey, columnFamily, columnKeyWithType, timeStamp, value.toString)
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

  def get(row: Any, family: Any, qualifier: Any): Array[Byte] = {
    val result = table.get(new Get(HBridge.toBytes(row)))
    return result.getValue(HBridge.toBytes(family), HBridge.toBytes(qualifier))
  }

  def exists(row: Any): Boolean = table.exists(new Get(HBridge.toBytes(row)))

  def exists(row: Any, family: Any, qualifier: Any): Boolean = {
    val get = new Get(HBridge.toBytes(row))
    get.addColumn(HBridge.toBytes(family), HBridge.toBytes(qualifier))
    table.exists(get)
  }

  def getDateString(row: Any, family: Any, qualifier: Any): String = {
    val milliSeconds: Long = Bytes.toLong(get(row, family, qualifier))
    val zoneUTC = DateTimeZone.UTC
    val dateTime = new DateTime(milliSeconds, zoneUTC)
    dateTime.toString
  }

  def getLong(row: Any, family: Any, qualifier: Any): Long =
    Bytes.toLong(get(row, family, qualifier))

  def getDouble(row: Any, family: Any, qualifier: Any): Double =
    Bytes.toDouble(get(row, family, qualifier))

  def getString(row: Any, family: Any, qualifier: Any): String =
    Bytes.toString(get(row, family, qualifier))

  def get(row: Any,
    func: (Array[Byte], Array[Byte], Array[Byte]) => Unit) {
    for (kv <- table.get(new Get(HBridge.toBytes(row))).raw) {
      func(kv.getFamily, kv.getQualifier, kv.getValue)
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
    func: (Array[Byte], Array[Byte], Array[Byte], Array[Byte]) => Unit) {
    val rs = table.getScanner(new Scan(HBridge.toBytes(startRow), HBridge.toBytes(endRow)))
    try {
      for (item <- rs) {
        for (kv <- item.raw) {
          func(item.getRow, kv.getFamily, kv.getQualifier, kv.getValue)
        }
      }
    } finally {
      rs.close
    }
  }

  def scan(startRow: Any,
    func: (Array[Byte], Array[Byte], Array[Byte], Array[Byte]) => Unit) {
    scan(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanBytes(startRow: Any, endRow: Any,
    func: (String, String, String, Array[Byte]) => Unit) {
    scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
      qualifier: Array[Byte], value: Array[Byte]) => {
      func(Bytes.toString(row), Bytes.toString(family),
        Bytes.toString(qualifier), value)
    })
  }

  def scanBytes(startRow: Any,
    func: (String, String, String, Array[Byte]) => Unit) {
    scanBytes(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanString(startRow: Any, endRow: Any,
    func: (String, String, String, String) => Unit) {
    scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
      qualifier: Array[Byte], value: Array[Byte]) => {
      func(Bytes.toString(row), Bytes.toString(family),
        Bytes.toString(qualifier), Bytes.toString(value))
    })
  }

  def scanString(startRow: Any,
    func: (String, String, String, String) => Unit) {
    scanString(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanLong(startRow: Any, endRow: Any,
    func: (String, String, String, Long) => Unit) {
    scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
      qualifier: Array[Byte], value: Array[Byte]) => {
      func(Bytes.toString(row), Bytes.toString(family),
        Bytes.toString(qualifier), Bytes.toLong(value))
    })
  }

  def scanLong(startRow: Any,
    func: (String, String, String, Long) => Unit) {
    scanLong(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanDouble(startRow: Any, endRow: Any,
    func: (String, String, String, Double) => Unit) {
    scan(startRow, endRow, (row: Array[Byte], family: Array[Byte],
      qualifier: Array[Byte], value: Array[Byte]) => {
      func(Bytes.toString(row), Bytes.toString(family),
        Bytes.toString(qualifier), Bytes.toDouble(value))
    })
  }

  def scanDouble(startRow: Any,
    func: (String, String, String, Double) => Unit) {
    scanDouble(startRow, stepNextBytes(HBridge.toBytes(startRow)), func)
  }

  def scanRow(startRow: Any, endRow: Any, func: (Result) => Unit) {
    table.getScanner(new Scan(HBridge.toBytes(startRow), HBridge.toBytes(endRow)))
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
      for (
        item <- rs;
        keyvalue <- item.raw()
      ) yield (Bytes.toString(keyvalue.getRow), Bytes.toString(keyvalue.getQualifier), Bytes.toLong(keyvalue.getValue))
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

      for (
        item <- rs;
        keyvalue <- item.raw()
      ) yield (Bytes.toString(keyvalue.getRow), Bytes.toString(keyvalue.getQualifier), getValueByType(Bytes.toString(keyvalue.getQualifier), keyvalue.getValue))

    } finally {
      rs.close()
    }
  }

  def scanRowWithFilterList(rowExp: String, qualifierExp: String) = {
    var filters = new java.util.ArrayList[Filter]()
    val typedQualifer = qualifierExp + ".*"
    val rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(rowExp))
    filters.add(rowFilter)
    val qualiferFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(typedQualifer))
    filters.add(qualiferFilter)
    val filterList: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters)
    scanRowWithFilter(filterList)
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
    var nextBytes = new Array[Byte](bytes.length)
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

  def setAutoFlush(flushState: Boolean) = table.setAutoFlush(flushState)
  def isAutoFlush: Boolean = table.isAutoFlush
  def commit = table.flushCommits()

  def close = table.close

}