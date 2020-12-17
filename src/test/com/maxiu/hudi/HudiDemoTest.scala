package com.maxiu.hudi

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession
import org.junit._

import scala.collection.JavaConversions._

@Test
class HudiDemoTest extends Assert {

  var jsc: SparkContext = _
  var spark: SparkSession = _
  val tableName = "flink_integration"
  //  val basePath = "hdfs://localhost:8020/hudi/test/data/timeline01"
  //  val basePath = "hdfs://localhost:8020/hudi/flink-integration/table01"
  val basePath = "file:///tmp/hudi_trips_cow_01"
  var dataGen: DataGenerator = _

  @Before
  def before(): Unit = {
    spark = SparkSession.builder()
      .master("local[4]")
      .appName("HudiDemoTest")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    dataGen = new DataGenerator
  }

  @After
  def after(): Unit = {
    spark.close()
  }

  @Test
  def insert(): Unit = {
    val inserts = convertToStringList(dataGen.generateInserts(10000))

    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "ts:TIMESTAMP").
      option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.CustomKeyGenerator").
      option("hoodie.deltastreamer.keygen.timebased.timestamp.type", "EPOCHMILLISECONDS").
      option("hoodie.deltastreamer.keygen.timebased.output.dateformat", "yyyy/MM/dd").
      option(TABLE_NAME, tableName).
//            mode(Append).
      mode(Overwrite).
      save(basePath)
  }

  @Test
  def insertInLoop(): Unit = {
    for (a <- 1 to 20000) {
      insert()
      Thread.sleep(5000)
    }
  }

  @Test
  def query(): Unit = {
    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

//    spark.sql("select * from  hudi_trips_snapshot ").show()
        spark.sql("select distinct(_hoodie_partition_path) from  hudi_trips_snapshot ").show()
    //    spark.sql("select count(_hoodie_commit_time) from  hudi_trips_snapshot where _hoodie_commit_time >= 20201208142810 and _hoodie_commit_time <20201208142814").show()

  }

  @Test
  def update(): Unit = {
    val inserts = convertToStringList(dataGen.generateInserts(10))
    val updates = convertToStringList(dataGen.generateUpdates(10))
    val df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)
  }

  @Test
  def delete(): Unit = {
    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

    spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
    // fetch two records to be deleted
    val ds = spark.sql("select uuid, partitionpath from hudi_trips_snapshot").limit(2)

    // issue deletes
    val deletes = dataGen.generateDeletes(ds.collectAsList())
    val df = spark.read.json(spark.sparkContext.parallelize(deletes, 2))

    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY, "delete").
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    // run the same read query as above.
    val roAfterDeleteViewDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")

    roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
    // fetch should return (total - 2) records
    spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
  }

  @Test
  def incrementalQuery(): Unit = {
  }

  @Test
  def pointInTimeQuery(): Unit = {
    val tripsPointInTimeDF = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20201209141521")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, "20201209141530")
      .load(basePath)
    tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")
    spark.sql("select * from hudi_trips_point_in_time").show()
  }


}
