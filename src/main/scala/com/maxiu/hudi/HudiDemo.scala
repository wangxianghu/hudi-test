package com.maxiu.hudi

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.QuickstartUtils.{DataGenerator, convertToStringList}
import org.apache.spark.sql.SparkSession
import org.apache.hudi.QuickstartUtils._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._

object HudiDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("HudiDemo")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    val tableName = "flink_integration"
    val basePath = "hdfs://localhost/tmp/hudi_trips_cow_01"
    val dataGen = new DataGenerator

    val inserts = convertToStringList(dataGen.generateInserts(10))
    var df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Overwrite).
      save(basePath)

//    // spark-shell
    val updates = convertToStringList(dataGen.generateUpdates(10))
     df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()



    spark.close()
  }
}
