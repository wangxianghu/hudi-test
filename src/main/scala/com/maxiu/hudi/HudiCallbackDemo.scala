package com.maxiu.hudi

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.{DataGenerator, _}
import org.apache.hudi.config.HoodieWriteCommitCallbackConfig
//import org.apache.hudi.config.{HoodieWriteCommitCallbackConfig}
import org.apache.spark.sql.SaveMode._

import scala.collection.JavaConversions._
import org.apache.spark.sql.SparkSession

object HudiCallbackDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("HudiDemo")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    val tableName = "flink_integration"
    val basePath = "file:///tmp/hudi_trips_cow"
    val dataGen = new DataGenerator

    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(HoodieWriteCommitCallbackConfig.CALLBACK_ON, "true").
      option(HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_URL_PROP, "https:xxxxxxxxx").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option("hoodie.table.name", tableName).
      mode(Overwrite).
      save(basePath)

    spark.close()

  }

}
