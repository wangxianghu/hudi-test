package com.maxiu.hudi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class HoodieSparkEngineContext extends HoodieEngineContext {
  private static JavaSparkContext jsc;

  // tmp
  static {
    SparkConf conf = new SparkConf()
        .setMaster("local[4]")
        .set("spark.driver.host","localhost")
        .setAppName("HoodieSparkEngineContext");

    jsc = new JavaSparkContext(conf);
  }

  @Override
  public <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) {
    return jsc.parallelize(data, parallelism).map(func::call).collect();
  }

  public static void main(String[] args) {

  }


}
