package com.morganstan
import com.typesafe.config.ConfigFactory
import com.utils.morganstan.csvUtils.{readCsvFile, writeCsvFile}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object firstSparkCode {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("firstSparkCode")
      .getOrCreate();

    val props = ConfigFactory.load()
    val envProps = props.getConfig(args(0))
    val input_file = envProps.getString("input_path.input_file")
    val output_path = envProps.getString("output_path.output_file")

    spark.sparkContext.setLogLevel("ERROR");

    //val df = readCsvFile(spark,"/var/tmp/data/test.csv");
    val df = readCsvFile(spark,input_file);

    val dfTransformed
       = df.withColumn("AgeAfter5yrs",col("Age")+5);

    dfTransformed.show();
    //Test Line
    writeCsvFile(spark,
                 dfTransformed,
                 "overwrite",
                 output_path);
  }
}
