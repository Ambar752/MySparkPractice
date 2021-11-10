package com.utils.morganstan

import org.apache.spark.sql.{DataFrame, SparkSession}

object csvUtils {
  def readCsvFile(spark: SparkSession, csvPath:String) : DataFrame = {
    spark.read
      .option("inferSchema","true")
      .option("header","true")
      .csv(csvPath);
  }

  def writeCsvFile(spark: SparkSession,
                   inDataFrame: DataFrame,
                   mode: String,
                   csvOutPath: String) : Unit = {
    inDataFrame.write
      .mode(mode)
      .format("csv")
      .save(csvOutPath);
  }
}
