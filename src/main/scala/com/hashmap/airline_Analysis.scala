package com.hashmap

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.bround

object airline_Analysis{

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Airline Analysis")
      .master("local")
      .config("spark.sql.warehouse.dir","hdfs:///warehouse")
      .enableHiveSupport()
      .getOrCreate()


  def main(args: Array[String]): Unit = {
    val filePath="hdfs:///test_df.csv"
    val data=read(filePath)
    val percentageDf=percentageCalculation(delayByDayOfWeek(data),onTimeByDayOfWeek(data))
    percentageDf.write.format("hive").mode(SaveMode.Append).saveAsTable("airline.airline_analysis")
    println("Table Added")
  }

  def read(filePath:String):DataFrame={
    val df: DataFrame = spark.
      read.
      format("csv").
      option("header", "true").
      option("inferSchema","true").
      load(filePath)
    df
  }

  def delayedCount(df: DataFrame): Long = {
    df
      .select("ARR_DELAY")
      .where("ARR_DELAY!='NA'")
      .where("ARR_DELAY!=0")
      .count()
  }

  def onTimeCount(df:DataFrame):Long={
    df
      .select("ARR_DELAY")
      .where("ARR_DELAY!='NA'")
      .where("ARR_DELAY=0")
      .count()
  }

  def delayByDayOfWeek(df:DataFrame):DataFrame={
    val delayByWOD=df
      .select("ARR_DELAY","DAY_OF_WEEK")
      .where("ARR_DELAY!='NA'")
      .where("ARR_DELAY!=0")
      .groupBy("DAY_OF_WEEK")
      .count()
      .withColumnRenamed("count","delayedCount")
    delayByWOD
  }

  def onTimeByDayOfWeek(df:DataFrame):DataFrame={
    val onTimeByWOD=df
      .select("ARR_DELAY","DAY_OF_WEEK")
      .where("ARR_DELAY!='NA'")
      .where("ARR_DELAY=0")
      .groupBy("DAY_OF_WEEK")
      .count()
      .withColumnRenamed("count","onTimeCount")
    onTimeByWOD
  }

  def percentageCalculation(delayDF:DataFrame,onTimeDF:DataFrame):DataFrame={
    val percentageDF=delayDF
      .join(onTimeDF, "DAY_OF_WEEK")
      .withColumn("totalCountByDOW",expr("delayedCount+onTimeCount"))
      .withColumn("delay_PER",bround(expr("(delayedCount/totalCountByDOW)*100"),2))
      .withColumn("onTime_PER",bround(expr("(onTimeCount/totalCountByDOW)*100"),2))
      .withColumn("ratio",bround(expr("(delayedCount/onTimeCount)"),2))
      .orderBy("DAY_OF_WEEK")
    percentageDF
  }

  def overAllDelayPercentage(delayCount:Long,onTimeCount:Long):Double={
      val totalCount=delayCount+onTimeCount
      (delayCount*100)/totalCount.toDouble
  }

  def overAllOnTimePercentage(delayCount:Long,onTimeCount:Long): Double ={
    val totalCount=delayCount+onTimeCount
    (onTimeCount*100)/totalCount.toDouble
  }
}

