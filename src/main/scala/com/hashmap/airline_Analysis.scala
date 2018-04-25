package com.hashmap

import java.io
import javax.annotation.Resource

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.expr

object airline_Analysis extends App {
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Airline Analysis")
      .config("spark.master", "local")
      .getOrCreate()

  def read(resource: String): DataFrame = {

    val schema = dfSchema
    val dataFrame = spark.read.schema(schema).option("header", value = true).csv(resource)
    dataFrame
  }


  def dfSchema: StructType = {
    val fields = Array(StructField("YEAR", IntegerType, nullable = true),
      StructField("MONTH", IntegerType, nullable = true),
      StructField("DAY_OF_MONTH", IntegerType, nullable = true),
      StructField("DAY_OF_WEEK", IntegerType, nullable = true),
      StructField("CARRIER", StringType, nullable = true),
      StructField("FL_NUM", IntegerType, nullable = true),
      StructField("ORIGIN", StringType, nullable = true),
      StructField("DES", StringType, nullable = true),
      StructField("DEP_TIME", IntegerType, nullable = true),
      StructField("DEP_DELAY", IntegerType, nullable = true),
      StructField("ARR_TIME", IntegerType, nullable = true),
      StructField("ARR_DELAY", IntegerType, nullable = true),
      StructField("CANCELLED", IntegerType, nullable = true),
      StructField("CANCELLATION_CODE", IntegerType, nullable = true),
      StructField("AIR_TIME", IntegerType, nullable = true),
      StructField("DISTANCE", IntegerType, nullable = true))
    val schema = StructType(fields)
    schema
  }

  def delayedCount(df: DataFrame): Long = {
    df.createOrReplaceTempView("data")
    val delayed_data = spark.sql("select DEP_DELAY from data where (DEP_DELAY>0)")
    delayed_data.count()
  }

  def onTimeCount(df:DataFrame):Long={
    df.createOrReplaceTempView("data")
    val onTime_data: DataFrame =spark.sql("select DEP_DELAY from data where (DEP_DELAY==0)")
    onTime_data.count()
  }

  def delayByDayOfWeek(df:DataFrame):DataFrame={
    df.createOrReplaceTempView("data")
    val byDOW=spark.sql("select DEP_DELAY,DAY_OF_WEEK from data where DEP_DELAY>0")
    val delayByWOD: DataFrame =byDOW.groupBy("DAY_OF_WEEK").count()
    delayByWOD.withColumnRenamed("count","delayedCount")
  }

  def onTimeByDayOfWeek(df:DataFrame):DataFrame={
    df.createOrReplaceTempView("data")
    val byDOW=spark.sql("select DEP_DELAY,DAY_OF_WEEK from data where DEP_DELAY=0")
    val onTimeByWOD: DataFrame =byDOW.groupBy("DAY_OF_WEEK").count()
    onTimeByWOD.withColumnRenamed("count","onTimeCount")
  }

  def totalCountByDOW(df:DataFrame):DataFrame={
    df.createOrReplaceTempView("data")
    val total: DataFrame =spark.sql("select DEP_DELAY,DAY_OF_WEEK from data")
    val totalCount=total.groupBy("DAY_OF_WEEK").count
    totalCount.withColumnRenamed("count","totalCountByDOW")
  }

  def delayPercentageCalculation(delay:DataFrame,onTime:DataFrame,total:DataFrame):DataFrame={
    val delayTotalDF: DataFrame =delay.join(total, "DAY_OF_WEEK")
    val joinedDF: DataFrame =delayTotalDF.join(onTime,"DAY_OF_WEEK")
    val delayedPercentage: DataFrame =joinedDF.withColumn("DELAY_PER",expr("(delayedCount/totalCountByDOW)*100"))
    val onTimePercentage: DataFrame =delayedPercentage.withColumn("ONTIME_PER",expr("(onTimeCount/totalCountByDOW)*100"))
    val ratio=onTimePercentage.withColumn("Ratio",expr("(delayedCount/onTimeCount)"))
    ratio
  }

  def overallTotalCount(df:DataFrame):Long={
    df.createOrReplaceTempView("data")
    val totalCount: Long =spark.sql("select DEP_DELAY from data").count()
    totalCount
  }

  def overallDelayPercentage(delayCount:Long,overAllCount:Long):Long={
    val overAllDelayedPer: Long =(delayCount/overAllCount)*100
    overAllDelayedPer
  }

  def overallOnTimePercentage(onTimeCount:Long,overAllCount:Long): Long ={
    val overAllOnTimePer: Long =(onTimeCount/overAllCount)*100
    overAllOnTimePer
  }
//  val df=read("C:\\Users\\hashmap\\Downloads\\airline_data\\train_df.csv")
//  val x=delayedCount(df)
//  val y=onTimeCount(df)
//  val t=overallTotalCount(df)
//  val s=overallDelayPercentage(x,t)
//  val a=overallOnTimePercentage(y,t)
//  println(s,a)

}

