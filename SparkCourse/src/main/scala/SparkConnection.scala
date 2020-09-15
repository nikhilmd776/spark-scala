package main.scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkConnection {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("MySpark").setMaster("local[*]")

  def sparkSession:SparkSession ={
    SparkSession.builder().config(conf).getOrCreate()
  }

  def sparkContext:SparkContext ={
    sparkSession.sparkContext
  }
}
