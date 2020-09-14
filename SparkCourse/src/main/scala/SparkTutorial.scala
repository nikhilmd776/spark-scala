import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object SparkTutorial {
  def main (args:Array[String]):Unit = {
    val conf = new SparkConf().setAppName("MySpark").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()
    val df = spark.read.csv("F://spark-scala//SampleFiles//test.csv")
    df.show(false)
    val rdd = sc.parallelize(List(1,2,5,9))
    val x = rdd.map(x=>x*x)
    x.foreach(println)
  }
}
