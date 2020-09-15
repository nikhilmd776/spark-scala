package main.scala

object SparkTutorial {
  def main (args:Array[String]):Unit = {
    val spark = SparkConnection.sparkSession
    val df = spark.read.csv("F://spark-scala//SampleFiles//test.csv")
    df.show(false)
    val sc = SparkConnection.sparkContext
    val rdd = sc.parallelize(List(1,2,5,9))
    val x = rdd.map(x=>x*x)
    x.foreach(println)

    val z = List(1,2,3).fold(1){_*_}
    println(z)
  }
}
