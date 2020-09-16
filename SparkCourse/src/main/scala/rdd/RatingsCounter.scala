package main.scala.rdd

import main.scala.SparkConnection

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
  def main(args:Array[String]):Unit = {
    val sc = SparkConnection.sparkContext
    val lines =  sc.textFile("F://spark-scala//SampleFiles//ml-100k//u.data")
    val ratings = lines.map(x=>x.toString().split("\t")(2))
    val results = ratings.countByValue()
    val sortedResults = results.toSeq.sortBy(_._1)
    sortedResults.foreach(println)
  }

}
