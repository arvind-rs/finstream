
/*
 * Data pipeline to collect, transform and store financial price and social media text data
 * for a given list of stocks using Spark streaming.
 * @author: arvind-rs
 * @date: 03/31/2018
 */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.log4j.{Level, Logger}

object FinStream {

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.ERROR)

		val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Test")
		val ssc = new StreamingContext(sparkConf, Seconds(1))

		val lines = ssc.socketTextStream("localhost",9999)

		val words = lines.flatMap(_.split(" "))
		val pairs = words.map((_, 1))
		val wordCount = pairs.reduceByKey(_ + _)

		wordCount.print()

		ssc.start()
		ssc.awaitTermination()
	}
}