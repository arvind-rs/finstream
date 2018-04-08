
/*
 * Data pipeline to collect, transform and store financial price and social media text data
 * for a given list of stocks using Spark streaming.
 * @author: arvind-rs
 * @date: 03/30/2018
 */

/*
 * Things to do:
 * 1. Build Fetcher Receiver X
 * 2. Retrofit the scraper code to work with FinStream X
 * 3. Add capabilities to crawl and scrape JSON data from APIs
 * 4. Transform the data into DataFrames
 * 5. Add MySQL db connections
 * 6. Write the data to the MySQL tables
 */

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ 
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import java.util.Properties
import java.sql.{Connection, DriverManager, ResultSet}
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.JdbcRDD
import finstream.receivers._
import finstream.scraper._
import finstream.utils._


object FinStream {

	val masterConfig = "conf/master.conf"
	val scrapeConfigPath = UtilMethods.getMasterConfig.getString("SCRAPE_CONFIG")
	val scrapeConfig = UtilMethods.loadScrapeConfig(scrapeConfigPath)(0)

	val urlList = List("https://www.google.com/search?q=FB", "https://www.google.com/search?q=AMZN","https://www.google.com/search?q=GOOG")
	//val urlList = List("https://www.google.com/search?q=FB")

	val connectionUrl = "jdbc:mysql://localhost:3306/finstream"
	val connectionProperties = new Properties()
	val username = "root"
	val password = "ubuntu"
	connectionProperties.put("user", "root")
	connectionProperties.put("password", "ubuntu")

	case class StockPrice(ticker: String, marketCap: Double, price: Double, exchange: String, ltt: String)

	def main(args: Array[String]) {

		Logger.getLogger("org").setLevel(Level.ERROR)

		val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Test")
		val sc = new SparkContext(sparkConf) 
		val sqlContext = new SQLContext(sc)
		val ssc = new StreamingContext(sc, Seconds(60))

		val fetcherService = ssc.receiverStream(new Fetcher(urlList))
		println("Got data ...")
		//fetcherService.saveAsTextFiles("output/test1","")

		//fetcherService.map(_.contentType).print()
		//val cut: String => String = (s: String) => {s.substring(s.indexOf("W0pUAc fmob_pr fac-l"),s.indexOf("W0pUAc fmob_pr fac-l")+100)}
		//fetcherService.map(x => (x.contentType, 1)).reduceByKey(_ + _).print()
		val htmlScrapeDataDStream = fetcherService.map(x => Scraper.scrapeHTML(x.response, scrapeConfig))
		//println(scrapeConfig)
		//stockPriceData.print()

		

		//stockPriceData.foreachRDD { rdd =>
			//val df = rdd.map(_._2)
			//df.write.mode(SaveMode.append).jdbc(url, "stock_price", connectionProperties)
		//}
		
		val stockPriceDataDStream = htmlScrapeDataDStream.map(_.htmlScrapeDataList).map { rdd =>
			try {
				val dataPointList = List("ticker","market_cap","price","exchange","ltt")
				val list = scala.collection.mutable.ListBuffer[String]()
				for(dataPoint <- dataPointList) {
					val htmlScrapeData = rdd.filter(_.dataPoint.equals(dataPoint))
					val temp = if(htmlScrapeData.size > 0) htmlScrapeData(0).data else ""
					list += temp
				}
				StockPrice(list(0), list(1).replaceAll(",","").toDouble, list(2).replaceAll(",","").toDouble, list(3), list(4))
			} 
			catch {
				case ex: RuntimeException => println(ex)
			}
		}

		import org.apache.spark.sql.SQLContext
		import org.apache.spark.sql.SparkSession
		//val sc = SparkSession.builder.config(sparkConf).getOrCreate
		//import sc.implicits._
		//val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
		//val sqlContext= new org.apache.spark.sql.SQLContext(sparkConf)

		/*val schema = new StructType()
  			.add(StructField("ticker", StringType, true))
  			.add(StructField("market_cap", DoubleType, true))
  			.add(StructField("price", DoubleType, true))
  			.add(StructField("exchange", StringType, true))
  			.add(StructField("ltt", StringType, true))*/

		import sqlContext.implicits._
		Class.forName("com.mysql.jdbc.Driver").newInstance
		stockPriceDataDStream.foreachRDD { rdd =>
			//rdd.ticker
			rdd.foreachPartition { iterator =>
				val conn = DriverManager.getConnection(connectionUrl,username,password)
				val statement = conn.prepareStatement("INSERT INTO stock_price(ticker, market_cap, price, exchange, ltt) values(?,?,?,?,?);")
				for(data <- iterator) {
					try {
						println(" foo " + data)
						val stockPrice = data.asInstanceOf[StockPrice]
						println(" foo " + stockPrice.ticker)
						statement.setString(1, stockPrice.ticker)
						statement.setDouble(2, stockPrice.marketCap)
						statement.setDouble(3, stockPrice.price)
						statement.setString(4, stockPrice.exchange)
						statement.setString(5, stockPrice.ltt)
						statement.executeUpdate
					} catch {
						case ex: RuntimeException => println(ex)
					}
				}
				//iterator.write.mode(SaveMode.Append).jdbc(connectionUrl, "stock_price", connectionProperties)
				conn.close()
			}
			//val dataFrame = sqlContext.createDataFrame(rdd, schema)
			//dataFrame.write.mode(SaveMode.Append).jdbc(connectionUrl, "stock_price", connectionProperties)
		}
		stockPriceDataDStream.print()
		/*val lines = ssc.socketTextStream("localhost",9999)
		val words = lines.flatMap(_.split(" "))
		val pairs = words.map((_, 1))
		val wordCount = pairs.reduceByKey(_ + _)

		//wordCount.print()

		//wordCount.saveAsTextFiles("output/test","")*/

		ssc.start()
		ssc.awaitTermination()
	}
}