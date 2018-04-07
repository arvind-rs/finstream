package finstream.scraper

/*
 * Scraper implementation to scrape content from HTML and JSON strings.
 * @author: arvind-rs
 * @date: 04/01/2018
 */


import org.htmlcleaner.HtmlCleaner
import org.htmlcleaner._
import javax.xml.xpath._
import java.io._
import javax.xml.transform.stream._
import scala.collection.mutable.ListBuffer
import finstream.utils._

object Scraper {

	// Create a HTMLCleaner object
    val cleaner = new HtmlCleaner()

	def scrapeHTML(htmlPage: String, scrapeConfig: ScrapeConfig): HTMLParserResponse = {
		println("scrapeHTML")

    	// Create a ListBuffer to store the scraped data
    	val htmlScrapeDataList = new ListBuffer[HTMLScrapeData]
    	val content = htmlPage

    	// Get the root node
    	val rootNode = cleaner.clean(content)

    	// Get the document
    	val doc = new DomSerializer(new CleanerProperties()).createDOM(rootNode)

    	// Create an Xpath object
    	val xpath = XPathFactory.newInstance().newXPath()

    	// Get the classifier xpath and verify if the scrape config matches the current HTML page
    	val classifierXpath = scrapeConfig.classifier.xpath
    	val nodes = xpath.evaluate(classifierXpath, doc)
    	if (nodes == null || nodes.toString().equals("")) {
      		println("Given classifier did not match the page. Skipping the scraping process for the current HTML page.")
      		return HTMLParserResponse(null, null)
    	}
    
    	try {
      		for (xpathMap <- scrapeConfig.xpathList) {
        
        		// If the classifier matches, extract the data for each datapoint
        		val dataPoint = xpathMap.name.toString()
        		val xpathString = xpathMap.xpath.replaceAll("&quot;","\"")
        		//println(xpathString)
        		val nodes = xpath.evaluate(xpathString, doc)
        		val data = nodes.toString().trim()
        
        		// Combine the datapoint and data together as a single HTMLScrapeData data model
        		val htmlScrapeData = HTMLScrapeData(dataPoint, data)
        		htmlScrapeDataList += htmlScrapeData
      		}
    	} catch {
      		case ex: XPathExpressionException => { 
      			throw new RuntimeException("There was a problem in scraping the HTML. Please check the Xpaths provided.", ex) 
      		}
    	}

    	// Return the scraped data as a HTMLParserResponse
    	return HTMLParserResponse(scrapeConfig.useCase, htmlScrapeDataList.toList)

	}
}