package finstream.utils

/*
 * The various data models used in the application are defined here.
 * @author: arvind-rs
 * @date: 04/01/2018
 */

case class FetcherResponse(contentType: String, response: String)
case class HTMLParserResponse(useCase: String, htmlScrapeDataList: List[HTMLScrapeData])
case class ScrapeConfig(useCase: String, classifier: XpathMap, xpathList: List[XpathMap], normalizerList: List[RegexPatternMap])
case class HTMLScrapeData(dataPoint: String, data: String)
case class XpathMap(name: String, xpath: String)
case class RegexPatternMap(name: String, pattern: String)