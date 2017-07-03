package org.rubigdata

import nl.surfsara.warcutils.WarcInputFormat
import org.jwat.warc.{WarcConstants, WarcRecord}
import org.apache.hadoop.io.LongWritable;
import org.apache.commons.lang.StringUtils;


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.InputStreamReader;
reset( lastChanges= _.
      set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" ).
      set( "spark.kryo.classesToRegister", 
          "org.apache.hadoop.io.LongWritable," +
          "org.jwat.warc.WarcRecord," +
          "org.jwat.warc.WarcHeader" )
      )



object RUBigDataApp {
	def main(args: Array[String]) {

  val warcfile = "/data/public/common-crawl/crawl-data/CC-MAIN-2016-07/segments/1454701145519.33/warc/CC-MAIN-20160205193905-00000-ip-10-236-182-209.ec2.internal.warc.gz"
  val conf = new SparkConf().setAppName("RUBigDataApp")
  val sc = new SparkContext(conf)
  val stopwordsf = sc.textFile("stopwords.txt")

  /* ... new cell ... */

  val warcf = sc.newAPIHadoopFile(
                warcfile,
                classOf[WarcInputFormat],               // InputFormat
                classOf[LongWritable],                  // Key
                classOf[WarcRecord]                     // Value
      )

  /* ... new cell ... */

  //Function that checks whether a string is a wikipedia page
  def wikiSearch(str: String): Boolean = {
       str.startsWith("https://en.wikipedia.org") || str.startsWith("http://en.wikipedia.org")
  }

  /* ... new cell ... */

  //Function that returns the title of content
  def getTitle(str:String):String = {
    return (str.split(" - Wikipedia")(0))
  }

  /* ... new cell ... */

  //Function that checks whether a page is not a category page
  def notCategory(str:String):Boolean = {
    !str.startsWith("Category:")
  }

  /* ... new cell ... */

  //Utility to access payloads

  def getContent(record: WarcRecord):String = {
    val cLen = record.header.contentLength.toInt
    //val cStream = record.getPayload.getInputStreamComplete()
    val cStream = record.getPayload.getInputStream()
    val content = new java.io.ByteArrayOutputStream();
  
    val buf = new Array[Byte](cLen)
    
    var nRead = cStream.read(buf)
    while (nRead != -1) {
      content.write(buf, 0, nRead)
      nRead = cStream.read(buf)
    }
  
    cStream.close()
    
    content.toString("UTF-8");
  }

  /* ... new cell ... */

  //Function that converts html to text
  import java.io.IOException;
  import org.jsoup.Jsoup;
  def HTML2Txt(content: String) = {
    try {
      Jsoup.parse(content).text().replaceAll("[\\r\\n]+", " ")
    }
    catch {
      case e: Exception => throw new IOException("Caught exception processing input row ", e)
    }
  }

  /* ... new cell ... */

  //Function that converts the wikilinks to text contents
  val warc = warcf.
    filter{ _._2.header.warcTypeIdx == 2 /* response */ }.
    filter{ x => wikiSearch(x._2.header.warcTargetUriStr) }.
    map{wr => ( wr._2.header.warcTargetUriStr, HTML2Txt(getContent(wr._2)) )}.cache()

  /* ... new cell ... */

  //Maps the existing page Titles
  val pageArray = warc.map{ tt => (getTitle(tt._2))}.
    filter{ x => notCategory(x) }.
    reduce( _ + " " + _ )

  /* ... new cell ... */

  val stopWords = stopwordsf.flatMap(text => text.split("/n")).filter(_ != "").reduce( _ + " " + _)

  /* ... new cell ... */

  //Wordcount making lowercases and removing "junk"
  val allWords = warc.flatMap(text => text._2.split(" "))
                .map(w => w.toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
                .filter(_ != "")
                .map(w => (w,1))
                .reduceByKey( _ + _ )
                .filter(x => !stopWords.split(" ").contains(x._1))

  /* ... new cell ... */

  val topWords = allWords.takeOrdered(((allWords.count )/ 100).toInt)(Ordering[Int].reverse.on(x=>x._2))

  /* ... new cell ... */

  val candidates = topWords.filter( x => !pageArray.split(" ").contains(x._1)).cache()

  /* ... new cell ... */

  candidates.take(100)
	}
}
                  
