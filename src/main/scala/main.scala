import scala.collection.mutable.ListBuffer

import java.lang.Thread
import java.util.concurrent.ConcurrentLinkedQueue

import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.browser.Browser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import net.ruippeixotog.scalascraper.model._
import net.ruippeixotog.scalascraper.scraper.ContentParsers

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver._
import org.apache.spark.storage._
import scala.collection.immutable.ArraySeq



object SparkyCrawler extends App {

        //Setting up Spark
        val spark = SparkSession.builder()
        .master("local[2]")
        .appName("SparkyCrawler")
        .config("spark.executor.memory", "1g")
        .getOrCreate();

        val sc = spark.sparkContext
        val ssc = new StreamingContext(sc, Seconds(10))

        sc.setLogLevel("ERROR")
        sc.setCheckpointDir("./stream")

        val streamer = new ScrapsStreamer()
        val stream = ssc.receiverStream(streamer)

        println("Expect")


        val filterWords = (w: String) => !List("a", "the", "is", "i", "are", "we", "he", "his", "they", "she", "her", "their", "them", "and", "in", "to", "an", "of", "that", "on", "will", "with", "by", "be", "as", "for", "it", "no", "yes", "said", "have", "would", "was", "not", "your", "after", "or", "out", "at", "from", "there", "here", "-", "more", "how", "but", "up", "this", "new", "has", "about", "if", "our", "you", "were", "said", "can", "said.", "could", "all", "who", "which", "what", "had", "into", "also", "--", "one", "than", "then", "been", "like", "over", "some", "when", "share", "—", "below", "/", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "times", "while", "off", "him", "says", "so", "during", "get", "two", "its", "other", "us", "me", "first", "now", "check", "january", "february", "march", "april", "may", "june", "july", "august", "my", "through", "october", "back", "just", "people", "time", "work", "september", "november", "december").contains(w)

        var previousTop : Array[(String, Int)] = Array()

        val wordsFreq = stream.flatMap(s => {s.split("\\s")})
            .map(_.toLowerCase())
            .map(_.replaceAll("('|\"|“|,|\\.|’)", ""))
            .filter(filterWords)
            .map(w => (w, 1))
            .reduceByKey(_ + _)
            .updateStateByKey(increaseCount _)
            // .reduceByKeyAndWindow(_ + _, Seconds(10), Seconds(10))
            .foreachRDD(rdd => {
                println("News so far...")
                val top : Array[(String, Int)] = rdd.collect().toList.sortBy(el => {el._2}).reverseIterator.take(10).toArray
                if(top.sameElements(previousTop)){
                    println("No updates to the top 10 list yet")
                }
                else {
                    if(!top.isEmpty){
                        previousTop = top.clone()
                        for (i <- 1 to 10){
                            println("["+i+"] "+ top(i - 1))
                        }
                    }
                    else{
                        println("Still waiting on crawler")
                    }
                }
            })
        
        ssc.start()
        ssc.awaitTermination()

        sc.stop

        def increaseCount(newValue: Seq[Int], count: Option[Int]): Option[Int] = {
            count match {
                case Some(c) => {
                    newValue.reduceOption(_ + _) match {
                        case Some(nv) => Some(c+nv)
                        case None => Some(c)
                    }
                }
                case None => {
                    newValue.reduceOption(_ + _) match {
                        case Some(nv) => Some(nv)
                        case None => Some(0)
                    }
                }
            }
        }
}

class ScrapsStreamer() extends Receiver[String](StorageLevel.MEMORY_ONLY_SER) {
    def onStart() : Unit = {
        try{
            val thread = new Thread() {
                // needed some thread-safety
                val buf : ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue[String]
                override def run() : Unit = { 
                    val browser = JsoupBrowser.typed()

                    //Using actor system with 2 actors: crawler, scraper
                    //Could have way more actors but felt no need to scale in the current state
                    //of software. They all synchronize writing to the same queue
                    val system = ActorSystem("crawler")
                    val scraper : ActorRef = system.actorOf(Props(new Scraper(browser, buf)))
                    val crawler : ActorRef = system.actorOf(Props(new Crawler(scraper, browser)))
                    
                    //Before parallelization happens get some links
                    val startLinks = getStarterLinks("https://news.google.com/topstories", browser)

                    //Starting the show
                    crawler ! startLinks

                    while(!isStopped){
                        if (!buf.isEmpty()) {
                            store(buf.iterator)
                            buf.clear()
                        }
                    }
                }
                def getStarterLinks(newsPage: String, browser: Browser) : Iterable[String] = {
                    val consentPage = browser.get(newsPage)
                    val form = consentPage >> element("form") >> elementList("input")
                    val formMapping : Map[String, String] = form.filter(_.hasAttr("name")).map(e => Map(e.attr("name") -> e.attr("value"))).flatten.toMap
                    val nextPage = browser.post("https://consent.google.com/s", formMapping)
                    (nextPage >> "a").filter(a => a.hasAttr("href"))
                        .map(a => a.attr("href"))
                        .filter(s => (s.contains("/articles/") || s.contains("/publications/")))
                        .map(s => s.replaceFirst(".", "https://news.google.com"))
                }
            }
            thread.start()
        } catch {
            case e : Throwable => reportError("[ScrapsStreamer ERROR!]", e)
        }
    }
    def onStop() : Unit = {
    }
}

class Crawler(val scraper: ActorRef, val browser: Browser) extends Actor {
    import context._

    def receive = {
        case links: Iterable[String] => {
            try {
                crawlLinks(links)
            } catch {
                case _  => {} //ignore connection errors, filtering errors, whatever errors
            }
            
        }
    }

    def crawlLinks(startingFrom: Iterable[String]) : Unit = {
        var aggrVisitable : ListBuffer[String] = ListBuffer()
        var aggrLinks : ListBuffer[String] = ListBuffer()
        startingFrom.foreach(l => {
                val (visitable, newLinks) = (browser.get(l) >> elementList("a"))
                    .filter(_.hasAttr("href"))
                    .map(_.attr("href"))
                    .filterNot(s => List("policies.google.com", "about.google", "www.google.com", "accounts.google.com", "support.google.com", "play.google.com", "/topics/", "itunes.apple", "openfolio.com").contains(s))
                    .distinct
                    .partition(s => s.contains("http"))
                scraper ! self
                scraper ! visitable
                self ! newLinks.map(s => {
                    s.replaceFirst(".", "https://news.google.com")
                })
        })
    }
}

class Scraper(val browser: Browser, val buffer: ConcurrentLinkedQueue[String]) extends Actor {
    import context._
    var crawler : ActorRef = ActorRef.noSender

    def receive = {
        case links: Iterable[String] => {
            val scraps = scrape(links)
            scraps.foreach(buffer.add(_))
        }
        case a: ActorRef => {
            crawler = a
        }
    }
    def scrape(links: Iterable[String]) : Iterable[String] =  {
        val documents : Iterable[Document] = links.map(l =>{
            try {
                Some(browser.get(l))
            } catch {
                case _ : Throwable => None
            }
        }).flatten
        val newLinks = documents
            .map(d => d >?> extractor("div a[href^=\"https\"]"))
            .flatten
            .map(_ >?> attr("href"))
            .map(_.getOrElse(""))
            .filterNot(_.isEmpty())
        if(!newLinks.isEmpty){
            crawler ! newLinks
        }
        documents.map(d => d >> elementList("div"))
            .flatten
            .filter(d => d.hasAttr("class"))
            .filter(d => {
                val classVal = d.attr("class")
                classVal.contains("Article") || classVal.contains("article") || classVal.contains("Paragraph") || classVal.contains("paragraph")
            })
            .map(d => d >?> allText)
            .map(_.getOrElse(""))
            .filterNot(_.isEmpty())
    }
}