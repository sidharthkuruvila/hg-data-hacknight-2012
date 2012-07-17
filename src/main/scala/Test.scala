import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import scala.util.matching.Regex
import scala.util.matching.Regex.Match

import io.Source
import util.Unfoldable._
import util.Util._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.ISODateTimeFormat

object Test {

  def main(args: Array[String]){
    val src = Source.fromFile("beevolve.csv")
    readData(src) foreach {x => print("")}
  }

  case class Data(rating:String, summary:String, time:Option[DateTime], tpe:String, 
    description:String, reviewer:String, item:String, itemName:String, 
    itemUrl:String, itemPhoto:String, context:String)

  def readData(src: Source):Iterator[Data] = {
    val csv = readCSV(src)
    csv.next
    csv.zipWithIndex flatMap { case (line, idx) => 
      def l(i:Int): String = if(i >= line.length) "" else line(i)
      try {Some(Data(l(0), l(1), parseDateTime(l(2)), l(3), l(4), l(5), l(6), l(7), l(8), l(9), l(10)))}
      catch { case e => println("Failed at line " + (idx+1)); throw e}
    }
  }

  def readCSV(src: Source):Iterator[Seq[String]] = src.getLines map readLine

  def readLine(s: String) = try {(Some(s):Option[String]).unfoldLeft(None)(readValue).reverse} catch { case e: java.util.NoSuchElementException => Vector(); }

  val patterns = List[(Regex, Match => (String, Boolean))](
    ("([^,\"]*)(,|$)".r, (m : Match) => (m.group(1), m.group(2) != ",")),
    ("\"((?:[^\"]|\"\")*)\"(,)".r, (m : Match) => (m.group(1), false)),
    ("\"((?:[^\"]|\"\")*)\"?($)".r, (m  : Match) => (m.group(1), true))
  )

  def readValue(so: Option[String]):(String, Option[String]) = {
    val s = so.get
    try{
    patterns flatMap { 
      case (r, f) => r.findPrefixMatchOf(s) map { m => 
        val (s_, eol) = f(m)
        (s_, if(eol) None else Some(s.substring(m.end)))
      }
    } first
    } catch {
      case e:java.lang.StackOverflowError => println("stack overflow exception in parsing"); ("", None)
    }
  }

  def dp(s:String) = DateTimeFormat.forPattern(s)

  val dateFormats = List(
    dp("MM/dd/yy"),
    dp("yyyy-dd-MM"),
    dp("MMMMM d, yyyy"),
    dp("MMMMM  d, yyyy"),
    dp("yyyy-MM-dd'T'HH:mm:ss"),
    dp("d MMMMM, yyyy"),
    dp("EEE MMM dd yyyy"),
    dp("yyyyMMdd"),
    dp("yyyy-mm-dd"),//Someone messed up their format string
    dp("yyyyMMdd'T'HHmmZ"),
    dp("yyyy-MM-dd'T'HH:mm"),
    dp("yyyy-MM-dd HH:mm:ss"),
    dp("yyyy-MM-dd'T'HH:mm'Z'"),
    dp("yyyy-MM-dd HH:mm"),
    dp("'Review Date': dd/MM/yyyy"),
    dp("yyyymmdd'T'HHmm"),
    dp("yyyy/mm/dd"),
    dp("HH:mm dd MMMMM yyyy"),
    dp("dd/MM/yyyy HH:mm:ss"),
    dp("yyyy-MM-dd'T'HH:mm:ssZ"),
    dp("dd MMMMM yyyy"),
    dp("yyMdd't'HHmmss"),
    dp("yyyy-MM-dd'T'HH:mmZ"),
    dp("MMM dd yyyy"),
    dp("yyyy-MM-dd'MST'HH:mm:ss:-25200"),
    dp("yyyy-MM-dd'MDT'HH:mm:ss:-21600"),
    dp("yyyy-MM-dd'CDT'HH:mm:ss:-18000"),
    //2009-06-18CDT10:26:58:-18000
    dp("'Submitted': M/d/yy hh:mm a"),
    dp("MMMMM yy"),
    dp("yyyy-MM-dd HH:mm:ss ZZZ"),
    dp("yyyy-MM-dd HH:mm:ss.0 'PST'"),
    dp("yyyy-MM-dd HH:mm:ss.0 'PDT'"),
    dp("dd MMM yy, HH:mm"),
    dp("MMM\240dd,\240yyyy"), // Uses nbsps
    dp("MMM. dd, yyyy"),
    dp("EEE MMM dd HH:mm:ss ZZZ yyyy"),
    dp("EEEEE MMM dd, yyyy"),
    dp("yyyy-MM-DD"),
    dp("yyyyMMdd'T'HHmm-0000:00"),
    dp("dd/MM/yyyy"),
    dp("dd MMM yyyy HH:mm:ss 'EDT'"),
    dp("yyyy.MM.dd"),
    dp("MM/yyyy")
    /*,
    DateTimeFormat.forPattern("MMM dd, yyyy")*/
    /*,
    ISODateTimeFormat.dateTime()*/
  )

  def parseDateTime(s: String):Option[DateTime] = {
    //Really bad dates ones i need to remove 
    
    if(List(
      "Apr 1st, 2004", "06 f?vrier 2008", "08729t010339",
      "04 F?vrier 2009", "%Y/%M/%D-0600", "09 ???????, ??????? / 21:45", "20061032", 
      "18. Mar 2003", "20090223T1858-0000:16",
        ",14/01/2009", "Posted 6 months ago (01/01/09)", ",17/06/2009", 
        "2009427T1200-0800", "2008226T1200-0800", 
        "2 months",
        "Tue Sep 30 13:20:00 -0500 2008", "Tue Sep 30 08:51:00 -0500 2008", "Thu Aug 07 15:47:00 -0500 2008",
        "Feb 17, 2009 10.05",
        "2009425T1200-0800", "22 D?cembre 2006").contains(s) || s.contains("gravatar.com")){
      None
    } else {
      val d = dateFormats flatMap { x => tryO(x.parseDateTime(s))} lift(0)
      if(s != "" && d == None) {
        parseDateTime(cleanDateString(s))
      }
      d
    }
  }


  val lookupMonth = Map(
    "Janvier" -> "January",
    "Febrero" -> "February",
    "Abril" -> "April",
    "Avril" -> "April",
    "Mai" -> "May",
    "Juin" -> "June",
    "Juillet" -> "July",
    "Julio" -> "July",
    "Agosto" -> "August",
    "Octobre" -> "October",
    "Novembre" -> "November",
    "Noviembre" -> "November",
    "Diciembre" -> "December"
  )
  var badDates = List(
    ("(.+?) [|]".r , (m:Match) => m.group(1)),
    ("\\d+t\\d+".r , (m:Match) => {println("bad date 1 needs to be implemented here"); "10/10/10"}),
    ("(\\w+ \\d+)(th|nd|rd|st)(, \\d+)".r , (m:Match) =>  {m.group(1) + m.group(3)}),
    ("(\\d+\\s+(year|month|day|week|hour)s?\\s*)+ago.*".r, (m:Match) =>  {/*println("bad date 3 needs to be implemented here");*/ "10/10/10"}),
    ("Posted (over|about) \\d+ years? ago \\(([^)]+)\\)".r, (m:Match) =>  m.group(2)),
    ("Posted \\d+ (?:month|day)s? ago \\(([^)]+)\\)".r, (m:Match) =>  m.group(1)),
    
    (",(\\d+/\\d+/\\d+)".r, (m:Match) =>  m.group(1)),
    (("(\\d+ )(" + lookupMonth.keys.mkString("|") + ")( \\d+)").r, (m:Match) =>  m.group(1) + lookupMonth(m.group(2)) + m.group(3))
  )

  def cleanDateString(s:String) =  {
    assert(s != null)
    //Some date strings contain the author of the post
    val out = badDates flatMap { case (r, f) => r.findPrefixMatchOf(s) map f } lift(0) getOrElse {
      println("'"+s+"'")
      throw new RuntimeException();      
    }
    assert(s != out)
    out
  }

  def checkDateFormat(s:String) = {
    if(s == "") false
    else {
      val parts = s.split("/")
      if(parts.length == 3) false 
      else {
        println(s)
        true
      }
    }
  }
}
