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

object Test {

  def main(args: Array[String]){
    val src = Source.fromFile("beevolve.csv")
    readData(src).take(4).toList
  }

  case class Data(rating:String, summary:String, time:String, tpe:String, 
    description:String, reviewer:String, item:String, itemName:String, 
    itemUrl:String, itemPhoto:String, context:String)

  def readData(src: Source):Iterator[Data] = {
    val csv = readCSV(src)
    csv.next
    csv flatMap {line => 
      if(line.length == 11){
        Some(Data(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10)))
      } else {
        println("Incomplete fields:" + line.length + ":"+ line)
        None
      }
    }
  }

  def readCSV(src: Source):Iterator[Seq[String]] = src.getLines map readLine

  def readLine(s: String) = (Some(s):Option[String]).unfoldLeft(None)(readValue).reverse

  val patterns = List[(Regex, Match => (String, Boolean))](
    ("([^,\"]*)(,|$)".r, (m : Match) => (m.group(1), m.group(2) != ",")),
    ("\"((?:[^\"]|\"\")*)\"(,)".r, (m : Match) => (m.group(1), false)),
    ("\"((?:[^\"]|\"\")*\"?)($)".r, (m  : Match) => (m.group(1), true))
  )

  def readValue(so: Option[String]):(String, Option[String]) = {
    val s = so.get
    patterns flatMap { 
      case (r, f) => r.findPrefixMatchOf(s) map { m => 
        val (s_, eol) = f(m)
        (s_, if(eol) None else Some(s.substring(m.end)))
      }
    } first
  }
}
