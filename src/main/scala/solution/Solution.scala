package solution

import com.github.tototoshi.csv.{CSVReader => CsvReader, CSVWriter => CsvWriter}
import java.io.File
import org.joda.time.{DateTime, Duration, PeriodType}
import org.joda.time.format.{DateTimeFormat, PeriodFormat}
import scala.io.Source

object Solution extends App {
  /*
    1. Add current request's ip address to correspoinding Map(request time -> Seq(IpAddress)): ip address start times
    2. Increment current request's ip address count in Map(ip -> count): request count
    3. Update current request's ip address lastUpdated time in Map(ip -> lastUpdated)
    4. When a session ends, use Maps to calculate session duration (lastUpdate - request time)
         and request count
    5. Output to file
  */

  val startDateTime = DateTime.now()

  val projectPath = new File(".").getCanonicalPath
  val inputFile = s"$projectPath/input/log.csv"

  case class StatefulData(
    ipRequestCountByIp: Map[String, Int],
    requestDateTimesByIp: Vector[(String, Set[DateTime])]
  )

  val logs = {
    CsvReader
      .open(inputFile)
      .allWithHeaders()
  }

  val inactivityPeriodFilePath = s"$projectPath/input/inactivity_period.txt"
  val bufferedInactivityPeriodFile = Source.fromFile(inactivityPeriodFilePath)
  // NOTE: range is 1 to 86,400
  // TODO: Ensure period is within range
  val inactivityPeriod = { // seconds
    bufferedInactivityPeriodFile
      .getLines
      .toList
      .headOption
      .getOrElse(throw new Exception(s"Could not find an inactive period value in empty file $inactivityPeriodFilePath"))
      .replaceAll(",", "")
      .toInt
  }

  bufferedInactivityPeriodFile.close()

  def getRequiredValueFromRequest(key: String, request: Map[String, String]): String = {
    val rawValue = request.getOrElse(key, throw new Exception(s"Key $key not found in request $request"))

    if (rawValue.isEmpty) {
      throw new Exception(s"Required value for $key not found in request $request")
    }

    rawValue
  }

  def closeInactiveSessions(oldestPossibleEndOfSessionDateTimeOpt: Option[DateTime], statefulData: StatefulData): StatefulData = {
    val StatefulData(_, requestDateTimesByIp) = statefulData
    val ipsToClose = {
      oldestPossibleEndOfSessionDateTimeOpt match {
        case Some(oldestPossibleEndOfSessionDateTime) => {
          requestDateTimesByIp
            .flatMap({ case (ip, requestDateTimes) => {
              val lastRequestDateTime = requestDateTimes.last

              if (lastRequestDateTime.isBefore(oldestPossibleEndOfSessionDateTime)) {
                Some((ip, requestDateTimes.head))
              } else {
                None
              }
            }})
            .toList
            .sortBy(_._2.getMillis)
            .map(_._1)
        }
        case None => {
          requestDateTimesByIp.toSeq
            .map({ case(ip, dateTimes) => (ip, dateTimes.head)})
            .toList
            .sortBy(_._2.getMillis)
            .map(_._1)
        }
      }
    }

    ipsToClose.foldLeft(statefulData)({ case(StatefulData(ipRequestCountByIp, requestDateTimesByIp), ip) => {
      val (targetRequestDateTimesByIp, updatedRequestDateTimesByIp) = {
        requestDateTimesByIp
          .partition({ case(currentIp, _) => currentIp == ip})
      }

      val requestDateTimes = {
        targetRequestDateTimesByIp
          .headOption
          .getOrElse(throw new Exception(s"Could not find the request dateTimes of ip $ip"))
          ._2
      }
      val firstRequestDateTime = requestDateTimes.head
      val lastRequestDateTime = requestDateTimes.last
      val sessionDuration = new Duration(firstRequestDateTime, lastRequestDateTime).getStandardSeconds

      val firstRequestDateTimeString = requestDateTimes.head.toString("yyyy-MM-dd HH:mm:ss")
      val lastRequestDateTimeString = requestDateTimes.last.toString("yyyy-MM-dd HH:mm:ss")
      val requestCount = ipRequestCountByIp.getOrElse(
        ip,
        throw new Exception(s"Could not find the request count of ip $ip")
      )

      outputWriter.writeRow(List(
        ip,
        firstRequestDateTimeString,
        lastRequestDateTimeString,
        if (sessionDuration == 0) 1 else 1 + sessionDuration,
        requestCount
      ))

      StatefulData(
        (ipRequestCountByIp - ip),
        (updatedRequestDateTimesByIp)
      )
    }})
  }

  val dateTimeFormatter = DateTimeFormat.forPattern("yy-MM-dd HH:mm:ss")

  val outputFile = new File(s"$projectPath/output/sessionization.txt")
  val outputWriter = CsvWriter.open(outputFile, append = true)

  var initialIpRequestCountByIp = Map.empty[String, Int]
  var initialRequestDateTimesByIp = Vector.empty[(String, Set[DateTime])]

  // Process each log
  val (
    finalIpRequestCountByIp,
    finalRequestDateTimesByIp
  ) = logs.foldLeft(
    (initialIpRequestCountByIp, initialRequestDateTimesByIp)
  )({ case ((ipRequestCountByIp, requestDateTimesByIp), request) => {
    // TODO: Validate format
    val ip = getRequiredValueFromRequest("ip", request)

    val currentRequestDate = getRequiredValueFromRequest("date", request)
    val currentRequestTime = getRequiredValueFromRequest("time", request)
    val currentRequestDateTimeString = s"$currentRequestDate $currentRequestTime"
    val currentRequestDateTime = dateTimeFormatter.parseDateTime(currentRequestDateTimeString)
    val oldestPossibleEndOfSessionDateTime = currentRequestDateTime.minusSeconds(inactivityPeriod)

    val cik = getRequiredValueFromRequest("cik", request)
    val accession = getRequiredValueFromRequest("accession", request)
    val extention = getRequiredValueFromRequest("extention", request)

    val updatedIpRequestCountByIp = {
      val ipRequestCountOpt = ipRequestCountByIp.get(ip)

      ipRequestCountOpt match {
        case Some(count) => {
          ipRequestCountByIp + (ip -> (count + 1))
        }
        case None => {
          ipRequestCountByIp + (ip -> 1)
        }
      }
    }

    // TODO: Optimize
    val updatedRequestDateTimesByIp: Vector[(String, Set[DateTime])] = {
      val existingRequestDateTimes = requestDateTimesByIp.filter({ case(currentIp, _) => currentIp == ip })

      existingRequestDateTimes.isEmpty match {
        case true => requestDateTimesByIp :+ (ip, Set(currentRequestDateTime))
        case false => {
          requestDateTimesByIp.map({ case (currentIp, rDts) => {
            if (currentIp == ip) {
              (currentIp, (rDts + currentRequestDateTime))
            } else {
              (currentIp, rDts)
            }
          }})
        }
      }
    }

    val statefulData = StatefulData(updatedIpRequestCountByIp, updatedRequestDateTimesByIp)

    val StatefulData(updatedIpRequestCountByIp2, updatedRequestDateTimesByIp2) = closeInactiveSessions(Some(oldestPossibleEndOfSessionDateTime), statefulData)

    Tuple2(
      updatedIpRequestCountByIp2,
      updatedRequestDateTimesByIp2
    )
  }})

  //Close left-over sessions
  closeInactiveSessions(None, StatefulData(finalIpRequestCountByIp, finalRequestDateTimesByIp))
}
