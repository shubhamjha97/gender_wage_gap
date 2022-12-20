package IncomeDataReader

//read input packages
import scala.io.StdIn.readInt
import scala.io.StdIn.readLine

//scala utils
import scala.util.{Try, Success, Failure}
import scala.math.BigDecimal
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

//mongodb packages and helper
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import scala.collection.JavaConverters._
import tour.Helpers._

//logging packages
// import com.typesafe.scalalogging.LoggerMacro
// import com.typesafe.scalalogging.Logger
import java.io._
import java.util.Calendar

object IncomeDataReader extends App {
  if (args.length == 0) {
    val bufferedSource = io.Source.fromFile("adult.data")
    var rich = 0
    var poor = 0
    var count = 0.0
    var richMale = 0
    var richFemale = 0
    var poorMale = 0
    var poorFemale = 0
    var numMales = 0.0
    var numFemales = 0.0
    var percentageOfPoorFemales = 0.0
    var percentageOfPoorMales = 0.0
    var percentageOfRichFemales = 0.0
    var percentageOfRichMales = 0.0

    //val logger = Logger("DataLog")

    def getData = {
      for (line <- bufferedSource.getLines) {
        val cols = line.split(",").map(_.trim)

        //counting gender and income
        if (cols(14) == ">50K") {
          rich += 1
          if (cols(9) == "Female") {
            richFemale += 1
            numFemales += 1
          } else if (cols(9) == "Male") {
            richMale += 1
            numMales += 1
          } else {
            println("Error")
          }
        } else {
          poor += 1
          if (cols(9) == "Female") {
            poorFemale += 1
            numFemales += 1
          } else if (cols(9) == "Male") {
            poorMale += 1
            numMales += 1
          } else {
            println("Error")
          }
        }
        //counting
        count += 1
      }
      //https://stackoverflow.com/questions/11106886/scala-doubles-and-precision
      //calculate average per category and compare
      percentageOfPoorFemales = BigDecimal((poorFemale / numFemales) * 100)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
      percentageOfPoorMales = BigDecimal((poorMale / numMales) * 100)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
      percentageOfRichFemales = BigDecimal((richFemale / numFemales) * 100)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
      percentageOfRichMales = BigDecimal((richMale / numMales) * 100)
        .setScale(2, BigDecimal.RoundingMode.HALF_UP)
        .toDouble
    }

    //https://alvinalexander.com/scala/how-to-parse-number-int-long-float-bigint-from-string-in-scala/
    //making sure user types valid inputs only
    def toInt(s: String): Option[Int] = {
      try {
        Some(s.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }

    //intro statements for user to read during load
    println("")
    println("Welcome to Wage Gap Analysis")

    //loading data first before analysis
    //works cited - rastal
    val dataFuture = Future { getData }
    dataFuture.onComplete {
      case Success(value) =>
        println("")
        println("Loading..")
      case Failure(exception) => exception.printStackTrace
    }
    Thread.sleep(5000)

    var contProg = true
    while (contProg) {
      println("")
      println("Please select a option for analysis..")
      printf("1.Race\n2.Gender\n3.Add Data\n4.Exit\n\n")

      var userOption = readLine()

      while (
        (toInt(userOption)
          .getOrElse(0) >= 5) || (toInt(userOption).getOrElse(0) <= 0)
      ) {
        println("Please enter a valid number option")
        userOption = readLine()
      }

      userOption.toInt match {
        case 1 => println("printing race analysis...")
        case 2 =>
          println("printing gender analysis..")
          println("Printing raw stats..")
          println(s"Number of rich males: $richMale")
          println(s"Number of rich females: $richFemale")
          println(s"Number of poor males: $poorMale")
          println(s"Number of poor females: $poorFemale")

          println(s"Total Number of Females: $numFemales")
          println(s"Total Number of Males: $numMales")
          println(s"Total dataset count: $count")
          println("")

          println("Printing analysis..")
          println(
            s"Percentage of females making under or equal to 50k: $percentageOfPoorFemales"
          )
          println(
            s"Percentage of females making over 50k: $percentageOfRichFemales"
          )
          println("")
          println(
            s"Percentage of males under or equal to 50k: $percentageOfPoorMales"
          )
          println(
            s"Percentage of males making over 50k: $percentageOfRichMales"
          )
        case 3 =>
          var contLoop = true
          var newPerson = "PlaceholderText"
          var newWage = "PlaceHolderText"
          var logTime = Calendar.getInstance().getTime()

          println("adding data...")

          println("Please enter Male or Female")
          while (contLoop) {
            newPerson = readLine()
            if ((newPerson == "Male") || (newPerson == "Female")) {
              contLoop = false
            } else {
              println("Please only choose from Male or Female")
            }
          }

          println("Please enter <=50k or >50k")
          //reset loop
          contLoop = true
          while (contLoop) {
            newWage = readLine()
            if ((newWage == "<=50k") || (newWage == ">50k")) {
              contLoop = false
            } else {
              println("Please only choose from <=50k or >50k")
            }
          }

          //https://alvinalexander.com/scala/how-to-write-text-files-in-scala-printwriter-filewriter/
          //each time i add something, write to log file with timestamp
          val bw = new BufferedWriter(
            new FileWriter(new File("AddDataLog.txt"), true)
          )
          bw.write(s"$logTime\n")
          bw.write(s"Added:: Gender:$newPerson and Wage:$newWage\n")
          bw.close

          val mongoClient: MongoClient = MongoClient(
            "mongodb://localhost:27017/"
          )
          val database: MongoDatabase = mongoClient.getDatabase("proj0db")
          val collection: MongoCollection[Document] =
            database.getCollection("test2")

          val dbName = collection.namespace
          val doc: Document = Document(
            // "_id" -> 1,
            "age" -> "N/A",
            "fnlwgt " -> "N/A",
            "education" -> "N/A",
            "education-num" -> "N/A",
            "marital-status" -> "N/A",
            "occupation" -> "N/A",
            "relationship" -> "N/A",
            "race" -> "N/A",
            "sex" -> newPerson,
            "capital-gain" -> "N/A",
            "capital-loss" -> "N/A",
            "hours-per-week" -> "N/A",
            "native country" -> "N/A",
            "wage" -> newWage
          )

          // val doc1: Document = Document(
          //   // "_id" -> 1,
          //   "age" -> "N/A",
          //   "fnlwgt " -> "N/A",
          //   "education" -> "N/A",
          //   "education-num" -> "N/A",
          //   "marital-status" -> "N/A",
          //   "occupation" -> "N/A",
          //   "relationship" -> "N/A",
          //   "race" -> "N/A",
          //   "sex" -> newPerson,
          //   "capital-gain" -> "N/A",
          //   "capital-loss" -> "N/A",
          //   "hours-per-week" -> "N/A",
          //   "native country" -> "N/A",
          //   "wage" -> newWage
          // )
          collection.insertOne(doc).results()

          //collection.find().first().printHeadResult()
          println("")
          println(s"Completed writing to MongoDB: $dbName")
        case 4 =>
          println("Exiting..Goodbye!")
          contProg = false
        case _ => println("you should not be here..")
      }

      bufferedSource.close
    }
  } else {
    if (args(0) == "log") {
      println("reading log file..")
      val bufferedSource = io.Source.fromFile("AddDataLog.txt")
      for (line <- bufferedSource.getLines) {
        println(line)
      }
    } else {
      println(
        "Rerun using \" sbt --error \"run log\" \" or \" sbt --error run \""
      )
    }
  }

  //added for testing purposes
  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }
}

