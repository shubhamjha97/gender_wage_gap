import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


val path = "hw7/input/gender_wage_gap.csv"
val df = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(path)

val dfTOTAvg = df.where(col("SUBJECT")==="TOT").groupBy("LOCATION").avg("Value")
val dfSEAvg = df.where(col("SUBJECT")==="SELFEMPLOYED").groupBy("LOCATION").avg("Value")
val dfTOTMax = df.where(col("SUBJECT")==="TOT").groupBy("LOCATION").max("Value")
val dfSEMax = df.where(col("SUBJECT")==="SELFEMPLOYED").groupBy("LOCATION").max("Value")
val dfTOTMin = df.where(col("SUBJECT")==="TOT").groupBy("LOCATION").min("Value")
val dfSEMin = df.where(col("SUBJECT")==="SELFEMPLOYED").groupBy("LOCATION").min("Value")

dfTOTAvg.write.option("header", true).csv("BigDataProject/dfTOTAvg")
dfSEAvg.write.option("header", true).csv("BigDataProject/dfSeAvg")
dfTOTMax.write.option("header", true).csv("BigDataProject/dfTOTMax")
dfSEMax.write.option("header", true).csv("BigDataProject/dfSEMax")
dfTOTMin.write.option("header", true).csv("BigDataProject/dfTOTMin")
dfSEMin.write.option("header", true).csv("BigDataProject/dfSEMin")

