import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


val path = "bdad_project_data/gender_wage_gap.csv"
val df = spark.read.format("csv")
  .option("header","true")
  .option("inferSchema", "true")
  .load(path)

val dfTotalEmploymentAvg = df.where(col("SUBJECT")==="TOT")
  .groupBy("LOCATION")
  .avg("Value")
dfTotalEmploymentAvg.write
  .option("header", true)
  .csv("bdad_project_data/output/dfTotalEmploymentAvg")

val dfSelfEmploymentAvg = df.where(col("SUBJECT")==="SELFEMPLOYED")
  .groupBy("LOCATION")
  .avg("Value")
dfSelfEmploymentAvg.write
  .option("header", true)
  .csv("bdad_project_data/output/dfSelfEmploymentAvg")

val dfTotalEmploymentMax = df.where(col("SUBJECT")==="TOT")
  .groupBy("LOCATION")
  .max("Value")
dfTotalEmploymentMax.write
  .option("header", true)
  .csv("bdad_project_data/output/dfTotalEmploymentMax")


val dfSelfEmploymentMax = df.where(col("SUBJECT")==="SELFEMPLOYED")
  .groupBy("LOCATION")
  .max("Value")
dfSelfEmploymentMax.write
  .option("header", true)
  .csv("bdad_project_data/output/dfSelfEmploymentMax")

val dfTotalEmploymentMin = df.where(col("SUBJECT")==="TOT")
  .groupBy("LOCATION")
  .min("Value")
dfTotalEmploymentMin.write
  .option("header", true)
  .csv("bdad_project_data/output/dfTotalEmploymentMin")

val dfSelfEmploymentMin = df.where(col("SUBJECT")==="SELFEMPLOYED")
  .groupBy("LOCATION")
  .min("Value")
dfSelfEmploymentMin.write
  .option("header", true)
  .csv("bdad_project_data/output/dfSelfEmploymentMin")
