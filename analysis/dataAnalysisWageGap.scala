val df=spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("bdad_project_data/gender_wage_gap.csv")
df.printSchema()

df.groupBy("LOCATION")
  .avg("Value")
  .show(false)
df.show()
df.write
  .option("header", true)
  .csv("bdad_project_data/output/dfOverallEmploymentAvgByLocation")


df.groupBy("LOCATION")
  .max("Value")
  .show(false)
df.write
  .option("header", true)
  .csv("bdad_project_data/output/dfOverallEmploymentMaxByLocation")


df.groupBy("TIME")
  .max("Value")
  .show(false)
df.write
  .option("header", true)
  .csv("bdad_project_data/output/dfOverallEmploymentMaxByTime")


df.select("SUBJECT").distinct.show(false);
df.select("LOCATION").distinct.show(false);

df.createOrReplaceTempView("wage_gap_analysis_tbl");
spark.sql("""select * from wage_gap_analysis_tbl""");