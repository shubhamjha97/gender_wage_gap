schema = StructType([ StructField("gender", StringType(), True),
  StructField("salary", StringType(), True) ])

val df=spark.read.csv("bdad_project_data/adult.data", header=False, schema=schema)

// Number of males
df.where("LOCATION")
  .count()
  .show(false)
df.show()
df.write
  .option("header", true)
  .csv("bdad_project_data/output/dfOverallEmploymentAvgByLocation")



// Number of females
df.groupBy("LOCATION")
  .avg("Value")
  .show(false)
df.show()
df.write
  .option("header", true)
  .csv("bdad_project_data/output/dfOverallEmploymentAvgByLocation")

// Number, % of poor, rich males
df.groupBy("LOCATION")
  .avg("Value")
  .show(false)
df.show()
df.write
  .option("header", true)
  .csv("bdad_project_data/output/dfOverallEmploymentAvgByLocation")


// Number, % of poor, rich females
df.groupBy("LOCATION")
  .avg("Value")
  .show(false)
df.show()
df.write
  .option("header", true)
  .csv("bdad_project_data/output/dfOverallEmploymentAvgByLocation")

