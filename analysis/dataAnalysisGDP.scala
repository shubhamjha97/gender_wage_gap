val df=spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("bdad_project_data/gdp.csv")


df.select("TIME").distinct.show(false)

df.createOrReplaceTempView("gdpTbl");
spark.sql("""select MAX(Value) FROM gdpTbl""").show(false)
spark.sql("""select LOCATION, MAX(Value) as max FROM gdpTbl GROUP BY LOCATION ORDER BY max asc """).show(false)
spark.sql("""select LOCATION, MAX(Value) as max FROM gdpTbl GROUP BY LOCATION ORDER BY max desc """).show(false)
spark.sql("""select min(Value) as min FROM gdpTbl""").show(false)
spark.sql("""select AVG(Value) as max FROM gdpTbl GROUP BY LOCATION ORDER BY max desc """).show(false)

val avg=df.select(mean(df("Value"))).show()

val higher_than_avg=df.filter(df("Value") >= 36596.98).show(false)
val lower_than_avg=df.filter(df("Value") <= 36596.98).show(false)
