import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

schema = StructType([ StructField("gender", StringType(), True),
  StructField("salary", StringType(), True) ])

val df=spark.read.csv("bdad_project_data/adult.data", header=False, schema=schema)

// Number of males
df.groupBy("gender")
  .count()
  .show(false)
df.show()

df.where(col("salary")==="<=50K")
  .groupBy("gender")
  .count()
  .show(false)
df.show()

df.where(col("salary")===">50K")
  .groupBy("gender")
  .count()
  .show(false)
df.show()
