import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.File



object Main extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("finalproject")
    .getOrCreate();

  // Read data from CSV file
  val data = spark.read.option("header", true).option("inferSchema", true).csv("C:\\Users\\shabn\\OneDrive\\Desktop\\2dec\\input")

  // Data Processing
  val processed_data = data.where("link is not null")
  processed_data.show(false)

  // Save the data as JSON file
  processed_data.coalesce(1).write.mode("overwrite")
    .json("C:\\\\Users\\\\shabn\\\\OneDrive\\\\Desktop\\\\2dec\\\\output")

  // Save the data using GZIP compression
  processed_data.coalesce(1).write.mode("overwrite")
    .option("compression", "GZIP").json("C:\\\\Users\\\\shabn\\\\OneDrive\\\\Desktop\\\\2dec\\compressed\\gz")

// Save the data using SNAPPY compression
 processed_data.coalesce(1).write.mode("overwrite").option("compression","snappy").parquet("C:\\Users\\shabn\\OneDrive\\Desktop\\2dec\\compressed\\snappy")



  // Uncompress the compressed file
  val df = spark.read.option("header", true).option("inferSchema", true)
    .json("C:\\Users\\shabn\\OneDrive\\Desktop\\2dec\\compressed")
  df.coalesce(1).write.mode("overwrite").json("C:\\Users\\shabn\\OneDrive\\Desktop\\2dec\\decompressed")


  //ou
  var someFile = new File("C:\\Users\\shabn\\OneDrive\\Desktop\\2dec\\output\\part-00000-1dbe42a6-a8de-4572-9758-4bc4413ad197-c000.json")
  var fileSize = someFile.length
  println("SIZE of JSON FILE : " + fileSize)

//comgz
  someFile = new File("C:\\Users\\shabn\\OneDrive\\Desktop\\2dec\\compressed\\gz\\part-00000-8994c6df-2c61-4c1c-b61d-67f7c6658449-c000.json.gz")
  fileSize = someFile.length
  println("SIZE of gz Compressed FILE : " + fileSize)


  someFile = new File("C:\\Users\\shabn\\OneDrive\\Desktop\\2dec\\compressed\\snappy\\part-00000-5d16b0ce-03c8-4c8e-9a86-67b2d931a316-c000.snappy.parquet")
   fileSize = someFile.length
   println("SIZE of snappy Compressed  FILE : " + fileSize)


  someFile = new File("C:\\Users\\shabn\\OneDrive\\Desktop\\2dec\\decompressed\\part-00000-7aa9d08f-0b0f-40e5-a7fa-e5e976fd1862-c000.json")
  fileSize = someFile.length
  println("SIZE of de-Compressed FILE : " + fileSize)*/



}