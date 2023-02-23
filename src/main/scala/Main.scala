package org.itc.sbe
import org.apache.spark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array_contains, col, lit, udf}
trait Variable{
  val path : String = "C:\\Users\\lexaw\\Downloads\\warehouse\\payments.csv"
  val orderPath : String = "C:\\Users\\lexaw\\Downloads\\warehouse\\orderdetails.csv"
  val sc = SparkSession.builder.master("local[*]").appName("SparkByExamples Tutorial").getOrCreate
  sc.sparkContext.setLogLevel("ERROR")
  val rdd = sc.read.option("header","true").option("inferSchema","true").csv(path)
  val rddForJoins = sc.read.option("header","true").option("inferSchema","true").csv(orderPath)
}

object Main extends App with Variable {
  // Where -> will be changed for multi column
  var rdd_where = rdd.where("order_number < 10200")
  rdd_where.show
  rdd_where = rdd.where(rdd("order_number") === 10103 && rdd("amount") === 6066.78)
  rdd_where.show
  //  rdd_where = rdd.where(array_contains(rdd("payment_id"),"HQ")) -> Works, but requires different dataset

  // With Columns
  var rdd_withCol = rdd.withColumn("disclosed_to_hmrc", col("amount") / 100)
    .withColumn("actual_amount", col("amount") * 100)
  rdd_withCol.withColumn("disclosed_to_hmrc",col("amount").cast("Integer"))
  rdd_withCol.show

  // Selecting multiple new columns via SQL
  rdd.createOrReplaceTempView("orders")
  val query_ = sc.sql("SELECT amount/100 as HMRC_DISCLOSED, amount*100 as ACTUAL_AMOUNT from orders")
  query_.show
  // Rename the column!
  rdd_withCol.withColumn(colName="new",col=col("amount"))
  rdd_withCol.show
  val tax_audit = rdd_withCol.drop(col("actual_amount")) // tax audit incoming
  tax_audit.show

  // With Columns Renamed

  val taxAudit1 = rdd.withColumnRenamed("amount","payment_amount")
    .withColumnRenamed("payment_id","pay_id")
  taxAudit1.show

  // Drop!
  val testDrop = rdd.drop("amount","payment_id")
  val seqDro = Seq("amount","payment_id")
  val seqDrop = rdd.drop(seqDro:_*)
  testDrop.show
  seqDrop.show
  println(testDrop == seqDrop)

  // Count Distinct
  val distinctDF = rdd.distinct
  println(distinctDF.count)

  // Drop all duplicate order ids
  val noDuplicateOrders = rdd.dropDuplicates("payment_id")
  println(distinctDF.count, noDuplicateOrders.count)
  noDuplicateOrders.show

  // Functions!
  val getInteger = (amount : String)z => {
    amount.split(".").mkString
  }
  val convertUDF = udf(getInteger)
  val fnTestRDD = rdd.select(col("order_number"),
    convertUDF(col("amount")).as("Amount_without_decimal"))
  fnTestRDD.show
}
