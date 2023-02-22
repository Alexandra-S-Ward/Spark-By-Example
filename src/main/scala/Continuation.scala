package org.itc.sbe
import Main._
import org.apache.spark.sql.functions._

object Continuation extends Variable with App{
  // GroupBy
  val rddGroupBy = rdd.groupBy("order_number").sum("amount").orderBy("order_number")
  rddGroupBy.show
  // Group By # of items bought per order number
  rdd.groupBy("order_number").count.orderBy("order_number").show

  // Allll the aggregation!

  rdd.groupBy("order_number").agg(
    sum("amount").as("sum_amount"),
    avg("amount").as("avg_amount"),
    max("amount").as("Biggest_spender")
  ).show

  // Only big spenders get to be in my table
  rdd.groupBy("order_number").agg(
    sum("amount").as("sum_amount"),
    avg("amount").as("avg_amount"),
    max("amount").as("Biggest_spender")
  ).where(col("sum_amount") > 100000).show

}
