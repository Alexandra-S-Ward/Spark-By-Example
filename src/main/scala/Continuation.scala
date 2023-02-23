package org.itc.sbe
import Main._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.storage.StorageLevel

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

  // Joins

  rdd.join(rddForJoins, rdd("order_number") === rddForJoins("order_number"),"outer").show
  // Left semi -> similar to inner join, but more useful.
  rdd.join(rddForJoins, rdd("order_number") === rddForJoins("order_number"), "leftanti").show

  // SQL Join
  rdd.createOrReplaceTempView("RDD")
  rddForJoins.createOrReplaceTempView("secondRDD")
  sc.sql("SELECT r.*, d.* FROM RDD r, secondRDD d where r.order_number == d.order_number").show

  val structData = Seq(
    Row("Alexandra","Stephanie","W","ABC123", "Bradford",1229),
    Row("Rupali", "", "W", "IABC123", "India", 12332),
    Row("Prosper", "", "", "PABC32", "Not London", 1331),
    Row("Eworitse", "", "T", "EBD321", "London", 313)
  )
  val structSchema = new StructType()
    .add("fname", StringType)
  .add("mname", StringType)
  .add("lname", StringType)
  .add("id", StringType)
    .add("location",StringType)
    .add("a_number", IntegerType)

  var newStructData = Seq(
    Row("Another","Person","Here","AAADDD", "Malysia",3333),
    Row("Second","Dataframe","Entry","OOODD", "Bangalore", 313151),
    Row("Erowitse","","T","EBD321","London",313) // Sneaky Ewo, trying to sneak in twice!
  )

  val dfFromStruct = sc.createDataFrame(sc.sparkContext.parallelize(structData), structSchema)
  val dfFromStructTwo = sc.createDataFrame(sc.sparkContext.parallelize(newStructData), structSchema)

  // Union.

  val unionedDF = dfFromStruct.union(dfFromStructTwo)
  unionedDF.show // Ewo succeeds in his sneaking. Tut, tut.
  val noDupes = dfFromStruct.union(dfFromStructTwo).distinct()
  noDupes.show

  // COLLECT -> ACTION.
  // Small datasets or you're out of memory!
  val collectedNoDuples = noDupes.collect
  val listNoDuples = noDupes.collectAsList
  collectedNoDuples.foreach(row=> {
    val name = row.getString(0)
    println(name)
  }
  ) // Collect is bad on big datasets. select > collect maybe

  // for each practice with some fraudulent behaviour!
  // Theoretically everybody is lying to HMRC for this!
  collectedNoDuples.foreach(row=>{
    val salary = row.getInt(5) // Get the number.
    val name = row.getString(0)
    val actual_salary = salary * 20
    println(f"$name%s said they earned $salary this year but they actually earned $actual_salary. Scandalous!")
  })

  val persistingRDD = rdd.persist(StorageLevel.DISK_ONLY) // I like RDD. RDD can stay on my hard disk
  val cachedRDD = rdd.cache // Cache => Memory only. Persist => Wherever you want it.
  // . . . a lot of persisting things later, I have decided I dont like persisting RDD.
  persistingRDD.unpersist // ta-ta', old friend!

  /* Time for some Persistent types, mm?

      Aight. SO! Memory related stuff (StorageLevel._) is implied:
          MEMORY_ONLY = Cache's default. Cache this in memory. Slower than MEMORY_AND_DISK if not enough memory due to recalculations.
          MEMORY_ONLY_SER = RDDs are serialised and stored in JVM Mem. More space efficient, but a little more processing.
          MEMORY_ONLY_2 = MEMORY_ONLY but replicates partitions to 2 cluster nodes.
          MEMORY_ONLY_SER_2 = MEMORY_ONLY_SER but replicates to 2 cluster nodes.

      How about some HDD storage too!
          MEMORY_AND_DISK = Store in JVM as deserialised object. Stores excess onto disc
          MEMORY_AND_DISK_SER = Same as above but serialised.
          DISK_ONLY = Store only on disk. Expensive due to I/O
          (_2 for the three above)
   */
  // Pivot -> Transpose Column to Row.

  val pivotedRDD = rdd.groupBy("payment_id").pivot("order_number").sum("amount")
  pivotedRDD.show // Pivot is best used for "categorical" data -> Data without a large amount of values. ID is bad, product {} is good.

  // pivoted RDD is unusable, but...
  val usefulPivot = rddForJoins.groupBy("quantity").pivot("product_category").sum("price").where("quantity is not null")
  usefulPivot.show
}
