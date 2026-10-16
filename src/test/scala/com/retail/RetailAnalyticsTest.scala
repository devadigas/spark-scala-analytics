package com.retail

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Rule, Test}
import org.junit.rules.TemporaryFolder

class RetailAnalyticsTest {
  private val folderRule = new TemporaryFolder()

  @Rule
  def temporaryFolder: TemporaryFolder = folderRule

  private def createInputFiles(): String = {
    val root = temporaryFolder.newFolder("project")
    val raw = root.toPath.resolve("data").resolve("raw")
    Files.createDirectories(raw)

    write(raw.resolve("orders.csv"),
      "order_id,order_date,ship_date,customer_id,product_id,quantity,discount,sales,profit,ship_mode\n" +
      "O001,2025-01-10,2025-01-12,C001,P001,2,0.1,100.0,20.0,Standard\n" +
      "O002,2025-01-11,2025-01-14,C001,P002,1,0.2,50.0,-5.0,Express\n" +
      "O002,2025-01-11,2025-01-14,C001,P002,1,0.2,50.0,-5.0,Express\n")
    write(raw.resolve("customers.csv"),
      "customer_id,segment,region,city\nC001,Consumer,North,Berlin\n")
    write(raw.resolve("products.csv"),
      "product_id,product_name,category,list_price,unit_cost\n" +
      "P001,Desk,Furniture,60.0,40.0\n" +
      "P002,Chair,Furniture,55.0,35.0\n")
    write(raw.resolve("returns.csv"), "order_id,return_reason\nO002,Damaged\n")

    root.getAbsolutePath
  }

  private def write(path: java.nio.file.Path, content: String): Unit =
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))

  @Test
  def retailPipelineWritesFactCustomerAndProductOutputs(): Unit = {
    val root = createInputFiles()

    RetailBatchApp.run(RetailAnalyticsTest.spark, root)

    val base = s"$root/data/processed/"
    val fact = RetailAnalyticsTest.spark.read.parquet(base + "fact_sales")
    val customer = RetailAnalyticsTest.spark.read.parquet(base + "customer_analytics")
    val product = RetailAnalyticsTest.spark.read.parquet(base + "product_analytics")

    assertEquals(2L, fact.count())
    assertEquals(1L, customer.count())
    assertEquals(2L, product.count())

    val customerRow = customer.first()
    assertEquals(150.0, customerRow.getAs[Double]("total_sales"), 0.001)
    assertEquals(1L, customerRow.getAs[Long]("returned_order_count"))
  }

  @Test
  def monthlySalesAggregatesOrderLinesByMonthRegionAndCategory(): Unit = {
    val root = createInputFiles()
    val fact = OrderAnalysis.buildFactSales(RetailAnalyticsTest.spark, root)
    val monthly = OrderAnalysis.monthlySales(fact)

    assertEquals(1L, monthly.count())
    val row = monthly.first()
    assertEquals("2025-01", row.getAs[String]("order_month"))
    assertEquals(150.0, row.getAs[Double]("total_sales"), 0.001)
    assertEquals(2L, row.getAs[Long]("order_count"))
  }

  @Test
  def categoryPerformanceAggregatesSalesProfitAndUnits(): Unit = {
    val root = createInputFiles()
    val fact = OrderAnalysis.buildFactSales(RetailAnalyticsTest.spark, root)
    val performance = OrderAnalysis.categoryPerformance(fact)

    assertEquals(2L, performance.count())
    val desk = performance.filter("product_name = 'Desk'").first()
    assertEquals(100.0, desk.getAs[Double]("total_sales"), 0.001)
    assertEquals(20.0, desk.getAs[Double]("total_profit"), 0.001)
    assertEquals(2L, desk.getAs[Long]("units_sold"))
  }
}

object RetailAnalyticsTest {
  private var sparkSession: SparkSession = _

  @BeforeClass
  def startSpark(): Unit = {
    sparkSession = SparkSession.builder()
      .appName("RetailAnalyticsTest")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
  }

  @AfterClass
  def stopSpark(): Unit = {
    if (sparkSession != null) {
      sparkSession.stop()
      sparkSession = null
    }
  }

  def spark: SparkSession = sparkSession
}
