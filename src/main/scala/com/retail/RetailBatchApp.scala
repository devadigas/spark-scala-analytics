package com.retail

import org.apache.spark.sql.SparkSession

object RetailBatchApp {
  def main(args: Array[String]): Unit = {
    val root = if (args.nonEmpty) args(0) else "."

    val spark = SparkSession.builder()
      .appName("RetailSalesBatchETL")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val postgresConfig = File_to_DB.fromEnvironment()
      if (postgresConfig.isEmpty) {
        println("PG_URL is not set; skipping PostgreSQL writes.")
      }
      run(spark, root, postgresConfig)
    } finally {
      spark.stop()
    }
  }

  def run(
      spark: SparkSession,
      root: String,
      postgresConfig: Option[PostgresConfig] = None
  ): Unit = {
    File_to_DB.ensureSchema(postgresConfig)

    val out = s"$root/data/processed/"
    val factSales = OrderAnalysis.buildFactSales(spark, root)
    val monthlySales = OrderAnalysis.monthlySales(factSales)
    val categoryPerformance = OrderAnalysis.categoryPerformance(factSales)

    // Replace output on each full batch run for repeatability.
    factSales.write.mode("overwrite").partitionBy("order_year").parquet(out + "fact_sales")
    monthlySales.write.mode("overwrite").parquet(out + "monthly_sales")
    categoryPerformance.write.mode("overwrite").parquet(out + "product_performance")

    File_to_DB.write(factSales, "fact_sales", postgresConfig)
    File_to_DB.write(monthlySales, "monthly_sales", postgresConfig)
    File_to_DB.write(categoryPerformance, "product_performance", postgresConfig)

    CustomerApp.run(factSales, root, postgresConfig)
    ProductApp.run(factSales, root, postgresConfig)

    println("ETL completed.")
    println(s"Fact rows: ${factSales.count()}")
    println(s"Monthly aggregate rows: ${monthlySales.count()}")
    println(s"Product performance rows: ${categoryPerformance.count()}")
  }
}
