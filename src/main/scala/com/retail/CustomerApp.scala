package com.retail

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CustomerApp {
  def run(
      factSales: DataFrame,
      root: String,
      postgresConfig: Option[PostgresConfig] = None
  ): Unit = {
    val customerAnalytics = factSales
      .groupBy("customer_id", "segment", "region", "city")
      .agg(
        countDistinct("order_id").as("order_count"),
        sum("sales").as("total_sales"),
        sum("profit").as("total_profit"),
        sum("quantity").as("units_purchased"),
        countDistinct(when(col("is_returned"), col("order_id"))).as("returned_order_count"),
        avg("discount").as("avg_discount")
      )
      .withColumn(
        "return_rate",
        when(col("order_count") > 0, col("returned_order_count") / col("order_count"))
          .otherwise(lit(0.0))
      )

    customerAnalytics.write.mode("overwrite")
      .parquet(s"$root/data/processed/customer_analytics")

    File_to_DB.write(customerAnalytics, "customer_analytics", postgresConfig)

    println(s"Customer analytics rows: ${customerAnalytics.count()}")
  }
}