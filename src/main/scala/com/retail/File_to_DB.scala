
package com.retail

import org.apache.spark.sql.DataFrame

final case class PostgresConfig(
    url: String,
    user: String,
    password: String,
    schema: String
)

object File_to_DB {
  private val IdentifierPattern = "[A-Za-z_][A-Za-z0-9_]*".r

  private val tableConstraints: Map[String, Seq[String]] = Map(
    "fact_sales" -> Seq(
      """ALTER COLUMN "customer_id" SET NOT NULL""",
      """ALTER COLUMN "product_id" SET NOT NULL""",
      """ALTER COLUMN "quantity" SET NOT NULL""",
      """ALTER COLUMN "sales" SET NOT NULL""",
      """ADD CONSTRAINT "pk_fact_sales" PRIMARY KEY ("order_id")""",
      """ADD CONSTRAINT "ck_fact_sales_quantity" CHECK ("quantity" > 0)""",
      """ADD CONSTRAINT "ck_fact_sales_sales" CHECK ("sales" >= 0)""",
      """ADD CONSTRAINT "ck_fact_sales_discount" CHECK ("discount" BETWEEN 0 AND 1)"""
    ),
    "monthly_sales" -> Seq(
      """ALTER COLUMN "order_month" SET NOT NULL""",
      """ALTER COLUMN "region" SET NOT NULL""",
      """ALTER COLUMN "category" SET NOT NULL""",
      """ADD CONSTRAINT "pk_monthly_sales" PRIMARY KEY ("order_month", "region", "category")""",
      """ADD CONSTRAINT "ck_monthly_sales_order_count" CHECK ("order_count" > 0)""",
      """ADD CONSTRAINT "ck_monthly_sales_total_sales" CHECK ("total_sales" >= 0)"""
    ),
    "product_performance" -> Seq(
      """ALTER COLUMN "category" SET NOT NULL""",
      """ALTER COLUMN "product_name" SET NOT NULL""",
      """ADD CONSTRAINT "pk_product_performance" PRIMARY KEY ("category", "product_name")""",
      """ADD CONSTRAINT "ck_product_performance_sales" CHECK ("total_sales" >= 0)""",
      """ADD CONSTRAINT "ck_product_performance_units" CHECK ("units_sold" > 0)""",
      """ADD CONSTRAINT "ck_product_performance_discount" CHECK ("avg_discount" BETWEEN 0 AND 1)"""
    ),
    "customer_analytics" -> Seq(
      """ALTER COLUMN "order_count" SET NOT NULL""",
      """ALTER COLUMN "total_sales" SET NOT NULL""",
      """ALTER COLUMN "units_purchased" SET NOT NULL""",
      """ADD CONSTRAINT "pk_customer_analytics" PRIMARY KEY ("customer_id")""",
      """ADD CONSTRAINT "ck_customer_analytics_orders" CHECK ("order_count" > 0)""",
      """ADD CONSTRAINT "ck_customer_analytics_sales" CHECK ("total_sales" >= 0)""",
      """ADD CONSTRAINT "ck_customer_analytics_units" CHECK ("units_purchased" > 0)""",
      """ADD CONSTRAINT "ck_customer_analytics_returns" CHECK ("returned_order_count" BETWEEN 0 AND "order_count")""",
      """ADD CONSTRAINT "ck_customer_analytics_return_rate" CHECK ("return_rate" BETWEEN 0 AND 1)"""
    ),
    "product_analytics" -> Seq(
      """ALTER COLUMN "order_count" SET NOT NULL""",
      """ALTER COLUMN "units_sold" SET NOT NULL""",
      """ALTER COLUMN "total_sales" SET NOT NULL""",
      """ADD CONSTRAINT "pk_product_analytics" PRIMARY KEY ("product_id")""",
      """ADD CONSTRAINT "ck_product_analytics_orders" CHECK ("order_count" > 0)""",
      """ADD CONSTRAINT "ck_product_analytics_units" CHECK ("units_sold" > 0)""",
      """ADD CONSTRAINT "ck_product_analytics_sales" CHECK ("total_sales" >= 0)""",
      """ADD CONSTRAINT "ck_product_analytics_returns" CHECK ("returned_order_count" BETWEEN 0 AND "order_count")"""
    )
  )

  def fromEnvironment(): Option[PostgresConfig] = {
    sys.env.get("PG_URL").map { _ =>
      val url = requiredEnvironmentVariable("PG_URL")
      val user = requiredEnvironmentVariable("PG_USER")
      val password = requiredEnvironmentVariable("PG_PASSWORD")
      val schema = sys.env.getOrElse("PG_SCHEMA", "data")
      validateIdentifier(schema, "PG_SCHEMA")
      PostgresConfig(url, user, password, schema)
    }
  }

  def ensureSchema(config: Option[PostgresConfig]): Unit = {
    config.foreach { postgres =>
      validateIdentifier(postgres.schema, "schema name")
      Class.forName("org.postgresql.Driver")
      val connection = java.sql.DriverManager.getConnection(
        postgres.url,
        postgres.user,
        postgres.password
      )
      try {
        val statement = connection.createStatement()
        try {
          statement.execute(s"""CREATE SCHEMA IF NOT EXISTS "${postgres.schema}"""")
        } finally {
          statement.close()
        }
      } finally {
        connection.close()
      }
    }
  }

  def write(dataFrame: DataFrame, tableName: String, config: Option[PostgresConfig]): Unit = {
    config.foreach { postgres =>
      validateIdentifier(tableName, "table name")

      dropTableIfExists(postgres, tableName)
      println(s"Writing PostgreSQL table ${postgres.schema}.$tableName (created if missing).")
      dataFrame.write
        .format("jdbc")
        .option("url", postgres.url)
        .option("dbtable", s""""${postgres.schema}"."$tableName"""")
        .option("user", postgres.user)
        .option("password", postgres.password)
        .option("driver", "org.postgresql.Driver")
        .mode("error")
        .save()
      applyConstraints(postgres, tableName)
      println(s"PostgreSQL table ${postgres.schema}.$tableName written successfully.")
    }
  }

  private def dropTableIfExists(postgres: PostgresConfig, tableName: String): Unit = {
    val connection = java.sql.DriverManager.getConnection(
      postgres.url,
      postgres.user,
      postgres.password
    )
    try {
      val statement = connection.createStatement()
      try {
        statement.execute(
          s"""DROP TABLE IF EXISTS "${postgres.schema}"."$tableName""""
        )
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def applyConstraints(postgres: PostgresConfig, tableName: String): Unit = {
    val statements = tableConstraints.getOrElse(
      tableName,
      throw new IllegalArgumentException(s"No PostgreSQL constraints are defined for table '$tableName'.")
    )
    val connection = java.sql.DriverManager.getConnection(
      postgres.url,
      postgres.user,
      postgres.password
    )
    try {
      val statement = connection.createStatement()
      try {
        statements.foreach { definition =>
          statement.execute(
            s"""ALTER TABLE "${postgres.schema}"."$tableName" $definition"""
          )
        }
      } finally {
        statement.close()
      }
    } finally {
      connection.close()
    }
  }

  private def requiredEnvironmentVariable(name: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(s"Environment variable $name must be set when PG_URL is configured.")
    }

  private def validateIdentifier(value: String, name: String): Unit =
    if (!IdentifierPattern.pattern.matcher(value).matches()) {
      throw new IllegalArgumentException(s"$name must be a simple PostgreSQL identifier; received '$value'.")
    }
}