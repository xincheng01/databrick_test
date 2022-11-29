// Databricks notebook source
dbutils.widgets.text("Hello", "hello")

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val x = dbutils.widgets.get("Hello")
println(s"Doing second work: $x")

// COMMAND ----------

dbutils.notebook.exit("SUCCESS")
