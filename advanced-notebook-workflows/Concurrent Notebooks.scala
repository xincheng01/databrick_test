// Databricks notebook source
// MAGIC %md This notebook illustrates various ways of running notebooks concurrently. To see print outs from the child notebooks, click the ``Notebook job #nnn`` links.

// COMMAND ----------

// MAGIC %md  
// MAGIC A caveat: Outputs from notebooks cannot interact with each other. If you're writing to files/tables, output paths should be parameterized via widgets. This is similar to two users running same notebook on the same cluster at the same time; they should share no state.

// COMMAND ----------

// MAGIC %md The ``parallel-notebooks`` notebook defines functions as if they were in a library.

// COMMAND ----------

// MAGIC %run "./parallel-notebooks"

// COMMAND ----------

// MAGIC %md 
// MAGIC The remaining commands invoke ``NotebookData`` functions in various ways.

// COMMAND ----------

// MAGIC %md The first version creates a sequence of notebook, timeout, parameter combinations and passes them into the ``parallelNotebooks`` function defined in the ``parallel notebooks`` notebook. The ``parallelNotebooks`` function prevents overwhelming the driver by limiting the the number of threads that are running.

// COMMAND ----------

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps

val notebooks = Seq(
  NotebookData("testing", 15),
  NotebookData("testing-2", 15, Map("Hello" -> "yes")),
  NotebookData("testing-2", 15, Map("Hello" -> "else")),
  NotebookData("testing-2", 15, Map("Hello" -> "lots of notebooks")),
  NotebookData("testing-2", 1, Map("Hello" -> "parallel")) // fails due to timeout, intentionally
)

val res = parallelNotebooks(notebooks)

Await.result(res, 30 seconds) // this is a blocking call.
res.value

// COMMAND ----------

// MAGIC %md This next version defines individual parallel notebooks. It takes the same parameters as above except does it on an individual basis and you have to specify the result sequence that has to run. That has to be a Future for you to receive.

// COMMAND ----------

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
/*
Note: this is a bit more manual but achieves the same result as the first version.
*/
val n1 = parallelNotebook(NotebookData("testing", 15))
val n2 = parallelNotebook(NotebookData("testing-2", 15, Map("Hello" -> "yes")))
val res = Future.sequence(List(n1, n2)) 

Await.result(res, 30 minutes) // this blocks
res.value

// COMMAND ----------

// MAGIC %md This last version is the most manual way of doing it. You individually define the Futures that need to be returned.

// COMMAND ----------

val ctx = dbutils.notebook.getContext()
/*
Note: This version illustrates the structure built into the preceding versions.
*/

val myHappyNotebookTown1 = Future {
  dbutils.notebook.setContext(ctx)
  println("Starting First --")
  val x = dbutils.notebook.run("testing", 15)
  println("Finished First -- ")
  x
}

val myHappyNotebookTown2 = Future {
  dbutils.notebook.setContext(ctx)
  println("Starting Second --")
  val x = dbutils.notebook.run("testing-2", 15, Map("Hello" -> "yes"))
  println("Finished Second -- ")
  x
}

val res = Future.sequence(List(myHappyNotebookTown1, myHappyNotebookTown2))

Await.result(res, 30 minutes) // this blocks
res.value

// COMMAND ----------

// MAGIC %md Each example is the exact same as the others except in terms of what it does automatically for you. They can all be used to achieve the same end result.
