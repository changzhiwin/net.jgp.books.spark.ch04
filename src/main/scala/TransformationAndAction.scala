package net.jgp.books.spark.ch04.lab200_transformation_and_action

import java.lang.System.currentTimeMillis
import org.apache.spark.sql.DataFrame
import net.jgp.books.spark.basic.Basic

object TransformationAndAction extends Basic{
  def run(mode: String): Unit = {
    val step1 = trace(Seq.empty)

    val spark = getSession("Analysing Catalyst's behavior")
    val step2 = trace(step1)

    val df =spark.read.format("csv").
      option("header", "true").
      load("data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv")
    val step3 = trace(step2)

    println("# init records .................. " + df.count())

    val bigDF = (1 to 3).toSeq.foldLeft(df)((uDF, _) => uDF.union(df))
    val step4 = trace(step3)

    val cleanDF = bigDF.withColumnRenamed("Lower Confidence Limit", "lcl").
      withColumnRenamed("Upper Confidence Limit", "ucl")
    val step5 = trace(step4)
    
    import spark.implicits._
    var doneDF: DataFrame = cleanDF
    if (!mode.contains("noop")) {
      doneDF = doneDF.
        withColumn("avg", ($"lcl" + $"ucl") / 2).
        withColumn("lcl2", $"lcl").
        withColumn("ucl2", $"ucl")

      if (mode.contains("full")) {
        doneDF = doneDF.
          drop($"avg").
          drop($"lcl2").
          drop($"ucl2")
      }
    }

    val step6 = trace(step5)

    doneDF.collect()
    val step7 = trace(step6)

    // see report table
    printCost(step7)
    println("# of records .................... " + doneDF.count())
  }

  def trace(steps: Seq[Long]): Seq[Long] = {
    currentTimeMillis() +: steps
  }

  def printCost(t: Seq[Long]): Unit = {
    println("1. Creating a session ........... " + (t(5) - t(6)))
    println("2. Loading initial dataset ...... " + (t(4) - t(5)))
    println("3. Building full dataset ........ " + (t(3) - t(4)))
    println("4. Clean-up ..................... " + (t(2) - t(3)))
    println("5. Transformations .............. " + (t(1) - t(2)))
    println("6. Final action ................. " + (t(0) - t(1)))
  }
}