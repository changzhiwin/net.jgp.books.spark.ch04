package net.jgp.books.spark.ch04.lab500_transformation_explain;

import org.apache.spark.sql.functions.expr
import net.jgp.books.spark.basic.Basic

object TransformationExplain extends Basic{
  def run(): Unit = {
    val spark = getSession("Showing execution plan")

    val df = spark.read.format("csv").
      option("header", "true").
      load("data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv")

    val uDF = df.union(df)

    import spark.implicits._
    val tDF = uDF.withColumnRenamed("Lower Confidence Limit", "lcl").
      withColumnRenamed("Upper Confidence Limit", "ucl").
      //withColumn("avg", expr("(lcl+ucl)/2")).
      withColumn("avg",  ($"lcl" + $"ucl") / 2).
      withColumn("lcl2", $"lcl").
      withColumn("ucl2", $"ucl")

    tDF.explain()
  }
}