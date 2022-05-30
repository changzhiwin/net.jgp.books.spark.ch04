package net.jgp.books.spark

import net.jgp.books.spark.ch04.lab200_transformation_and_action.TransformationAndAction
import net.jgp.books.spark.ch04.lab500_transformation_explain.TransformationExplain

object MainApp {
  def main(args: Array[String]) = {

    // Case: TransformationExplain
    // TransformationExplain.run()

    // Case: TransformationAndAction
    val mode = ( args.toSeq :+ "noop" )(0)
    TransformationAndAction.run(mode)
  }
}