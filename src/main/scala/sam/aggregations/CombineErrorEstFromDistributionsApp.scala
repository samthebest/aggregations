package sam.aggregations

object CombineErrorEstFromDistributionsApp {
  def main(args: Array[String]): Unit = {

    for (limit <- List(50, 100, 500)) {
      println("\n\nLimit = " + limit)
      val errors = for {
        n <- List(50, 100, 200, 300, 1000, 2000, 3000, 4000, 10000, 20000) if limit < n
        max <- List(100, 200, 300, 1000, 10000, 100000)
      } yield {
          val median = new DynamicBucketingMedian(limit)
          val error = ErrorEstimator.normalDistribution(median, n, max)
          println(s"(n, max) = ( $n , $max ), error: " + error)
          error
        }

      println("\nError Average = " + (errors.sum / errors.size))
      println("Worst Error = " + errors.max)
      println("Best Error = " + errors.min)
    }

    println("\n\nUNIFORM")

    for (limit <- List(20, 50, 100, 500)) {
      println("\n\nLimit = " + limit)
      val errors = for {
        n <- List(50, 100, 200, 300, 1000, 2000, 3000, 4000, 10000, 20000) if limit < n
        max <- List(100, 200, 300, 1000, 10000, 100000)
      } yield {
          val median = new DynamicBucketingMedian(limit)
          val error = ErrorEstimator.uniformDistribution(median, n, max)
          println(s"(n, max) = ( $n , $max ), error: " + error)
          error
        }

      println("\nError Average = " + (errors.sum / errors.size))
      println("Worst Error = " + errors.max)
      println("Best Error = " + errors.min)
    }
  }
}
