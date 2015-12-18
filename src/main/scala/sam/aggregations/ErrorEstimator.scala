package sam.aggregations

import breeze.stats.distributions.{Rand, Gaussian}
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import sam.aggregations.aggregators.MedianEstimator
import shapeless._
import HList._

import scala.collection.immutable.IndexedSeq
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Random

case class FromTestDataReport(errorsAndNumExamples: List[(Double, Int)] = Nil,
                              averageError: Double = 1.0,
                              bestError: Double = 1.0,
                              worstError: Double = 1.0,
                              averageNumExamples: Int = 0,
                              mostNumExamples: Int = 0,
                              leastNumExamples: Int = 0) {
  def pretty: String =
    "Average Error: " + averageError + "\n" +
      "Best Error: " + bestError + "\n" +
      "Worst Error: " + worstError + "\n" +
      "Average num examples: " + averageNumExamples + "\n" +
      "Most num examples: " + mostNumExamples + "\n" +
      "Least num examples: " + leastNumExamples + "\n" +
      "errorsAndNumExamples:\n" + errorsAndNumExamples.map(p => p._1.toString + "\t" + p._2).mkString("\n")
}

case class TestCase(totalDataPoints: Int, memoryLimit: Int)

case class FromDummyDataReport(totalDataPoints: Int,
                               memoryLimit: Int,
                               averageError: Double,
                               bestError: Double,
                               worstError: Double,
                               averageDistinctCounts: Double,
                               errorsAndDistinctCounts: List[(Double, Int)]) {
  def toTSV: String = productIterator.toList.dropRight(1).mkString("\t") + "\t" +
    errorsAndDistinctCounts.map(_.productIterator.mkString(",")).mkString("\t")
}

object FromDummyDataReport {
  def apply(totalDataPoints: Int,
            memoryLimit: Int,
            errorsAndDistinctCounts: List[(Double, Int)]): FromDummyDataReport = FromDummyDataReport(
    totalDataPoints = totalDataPoints,
    memoryLimit = memoryLimit,
    averageError = errorsAndDistinctCounts.map(_._1).sum / errorsAndDistinctCounts.map(_._1).size,
    bestError = errorsAndDistinctCounts.map(_._1).min,
    worstError = errorsAndDistinctCounts.map(_._1).max,
    averageDistinctCounts = errorsAndDistinctCounts.map(_._2).sum.toDouble / errorsAndDistinctCounts.map(_._2).size,
    errorsAndDistinctCounts = errorsAndDistinctCounts
  )
}

import Aggregator._

object ErrorEstimator {
  def fromTestData[T: ClassTag, M <: Aggregator[mutable.Map[(Long, Long), Long], Long, Double] : ClassTag](testData: RDD[(T, Long)],
                                                                                median: M,
                                                                                memoryCap: Int = 1000): FromTestDataReport = {
    val estimates: RDD[(T, Double)] = testData.aggByKey1(median :: HNil).mapValues(_.head)

    val correctMedianAndCounts: RDD[(T, (Double, Int))] =
      testData.groupBy(_._1).mapValues(_.map(_._2).toArray).flatMap {
        case (t, data) if data.length <= memoryCap => None
        case (t, data) => Some((t, (correctMedian(data), data.length)))
      }

    val errorsAndNumExamples: List[(Double, Int)] =
      estimates.join(correctMedianAndCounts).map {
        case (_, (estimate, (correct, count))) => (relativeError(estimate, correct), count)
      }
      .collect().toList

    val (errors, numExamples) = errorsAndNumExamples.unzip

    val size = errorsAndNumExamples.size

    if (size == 0) FromTestDataReport()
    else FromTestDataReport(
      errorsAndNumExamples = errorsAndNumExamples,
      averageError = errors.sum / size,
      bestError = errors.min,
      worstError = errors.max,
      averageNumExamples = numExamples.sum / size,
      mostNumExamples = numExamples.max,
      leastNumExamples = numExamples.min
    )
  }

  def correctMedian(numbers: Seq[Long]): Double = {
    val state = ExactMedian.zero
    numbers.foreach(ExactMedian.mutate(state, _))
    ExactMedian.result(state)
  }

  def relativeError[S, M <: Aggregator[S, Long, Double]](numbers: Seq[Long], median: M): Double = {
    val state = median.zero
    numbers.foreach(median.mutate(state, _))
    relativeError(median.result(state), correctMedian(numbers))
  }

  def relativeError(estimate: Double, correct: Double): Double = math.abs(correct - estimate) / correct

  val medianFac = MedianEstimator.apply _

  def cappedNormal(cap: Int): Rand[Long] = new Gaussian(cap / 2.0, cap / 6.0).map(math.floor).map {
    case i if i < 0 => 0L
    case i if i > cap => cap.toLong
    case i => i.toLong
  }

  def runExperimentsSlidingWindow(cases: Iterator[TestCase],
                                  runs: Int = 100,
                                  rand: Rand[Long] = cappedNormal(10000)): Iterator[FromDummyDataReport] =
    cases.map {
      case TestCase(totalDataPoints, memoryLimit) =>
        FromDummyDataReport(
          totalDataPoints = totalDataPoints,
          memoryLimit = memoryLimit,
          errorsAndDistinctCounts =
            (1 to runs)
            .map(_ => rand.sample(totalDataPoints).toList)
            .map(data => (
              ErrorEstimator.relativeError[mutable.Map[(Long, Long), Long], MedianEstimator](data, medianFac(memoryLimit)), data.distinct.size)).toList
        )
    }

  def randomiseRDD[T: ClassTag](rdd: RDD[T]): RDD[T] =
    rdd.map((new Random().nextInt(), _)).partitionBy(new HashPartitioner(rdd.partitions.length)).map(_._2)

  // TODO Forgot to do this TDD cos I thought it was gona be much simpler.
    def runExperimentsMapReduce(cases: Iterator[TestCase],
                                sc: SparkContext,
                                runs: Int = 100,
                                rand: () => Rand[Long] = () => cappedNormal(10000),
                                partitions: Int = 100): Array[FromDummyDataReport] = {
    ???


//    randomiseRDD(sc.makeRDD(cases.toSeq, partitions).flatMap {
//      case testCase@TestCase(totalDataPoints, memoryLimit) =>
//        val median = new MedianEstimator(memoryLimit)
//        (1 to runs).flatMap(run => {
//          val data = rand().sample(totalDataPoints).toList
//          val distinctCount = data.distinct.size
//          data.map((TestCaseKey(testCase, run, distinctCount, correctMedian(data)), _))
//        })
//        .map(kv => (kv._1, median.mutate(kv._2)))
//    })
//    .mapPartitions(_.toList.groupBy(_._1).mapValues(_.map(_._2).reduce(_ + _)).toIterator)
//    .reduceByKey(_ + _)
//    .map {
//      case (TestCaseKey(testCase, run, distinctCount, correctMedian), median) => testCase ->(distinctCount, correctMedian, median)
//    }
//    .groupByKey().map {
//      case (TestCase(totalDataPoints, memoryLimit), results) => FromDummyDataReport(
//        totalDataPoints = totalDataPoints,
//        memoryLimit = memoryLimit,
//        errorsAndDistinctCounts =
//          results.map {
//            case (distinctCount, correctMedian, median) =>
//              (relativeError(median.result, correctMedian), distinctCount)
//          }
//          .toList
//      )
//    }
//    .collect()
  }
}

case class TestCaseKey(testCase: TestCase, run: Int, distinctCount: Int, correctMedian: Double)
