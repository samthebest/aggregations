package sam.aggregations

import org.scalacheck.{Prop, Arbitrary, Gen}
import org.specs2.ScalaCheck
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification

import scala.util.{Random, Success, Try}

object MedianSpecUtils {
  // Move to unit tests and wrap other code
  def normalDistribution[S, M <: Aggregator[S, Long, Double]](median: M, n: Int, max: Int): Double =
    ErrorEstimator.relativeError[S, M](ErrorEstimator.cappedNormal(max).sample(n), median)

  val rand = new Random()

  def uniformDistribution[S, M <: Aggregator[S, Long, Double]](median: M, n: Int, max: Int): Double =
    ErrorEstimator.relativeError[S, M]((1 to n).map(_ => rand.nextInt(max).toLong), median)
}

import MedianSpecUtils._

class MedianSpecUtils extends Specification with ScalaCheck {

  implicit def toProp(m: MatchResult[Any]): Prop = resultProp(m)

  def basicMedianSpecs[S, T <: Aggregator[S, Long, Double]](fac: () => T, desc: String = "ExactMedian"): Unit =
    "Median aggregator " + desc should {
      "Throw exception when called with no update ever being called" in {
        val median = fac()
        val state = median.zero
        Try(median.result(state)) match {
          case _: Success[_] => failure("Did not throw exception but should have done")
          case t if t.failed.get.isInstanceOf[IllegalArgumentException] => success
          case t if t.failed.get.isInstanceOf[IllegalArgumentException] =>
            failure("Wrong type of exception: " + t.failed.get)
        }
      }

      "Return 6 when call update with just 6" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 6L)
        median.result(state) must_=== 6.0
      }

      "Return 88 when call update with just 88" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 88L)
        median.result(state) must_=== 88.0
      }

      "Return single element when call update with just a single element" ! check((elem: Long) => {
        val median = fac()
        val state = median.zero
        median.mutate(state, elem)
        median.result(state) must_=== elem.toDouble
      })

      "Return 4 when call update with just 4 and 4" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 4L, 4L)
        median.result(state) must_=== 4.0
      }

      "Return 77 when call update with just 77 and 77" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 77L, 77L)
        median.result(state) must_=== 77.0
      }

      "Return 1.5 when call update with just 1 and 2" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 1L, 2L)
        median.result(state) must_=== 1.5
      }

      "Return 2 when call update with just 1 and 2 and 3" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 1L, 2L, 3L)
        median.result(state) must_=== 2.0
      }

      "Return 2 when call update with just 3 and 1 and 2" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 3L, 1L, 2L)
        median.result(state) must_=== 2.0
      }

      "Return 4.5 when call update with just 3 and 6 and 4 and 5" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 3L, 6L, 4L, 5L)
        median.result(state) must_=== 4.5
      }

      "Return 55 when call update with just 55 and 456 and 4 and 5 and 999" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 55L, 456L, 4L, 5L, 999L)
        median.result(state) must_=== 55.0
      }

      "Return 60 when call update with just 55 and 456 and 4 and 5 and 999 and 65" in {
        val median = fac()
        val state = median.zero
        median.mutate(state, 55L, 456L, 4L, 5L, 999L, 65L)
        median.result(state) must_=== 60.0
      }
    }

  def sufficientMemoryProperties[S, T <: Aggregator[S, Long, Double]](memCappedFac: Int => T): Unit = {
    "median with sufficient memory 1" should {
      implicit val arbitraryParams: Arbitrary[(Int, Int, Int)] = Arbitrary(
        for {
          limit <- Gen.frequency((1, 10), (1, 20), (1, 50))
          n <- Gen.choose(5, limit)
          max <- Gen.choose(5, 500)
        } yield (limit, n, max)
      )

      "Give correct answer for Normal dist when number of examples is less than limit" ! check(
        (params: (Int, Int, Int)) => (params.productIterator.forall(_.asInstanceOf[Int] > 0) &&
          params._2 <= params._1) ==> (params match {
          case (limit, n, max) =>
            val median = memCappedFac(limit)
            normalDistribution[S, T](median, n, max) must_=== 0.0
        }))

      "return 60 when we update via another median with just 55 and 456 and 4 and 5 and 999 and 65" in {
        val median = memCappedFac(10)
        val state = median.zero
        median.mutate(state, 55L, 456L, 4L)

        val median2 = memCappedFac(10)
        val state2 = median.zero
        median2.mutate(state2, 5L, 999L, 65L)

        median.mutateAdd(state, state2)

        median.result(state) must_=== 60.0
      }
    }

    "median with sufficient memory 2" should {
      val bigMemCap = 500
      val longGen = Gen.choose(Long.MinValue / 2, Long.MaxValue / 2)
      implicit val arbitraryListLongUpTo50: Arbitrary[List[Long]] = Arbitrary(Gen.frequency(
        (1, Gen.listOfN(1, longGen)),
        (1, Gen.listOfN(2, longGen)),
        (1, Gen.listOfN(3, longGen)),
        (1, Gen.listOfN(5, longGen)),
        (1, Gen.listOfN(10, longGen)),
        (1, Gen.listOfN(25, longGen)),
        (1, Gen.listOfN(50, longGen))
      ))

      "2 medians produce same results as a single median" ! check((l1: List[Long], l2: List[Long]) => {
        val median = memCappedFac(bigMemCap)
        val state = median.zero

        Random.shuffle(l1 ++ l2).foreach(median.mutate(state, _))

        val median1 = memCappedFac(bigMemCap)
        val state1 = median1.zero
        val median2 = memCappedFac(bigMemCap)
        val state2 = median2.zero

        l1.foreach(median1.mutate(state1, _))
        l2.foreach(median2.mutate(state2, _))

        median1.mutateAdd(state1, state2)

        median1.result(state1) must_=== median.result(state)
      })

      "5 medians produce same results as a single median" ! check(
        (l1: List[Long], l2: List[Long], l3: List[Long], l4: List[Long], l5: List[Long]) => {
          val median = memCappedFac(bigMemCap)
          val state = median.zero

          Random.shuffle(l1 ++ l2 ++ l3 ++ l4 ++ l5).foreach(median.mutate(state, _))

          val median1 = memCappedFac(bigMemCap)
          val state1 = median1.zero
          val median2 = memCappedFac(bigMemCap)
          val state2 = median2.zero
          val median3 = memCappedFac(bigMemCap)
          val state3 = median3.zero
          val median4 = memCappedFac(bigMemCap)
          val state4 = median4.zero
          val median5 = memCappedFac(bigMemCap)
          val state5 = median5.zero

          l1.foreach(median1.mutate(state1, _))
          l2.foreach(median1.mutate(state2, _))
          l3.foreach(median1.mutate(state3, _))
          l4.foreach(median1.mutate(state4, _))
          l5.foreach(median1.mutate(state5, _))

          median1.mutateAdd(state1, state2)
          median1.mutateAdd(state1, state3)
          median1.mutateAdd(state1, state4)
          median1.mutateAdd(state1, state5)

          median1.result(state1) must_=== median.result(state)
        })
    }
  }

  def medianIsCommutative[S, T <: Aggregator[S, Long, Double]](memCappedFac: Int => T): Unit =
    "Median" should {
      val longGen = Gen.choose(Long.MinValue / 2, Long.MaxValue / 2)
      implicit val arbitraryListLongUpTo50: Arbitrary[List[Long]] = Arbitrary(Gen.frequency(
        (1, Gen.listOfN(1, longGen)),
        (1, Gen.listOfN(2, longGen)),
        (1, Gen.listOfN(3, longGen)),
        (1, Gen.listOfN(5, longGen)),
        (1, Gen.listOfN(10, longGen)),
        (1, Gen.listOfN(25, longGen)),
        (1, Gen.listOfN(50, longGen))
      ))

      "Order not important even with small cap (i.e. is commutative)" ! check((l1: List[Long], l2: List[Long]) =>
        (l1.nonEmpty && l2.nonEmpty) ==> {
          val smallCap = 10

          val median1 = memCappedFac(smallCap)
          val state1 = median1.zero
          val median1Copy = memCappedFac(smallCap)
          val state1Copy = median1Copy.zero
          val median2 = memCappedFac(smallCap)
          val state2 = median2.zero

          l1.foreach(median1.mutate(state1, _))
          l1.foreach(median1Copy.mutate(state1Copy, _))
          l2.foreach(median2.mutate(state2, _))

          median1.mutateAdd(state1, state2)
          median2.mutateAdd(state2, state1Copy)

          median1.result(state1) must_=== median2.result(state2)
        })
    }

}
