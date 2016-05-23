package sam.aggregations

import scala.reflect.ClassTag

object Utils {
  def overlap[T : Numeric](r1: (T, T), r2: (T, T)): Boolean = {
    val num = implicitly[Numeric[T]]
    import num.mkOrderingOps

    (r1, r2) match {
      case ((lower1, upper1), (lower2, _)) if lower1 < lower2 => upper1 >= lower2
      case ((lower1, _), (lower2, upper2)) if lower1 > lower2 => upper2 >= lower1
      case _ => true
    }
  }

  def cumulativeDensity[T](m: List[(T, Long)]): List[(T, Long)] =
    m.map(_._1).zip(m.drop(1).scanLeft(m.head._2)((cum, cur) => cum + cur._2))

  def percentiles[T: ClassTag](l: List[(T, Long)])(implicit ordering: Ordering[T]): Array[T] = nthtiles(100, l)

  // TODO Unit tests for case when total < n
  // TODO Think about case when num cumCounts.size < n, unit test
  def nthtiles[T: ClassTag](n: Int, l: List[(T, Long)])(implicit ordering: Ordering[T]): Array[T] = {
    val cumCounts = cumulativeDensity(l.sortBy(_._1))
    val total = cumCounts.last._2
    require(total >= n, "nthtiles don't make sense if we have less data points than n")
    val percentileSize = total.toDouble / n

    cumCounts.foldLeft((Nil: List[T], 0)) {
      case (cum@(cumPercentiles, curPercentile), (t, count)) =>
        if (count > percentileSize * curPercentile) (t +: cumPercentiles, curPercentile + 1)
        else cum
    }
    ._1.reverse.toArray
  }
}



// (Warning: has very little meaning, cannot be understood, invented by statisticians).
  // https://en.wikipedia.org/wiki/Percentile#Worked_Example_of_the_First_Variant
  def percentileLinInterpFirstVariant(values: List[Double], p: Double): Double = {
    val count = values.size
    require(count > 1, "Need at least 2 values")

    var ((value, index) :: tail, prevPercentRank, prevValue, result) =
      (values.sorted.zipWithIndex, Option.empty[Double], 0.0, Option.empty[Double])

    while (result.isEmpty) {
      (prevPercentRank, (100.0 / count) * (index + 1 - 0.5), tail) match {
        case (None, percentRank, _) if p < percentRank =>
          result = Some(value)
        case (_, percentRank, _) if p == percentRank =>
          result = Some(value)
        case (Some(prevPercentRank), percentRank, _) if prevPercentRank < p && p < percentRank =>
          result = Some(count * (p - prevPercentRank) * (value - prevValue) / 100 + prevValue)
        case (_, percentRank, Nil) =>
          result = Some(value)
        case (_, percentRank, (nextValue, nextIndex) :: nextTail) =>
          prevValue = value
          value = nextValue
          index = nextIndex
          tail = nextTail
          prevPercentRank = Some(percentRank)
      }
    }

    result.get
  }
  
  
  test("IMA.percentileLinInterpFirstVariant handles small sequences as per worked example on wikipedia: " +
    "https://en.wikipedia.org/wiki/Percentile#Worked_Example_of_the_First_Variant") {
    assert(IMA.percentileLinInterpFirstVariant(List(15, 20, 35, 40, 50), 5) == 15.0)
    assert(IMA.percentileLinInterpFirstVariant(List(15, 20, 35, 40, 50), 30) == 20.0)
    assert(IMA.percentileLinInterpFirstVariant(List(15, 20, 35, 40, 50), 40) == 27.5)
    assert(IMA.percentileLinInterpFirstVariant(List(15, 20, 35, 40, 50), 95) == 50)
  }

  test("IMA.percentileLinInterpFirstVariant handles weird cases") {
    assert(IMA.percentileLinInterpFirstVariant(List(0, 100), 50) == 50.0)
    assert(IMA.percentileLinInterpFirstVariant(List(0, 200), 50) == 100.0)
    assert(IMA.percentileLinInterpFirstVariant(List(0, 100), 50 + 12.5) == 75.0)
    assert(IMA.percentileLinInterpFirstVariant(List(0, 100, 200), 50) == 100.0)
    assert(IMA.percentileLinInterpFirstVariant(List(0, 100, 200, 300), 75) == 250.0)
  }
  
  
  test("IMA.percentileLinInterpFirstVariant handles ordinary cases") {
    assert(IMA.percentileLinInterpFirstVariant((1 to 99).map(_.toDouble).toList, 50) == 50.0)
    assert(IMA.percentileLinInterpFirstVariant((101 to 199).map(_.toDouble).toList, 50) == 150.0)
  }

