package sam.aggregations

object RangeUtils {
  def rangesOverlap[T : Numeric](r1: (T, T), r2: (T, T)): Boolean = {
    val num = implicitly[Numeric[T]]
    import num.mkOrderingOps

    (r1, r2) match {
      case ((lower1, upper1), (lower2, _)) if lower1 < lower2 => upper1 >= lower2
      case ((lower1, _), (lower2, upper2)) if lower1 > lower2 => upper2 >= lower1
      case _ => true
    }
  }
}
