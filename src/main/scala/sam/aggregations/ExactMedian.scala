package sam.aggregations

class ExactMedian() extends Median {
  private var elems: List[Long] = Nil
  def getElems: List[Long] = elems
  def result: Double = elems match {
    case Nil => throw new IllegalArgumentException("Cannot call result when no updates called")
    case l =>
      val sorted = l.sorted
      val count = l.size
      if (count % 2 == 0) (sorted((count / 2) - 1) + sorted(count / 2)) / 2.0 else sorted(count / 2)
  }

  def update(e: Long): Unit = elems = e +: elems
  def update(m: Median): Unit = m match {
    case em: ExactMedian => elems = elems ++ em.getElems
  }
}
