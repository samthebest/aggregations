package sam.aggregations.aggregators

import sam.aggregations.Aggregator

import scala.collection.mutable

case object ExactMedian extends Aggregator[mutable.MutableList[Long], Long, Double] {
  def mutate(state: mutable.MutableList[Long], element: Long): Unit = state.+=:(element)
  def mutateAdd(stateL: mutable.MutableList[Long], stateR: mutable.MutableList[Long]): Unit = stateL.++=(stateR)

  def result(state: mutable.MutableList[Long]): Double = state.toList match {
    case Nil => throw new IllegalArgumentException("Cannot call result when no updates called")
    case l =>
      val sorted = l.sorted
      val count = l.size
      if (count % 2 == 0) (sorted((count / 2) - 1) + sorted(count / 2)) / 2.0 else sorted(count / 2)
  }

  def copyStates(state: mutable.MutableList[Long]): mutable.MutableList[Long] = ???
  def zero: mutable.MutableList[Long] = mutable.MutableList.empty
}
