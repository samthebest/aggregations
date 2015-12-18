package sam.aggregations.aggregators

import sam.aggregations.Aggregator
import scala.collection.mutable
import scala.reflect.ClassTag

case class CountHistogram[T: ClassTag]() extends Aggregator[mutable.Map[T, Long], T, Map[T, Long]] {
  def mutate(state: mutable.Map[T, Long], element: T): Unit = state.update(element, state.getOrElse(element, 0L) + 1)
  def mutateAdd(stateL: mutable.Map[T, Long], stateR: mutable.Map[T, Long]): Unit = stateR.foreach {
    case (key, value) => stateL.update(key, stateL.getOrElse(key, 0L) + value)
  }
  def result(state: mutable.Map[T, Long]): Map[T, Long] = state.toMap
  def zero: mutable.Map[T, Long] = mutable.Map.empty
  def copyStates(state: mutable.Map[T, Long]): mutable.Map[T, Long] = mutable.Map.empty ++ state
}
