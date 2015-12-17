package sam.aggregations

import scala.collection.mutable
import scala.reflect.ClassTag

case class CountHistogram[T: ClassTag]() extends Aggregator[mutable.Map[T, Long], T, Map[T, Long]] {
  def mutate(state: mutable.Map[T, Long], e: T): Unit = state.update(e, state.getOrElse(e, 0L) + 1)
  def mutateAdd(state: mutable.Map[T, Long], e: mutable.Map[T, Long]): Unit = e.foreach {
    case (key, value) => state.update(key, state.getOrElse(key, 0L) + value)
  }
  def result(state: mutable.Map[T, Long]): Map[T, Long] = state.toMap
  def zero: mutable.Map[T, Long] = mutable.Map.empty
}
