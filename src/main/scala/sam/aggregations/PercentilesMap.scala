package sam.aggregations

import scala.collection.mutable
import scala.reflect.ClassTag

// Basic implementation, but massive scope for optimization for cases where there are many distinct T values
case class PercentilesMap[T: ClassTag](implicit ordering: Ordering[T]) extends Aggregator[mutable.Map[T, Long], T, T => Option[Int]] {
  val hist = CountHistogram[T]()

  def mutate(state: mutable.Map[T, Long], e: T): Unit = hist.mutate(state, e)
  def mutateAdd(state: mutable.Map[T, Long], e: mutable.Map[T, Long]): Unit = hist.mutateAdd(state, e)
  def zero: mutable.Map[T, Long] = hist.zero

  def result(state: mutable.Map[T, Long]): T => Option[Int] = Utils.percentilesMap(state.toMap)
}
