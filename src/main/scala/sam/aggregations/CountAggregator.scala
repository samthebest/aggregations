package sam.aggregations

case class LongMutable(var l: Long)

class CountAggregator[V] extends Aggregator[LongMutable, V, Long] {
  def mutate(state: LongMutable, e: V): Unit = state.l += 1
  def mutateAdd(state: LongMutable, e: LongMutable): Unit = state.l += e.l
  def result(state: LongMutable): Long = state.l
  def zero: LongMutable = LongMutable(0L)
}
