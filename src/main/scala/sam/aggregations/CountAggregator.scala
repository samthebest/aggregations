package sam.aggregations

case class LongMutable(var l: Long)

class CountAggregator[V] extends Aggregator[LongMutable, V, Long] {
  def mutate(state: LongMutable, e: V): LongMutable = {
    state.l += 1
    state
  }
  def mutateAdd(state: LongMutable, e: LongMutable): LongMutable = {
    state.l += e.l
    state
  }
  def result(state: LongMutable): Long = state.l
  def zero: LongMutable = LongMutable(0L)
}
