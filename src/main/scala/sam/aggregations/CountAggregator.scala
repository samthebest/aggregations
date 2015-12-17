package sam.aggregations

class CountAggregator[V] extends Aggregator[Long, V, Long] {
  def mutate(state: Long, e: V): Long = state + 1
  def mutateAdd(state: Long, e: Long): Long = state + e
  def result(state: Long): Long = state
  def zero: Long = 0L
}
