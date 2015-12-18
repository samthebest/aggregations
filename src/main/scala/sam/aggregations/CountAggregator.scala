package sam.aggregations

case class LongMutable(var l: Long) extends Serializable

case object CountAggregator extends Aggregator[LongMutable, Any, Long] {
  def mutate(state: LongMutable, e: Any): Unit = state.l += 1
  def mutateAdd(state: LongMutable, e: LongMutable): Unit = state.l += e.l
  def result(state: LongMutable): Long = state.l
  def zero: LongMutable = new LongMutable(0L)
  def copyStates(state: LongMutable): LongMutable = LongMutable(state.l)
}
