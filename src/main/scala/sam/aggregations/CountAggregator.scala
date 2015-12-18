package sam.aggregations

class LongMutable(var l: Long) extends Serializable {
  def copy(): LongMutable = LongMutable(l)
}

object LongMutable {
  def apply(l: Long): LongMutable = new LongMutable(l)
}

// TODO V should just be Any then this can be a case object
class CountAggregator[V] extends Aggregator[LongMutable, V, Long] {
  def mutate(state: LongMutable, e: V): Unit = state.l += 1
  def mutateAdd(state: LongMutable, e: LongMutable): Unit = state.l += e.l
  def result(state: LongMutable): Long = state.l
  def zero: LongMutable = new LongMutable(0L)
}
