package sam.aggregations.boiler_plate

import sam.aggregations.Aggregator
import shapeless.{::, HNil}

object MutateCode {
  def apply(num: Int): String =
    (1 to num).map(i => {
      val typeParms = (1 to i).map(j => s"V, S$j").mkString(", ")
      val aggType = (1 to i).map(j => s"Aggregator[S$j, V, _]").mkString(" :: ")
      val statesType = (1 to i).map(j => s"S$j").mkString(" :: ")
      val returnType = statesType
      val methodName = "mutate"
      s"def $methodName$i[$typeParms](agg: $aggType :: HNil, states: $statesType :: HNil): $returnType :: HNil = {" +
        s"  agg.head.mutate(states.head, v)\n" +
        s"  $methodName$i(agg.tail, states.tail, v)\n" +
        s"  states\n" +
        s"}"
    }).mkString("\n")

  def mutate0[V](agg: HNil, state: HNil, v: V): HNil = HNil

  def mutate1[V, S1](agg: Aggregator[S1, V, _] :: HNil, state: S1 :: HNil, v: V): S1 :: HNil = {
    agg.head.mutate(state.head, v)
    mutate0(agg.tail, state.tail, v)
    state
  }
}
