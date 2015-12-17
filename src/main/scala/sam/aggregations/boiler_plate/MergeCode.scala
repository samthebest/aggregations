package sam.aggregations.boiler_plate

import sam.aggregations.Aggregator
import shapeless.{::, HNil}

object MergeCode {
  def apply(num: Int): String =
    (1 to num).map(i => {
      val typeParms = (1 to i).map(j => s"V, S$j").mkString(", ")
      val aggType = (1 to i).map(j => s"Aggregator[S$j, V, _]").mkString(" :: ")
      val statesType = (1 to i).map(j => s"S$j").mkString(" :: ")
      val returnType = statesType
      val methodName = "merge"
      s"def $methodName$i[$typeParms](agg: $aggType :: HNil, states: $statesType :: HNil, toMergeIn: $statesType :: HNil): $returnType :: HNil = {" +
        s"  agg.head.mutateAdd(states.head, toMergeIn.head)\n" +
        s"  $methodName$i(agg.tail, states.tail, v)\n" +
        s"  states\n" +
        s"}"
    }).mkString("\n")

  def merge0[V](agg: HNil, state: HNil, toMergeIn: HNil): HNil = HNil

  def merge1[V, S1](agg: Aggregator[S1, V, _] :: HNil, state: S1 :: HNil, toMergeIn: S1 :: HNil): S1 :: HNil = {
    agg.head.mutateAdd(state.head, toMergeIn.head)
    merge0(agg.tail, state.tail, toMergeIn.tail)
    state
  }
}
