package sam.aggregations.boiler_plate

import sam.aggregations.Aggregator
import shapeless.{::, HNil}

object ZerosCode {
  def apply(num: Int): String =
    (1 to num).map(i => {
      val typeParms = (1 to i).map(j => s"S$j").mkString(", ")
      val aggType = (1 to i).map(j => s"Aggregator[S$j, _, _]").mkString(" :: ")
      val returnType = (1 to i).map(j => s"S$j").mkString(" :: ")
      val methodName = "zeros"
      s"def $methodName$i[$typeParms](agg: $aggType :: HNil): $returnType :: HNil = " +
        s"agg.head.zero :: $methodName${i - 1}(agg.tail)"
    }).mkString("\n")

  def zeros0(agg: HNil): HNil = HNil

  def zeros1[S1](agg: Aggregator[S1, _, _] :: HNil): S1 :: HNil = agg.head.zero :: zeros0(agg.tail)
  def zeros2[S1, S2](agg: Aggregator[S1, _, _] :: Aggregator[S2, _, _] :: HNil): S1 :: S2 :: HNil = agg.head.zero :: zeros1(agg.tail)

}
