package sam.aggregations.boiler_plate

import sam.aggregations.Aggregator
import shapeless.{::, HNil}

object AggToResultsCode {
  // TODO Get a PHd in Scala Macros so I can work out how to do really simple stuff like this using macros
  def apply(num: Int): String =
    (1 to num).map(i => {
      val typeParms = (1 to i).map(j => s"R$j, S$j").mkString(", ")
      val aggType = (1 to i).map(j => s"Aggregator[S$j, _, R$j]").mkString(" :: ")
      val statesType = (1 to i).map(j => s"S$j").mkString(" :: ")
      val returnType = (1 to i).map(j => s"R$j").mkString(" :: ")
      val methodName = "aggsToResults"
      s"def $methodName$i[$typeParms](agg: $aggType :: HNil, states: $statesType :: HNil): $returnType :: HNil = " +
        s"agg.head.result(states.head) :: $methodName${i - 1}(agg.tail, states.tail)"
    }).mkString("\n")

  def aggsToResults0(agg: HNil, state: HNil): HNil = HNil

  def aggsToResults1[R1, S1](agg: Aggregator[S1, _, R1] :: HNil, states: S1 :: HNil): R1 :: HNil = agg.head.result(states.head) :: aggsToResults0(agg.tail, states.tail)
  def aggsToResults2[R1, S1, R2, S2](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: HNil, states: S1 :: S2 :: HNil): R1 :: R2 :: HNil = agg.head.result(states.head) :: aggsToResults1(agg.tail, states.tail)
  def aggsToResults3[R1, S1, R2, S2, R3, S3](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: HNil, states: S1 :: S2 :: S3 :: HNil): R1 :: R2 :: R3 :: HNil = agg.head.result(states.head) :: aggsToResults2(agg.tail, states.tail)
  def aggsToResults4[R1, S1, R2, S2, R3, S3, R4, S4](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: Aggregator[S4, _, R4] :: HNil, states: S1 :: S2 :: S3 :: S4 :: HNil): R1 :: R2 :: R3 :: R4 :: HNil = agg.head.result(states.head) :: aggsToResults3(agg.tail, states.tail)
  def aggsToResults5[R1, S1, R2, S2, R3, S3, R4, S4, R5, S5](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: Aggregator[S4, _, R4] :: Aggregator[S5, _, R5] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: HNil): R1 :: R2 :: R3 :: R4 :: R5 :: HNil = agg.head.result(states.head) :: aggsToResults4(agg.tail, states.tail)
}
