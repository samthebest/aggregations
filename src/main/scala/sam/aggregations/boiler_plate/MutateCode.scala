package sam.aggregations.boiler_plate

import sam.aggregations.Aggregator
import shapeless.{::, HNil}

object MutateCode {
  def apply(num: Int): String =
    (1 to num).map(i => {
      val typeParms = (1 to i).map(j => s"S$j").mkString(", ")
      val aggType = (1 to i).map(j => s"Aggregator[S$j, V, _]").mkString(" :: ")
      val statesType = (1 to i).map(j => s"S$j").mkString(" :: ")
      val returnType = statesType
      val methodName = "mutate"
      s"def $methodName$i[V, $typeParms](agg: $aggType :: HNil, states: $statesType :: HNil, v: V): $returnType :: HNil = {\n" +
        s"  agg.head.mutate(states.head, v)\n" +
        s"  $methodName${i - 1}(agg.tail, states.tail, v)\n" +
        s"  states\n" +
        s"}"
    }).mkString("\n")

  def mutate0[V](agg: HNil, state: HNil, v: V): HNil = HNil

  def mutate1[V, S1](agg: Aggregator[S1, V, _] :: HNil, states: S1 :: HNil, v: V): S1 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate0(agg.tail, states.tail, v)
    states
  }
  def mutate2[V, S1, S2](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: HNil, states: S1 :: S2 :: HNil, v: V): S1 :: S2 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate1(agg.tail, states.tail, v)
    states
  }
  def mutate3[V, S1, S2, S3](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: HNil, states: S1 :: S2 :: S3 :: HNil, v: V): S1 :: S2 :: S3 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate2(agg.tail, states.tail, v)
    states
  }
  def mutate4[V, S1, S2, S3, S4](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate3(agg.tail, states.tail, v)
    states
  }
  def mutate5[V, S1, S2, S3, S4, S5](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate4(agg.tail, states.tail, v)
    states
  }
  def mutate6[V, S1, S2, S3, S4, S5, S6](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate5(agg.tail, states.tail, v)
    states
  }
  def mutate7[V, S1, S2, S3, S4, S5, S6, S7](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate6(agg.tail, states.tail, v)
    states
  }
  def mutate8[V, S1, S2, S3, S4, S5, S6, S7, S8](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate7(agg.tail, states.tail, v)
    states
  }
  def mutate9[V, S1, S2, S3, S4, S5, S6, S7, S8, S9](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate8(agg.tail, states.tail, v)
    states
  }
  def mutate10[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate9(agg.tail, states.tail, v)
    states
  }
  def mutate11[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate10(agg.tail, states.tail, v)
    states
  }
  def mutate12[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate11(agg.tail, states.tail, v)
    states
  }
  def mutate13[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate12(agg.tail, states.tail, v)
    states
  }
  def mutate14[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate13(agg.tail, states.tail, v)
    states
  }
  def mutate15[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate14(agg.tail, states.tail, v)
    states
  }
  def mutate16[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate15(agg.tail, states.tail, v)
    states
  }
  def mutate17[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate16(agg.tail, states.tail, v)
    states
  }
  def mutate18[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate17(agg.tail, states.tail, v)
    states
  }
  def mutate19[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: Aggregator[S19, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate18(agg.tail, states.tail, v)
    states
  }
  def mutate20[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19, S20](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: Aggregator[S19, V, _] :: Aggregator[S20, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate19(agg.tail, states.tail, v)
    states
  }
  def mutate21[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19, S20, S21](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: Aggregator[S19, V, _] :: Aggregator[S20, V, _] :: Aggregator[S21, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate20(agg.tail, states.tail, v)
    states
  }
  def mutate22[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19, S20, S21, S22](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: Aggregator[S19, V, _] :: Aggregator[S20, V, _] :: Aggregator[S21, V, _] :: Aggregator[S22, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: S22 :: HNil, v: V): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: S22 :: HNil = {
    agg.head.mutate(states.head, v)
    mutate21(agg.tail, states.tail, v)
    states
  }

}
