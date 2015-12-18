package sam.aggregations.boiler_plate

import sam.aggregations.Aggregator
import shapeless.{::, HNil}

object MergeCode {
  def apply(num: Int): String =
    (1 to num).map(i => {
      val typeParms = (1 to i).map(j => s"S$j").mkString(", ")
      val aggType = (1 to i).map(j => s"Aggregator[S$j, V, _]").mkString(" :: ")
      val statesType = (1 to i).map(j => s"S$j").mkString(" :: ")
      val returnType = statesType
      val methodName = "merge"
      s"def $methodName$i[V, $typeParms](agg: $aggType :: HNil, states: $statesType :: HNil, toMergeIn: $statesType :: HNil): $returnType :: HNil = {\n" +
        s"  agg.head.mutateAdd(states.head, toMergeIn.head)\n" +
        s"  $methodName${i - 1}(agg.tail, states.tail, toMergeIn.tail)\n" +
        s"  states\n" +
        s"}"
    }).mkString("\n")

  def merge0[V](agg: HNil, state: HNil, toMergeIn: HNil): HNil = HNil

  def merge1[V, S1](agg: Aggregator[S1, V, _] :: HNil, states: S1 :: HNil, toMergeIn: S1 :: HNil): S1 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge0(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge2[V, S1, S2](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: HNil, states: S1 :: S2 :: HNil, toMergeIn: S1 :: S2 :: HNil): S1 :: S2 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge1(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge3[V, S1, S2, S3](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: HNil, states: S1 :: S2 :: S3 :: HNil, toMergeIn: S1 :: S2 :: S3 :: HNil): S1 :: S2 :: S3 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge2(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge4[V, S1, S2, S3, S4](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: HNil): S1 :: S2 :: S3 :: S4 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge3(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge5[V, S1, S2, S3, S4, S5](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge4(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge6[V, S1, S2, S3, S4, S5, S6](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge5(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge7[V, S1, S2, S3, S4, S5, S6, S7](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge6(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge8[V, S1, S2, S3, S4, S5, S6, S7, S8](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge7(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge9[V, S1, S2, S3, S4, S5, S6, S7, S8, S9](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge8(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge10[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge9(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge11[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge10(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge12[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge11(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge13[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge12(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge14[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge13(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge15[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge14(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge16[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge15(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge17[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge16(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge18[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge17(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge19[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: Aggregator[S19, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge18(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge20[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19, S20](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: Aggregator[S19, V, _] :: Aggregator[S20, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge19(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge21[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19, S20, S21](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: Aggregator[S19, V, _] :: Aggregator[S20, V, _] :: Aggregator[S21, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge20(agg.tail, states.tail, toMergeIn.tail)
    states
  }
  def merge22[V, S1, S2, S3, S4, S5, S6, S7, S8, S9, S10, S11, S12, S13, S14, S15, S16, S17, S18, S19, S20, S21, S22](agg: Aggregator[S1, V, _] :: Aggregator[S2, V, _] :: Aggregator[S3, V, _] :: Aggregator[S4, V, _] :: Aggregator[S5, V, _] :: Aggregator[S6, V, _] :: Aggregator[S7, V, _] :: Aggregator[S8, V, _] :: Aggregator[S9, V, _] :: Aggregator[S10, V, _] :: Aggregator[S11, V, _] :: Aggregator[S12, V, _] :: Aggregator[S13, V, _] :: Aggregator[S14, V, _] :: Aggregator[S15, V, _] :: Aggregator[S16, V, _] :: Aggregator[S17, V, _] :: Aggregator[S18, V, _] :: Aggregator[S19, V, _] :: Aggregator[S20, V, _] :: Aggregator[S21, V, _] :: Aggregator[S22, V, _] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: S22 :: HNil, toMergeIn: S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: S22 :: HNil): S1 :: S2 :: S3 :: S4 :: S5 :: S6 :: S7 :: S8 :: S9 :: S10 :: S11 :: S12 :: S13 :: S14 :: S15 :: S16 :: S17 :: S18 :: S19 :: S20 :: S21 :: S22 :: HNil = {
    agg.head.mutateAdd(states.head, toMergeIn.head)
    merge21(agg.tail, states.tail, toMergeIn.tail)
    states
  }

}
