package sam.aggregations

import org.apache.spark.rdd.RDD
import shapeless._
import HList._
import shapeless.ops.hlist._

import scala.reflect.ClassTag
//import scalaz.{Semigroup, Monoid}

// Extending Semigroup causes serialization issues, should try to resolve somehow

trait Aggregator[S, V, +R] extends Serializable {//with Semigroup[S] {
  def mutate(state: S, e: V): Unit
  def mutateAdd(state: S, e: S): Unit
  def result(state: S): R
  def zero: S
}

object Aggregator {
  def aggsToResults0(agg: HNil, state: HNil): HNil = HNil

  def aggsToResults1[R1, S1](agg: Aggregator[S1, _, R1] :: HNil, states: S1 :: HNil): R1 :: HNil = agg.head.result(states.head) :: aggsToResults0(agg.tail, states.tail)
  def aggsToResults2[R1, S1, R2, S2](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: HNil, states: S1 :: S2 :: HNil): R1 :: R2 :: HNil = agg.head.result(states.head) :: aggsToResults1(agg.tail, states.tail)
  def aggsToResults3[R1, S1, R2, S2, R3, S3](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: HNil, states: S1 :: S2 :: S3 :: HNil): R1 :: R2 :: R3 :: HNil = agg.head.result(states.head) :: aggsToResults2(agg.tail, states.tail)
  def aggsToResults4[R1, S1, R2, S2, R3, S3, R4, S4](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: Aggregator[S4, _, R4] :: HNil, states: S1 :: S2 :: S3 :: S4 :: HNil): R1 :: R2 :: R3 :: R4 :: HNil = agg.head.result(states.head) :: aggsToResults3(agg.tail, states.tail)
  def aggsToResults5[R1, S1, R2, S2, R3, S3, R4, S4, R5, S5](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: Aggregator[S4, _, R4] :: Aggregator[S5, _, R5] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: HNil): R1 :: R2 :: R3 :: R4 :: R5 :: HNil = agg.head.result(states.head) :: aggsToResults4(agg.tail, states.tail)

  def zeros0(agg: HNil): HNil = HNil

  def zeros1[S1](agg: Aggregator[S1, _, _] :: HNil): S1 :: HNil = agg.head.zero :: zeros0(agg.tail)
  def zeros2[S1, S2](agg: Aggregator[S1, _, _] :: Aggregator[S2, _, _] :: HNil): S1 :: S2 :: HNil = agg.head.zero :: zeros1(agg.tail)


  def mutate0[V](agg: HNil, state: HNil, v: V): HNil = HNil

  def mutate1[V, S1](agg: Aggregator[S1, V, _] :: HNil, state: S1 :: HNil, v: V): S1 :: HNil = {
    agg.head.mutate(state.head, v)
    mutate0(agg.tail, state.tail, v)
    state
  }

  def merge0[V](agg: HNil, state: HNil, toMergeIn: HNil): HNil = HNil

  def merge1[V, S1](agg: Aggregator[S1, V, _] :: HNil, state: S1 :: HNil, toMergeIn: S1 :: HNil): S1 :: HNil = {
    agg.head.mutateAdd(state.head, toMergeIn.head)
    merge0(agg.tail, state.tail, toMergeIn.tail)
    state
  }

  implicit class PimpedPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def aggsByKey1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): RDD[(K, R1 :: HNil)] = {
      val updateStates: (S1 :: HNil, V) => S1 :: HNil = mutate1(aggregator, _, _)
      (rdd.combineByKey(updateStates(zeros1(aggregator), _), updateStates, merge1(aggregator, _, _)): RDD[(K, S1 :: HNil)])
      .mapValues(y => aggsToResults1(aggregator, y))
    }
  }
}

