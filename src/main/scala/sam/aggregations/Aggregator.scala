package sam.aggregations

import org.apache.spark.rdd.RDD
import sam.aggregations.boiler_plate.AggToResultsCode._
import sam.aggregations.boiler_plate.MergeCode._
import sam.aggregations.boiler_plate.MutateCode._
import sam.aggregations.boiler_plate.ZerosCode._
import shapeless._
import HList._

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
  implicit class PimpedPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def aggByKey1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): RDD[(K, R1 :: HNil)] = {
      val updateStates: (S1 :: HNil, V) => S1 :: HNil = mutate1(aggregator, _, _)
      (rdd.combineByKey(updateStates(zeros1(aggregator), _), updateStates, merge1(aggregator, _, _)): RDD[(K, S1 :: HNil)])
      .mapValues(y => aggsToResults1(aggregator, y))
    }
  }

  implicit class PimpedRDD[V: ClassTag](rdd: RDD[V]) {
    def agg1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): R1 :: HNil = {
      val updateStates: (S1 :: HNil, V) => S1 :: HNil = mutate1(aggregator, _, _)
      aggsToResults1(aggregator, rdd.map(updateStates.curried(zeros1(aggregator))).reduce(merge1(aggregator, _, _)))
    }
  }
}

