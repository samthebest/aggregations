package sam.aggregations

import org.apache.spark.rdd.RDD
import sam.aggregations.boiler_plate.AggToResultsCode._
import sam.aggregations.boiler_plate.MergeCode._
import sam.aggregations.boiler_plate.MutateCode._
import sam.aggregations.boiler_plate.ZerosCode._
import sam.aggregations.boiler_plate.CopyCode._
import shapeless._
import HList._

import scala.reflect.ClassTag
//import scalaz.{Semigroup, Monoid}

// Extending Semigroup causes serialization issues, should try to resolve somehow

trait Aggregator[S, -V, +R] extends Serializable {
  //with Semigroup[S] {
  def mutate(state: S, element: V): Unit
  def mutateAdd(stateL: S, stateR: S): Unit
  def result(state: S): R
  def zero: S
  def copyState(state: S): S
}

object Aggregator {
  // TODO an optimizer of some sort, e.g. when we ask for CountHistogram and Count, we can combine these


  implicit class PimpedPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    // TODO Optional Boolean param for each step so user can say if they want to keep that level

    /** tree is a list of functions from finer granularity keys to lists of coarser granularity keys.
      * User must choose tree for the domain to balance number of stages against amount of data in each stage.
      * See unit tests for examples.
      *
      * This will cause tree.length (i.e. tree depth) spark stages.  It caches each intermediate layer, the user
      * can choose to uncache the layers. */
    def aggTree1[S1, R1, KSuper >: K : ClassTag](aggregator: Aggregator[S1, V, R1] :: HNil,
                                                 tree: List[KSuper => List[KSuper]]): List[RDD[(KSuper, R1 :: HNil)]] =
    // asInstanceOf due to https://issues.apache.org/jira/browse/SPARK-1296
      tree.foldLeft(List(rdd.asInstanceOf[RDD[(KSuper, V)]].aggByKeyState1(aggregator)))((cum, branch) =>
        cum :+
          cum.last.cache().flatMap {
            case (key, state) => branch(key).map(_ -> copyStates1(aggregator, state))
          }
          .reduceByKey(merge1(aggregator, _, _))
      )
      .map(_.mapValues(aggsToResults1(aggregator, _)))

    def aggByKey1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): RDD[(K, R1 :: HNil)] =
      aggByKeyState1(aggregator).mapValues(aggsToResults1(aggregator, _))

//    def aggByKey2[S1, R1, S2, R2](aggregator: Aggregator[S1, V, R1] :: Aggregator[S2, V, R2] :: HNil): RDD[(K, R1 :: R2 :: HNil)] =
//      aggByKeyState2(aggregator).mapValues(aggsToResults1(aggregator, _))

    def aggByKeyState1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): RDD[(K, S1 :: HNil)] = {
      val updateStates: (S1 :: HNil, V) => S1 :: HNil = mutate1(aggregator, _, _)
      rdd.combineByKey(updateStates(zeros1(aggregator), _), updateStates, merge1(aggregator, _, _))
    }

//    def aggByKeyState2[S1, R1, S2, R2](aggregator: Aggregator[S1, V, R1] :: Aggregator[S2, V, R2] :: HNil): RDD[(K, S1 :: S2 :: HNil)] = {
//      val updateStates: (S1 :: S2 :: HNil, V) => S1 :: S2 :: HNil = mutate2(aggregator, _, _)
//      rdd.combineByKey(updateStates(zeros2(aggregator), _), updateStates, merge2(aggregator, _, _))
//    }
  }

  implicit class PimpedRDD[V: ClassTag](rdd: RDD[V]) {
    def agg1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): R1 :: HNil = {
      val updateStates: (S1 :: HNil, V) => S1 :: HNil = mutate1(aggregator, _, _)
      aggsToResults1(aggregator, rdd.map(updateStates.curried(zeros1(aggregator))).reduce(merge1(aggregator, _, _)))
    }
  }
}
