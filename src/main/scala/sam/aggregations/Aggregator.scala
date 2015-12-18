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

trait Aggregator[S, V, +R] extends Serializable {
  //with Semigroup[S] {
  def mutate(state: S, element: V): Unit
  def mutateAdd(stateL: S, stateR: S): Unit
  def result(state: S): R
  def zero: S
}

object Aggregator {
  // TODO an optimizer of some sort, e.g. when we ask for CountHistogram and Count, we can combine these


  implicit class PimpedPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {

    // TODO Optional Boolean param for each step so user can say if they want
    // to keep that level


    def aggByKey1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): RDD[(K, R1 :: HNil)] =
      aggByKeyState1(aggregator).mapValues(aggsToResults1(aggregator, _))


    def aggByKeyState1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): RDD[(K, S1 :: HNil)] = {
      val updateStates: (S1 :: HNil, V) => S1 :: HNil = mutate1(aggregator, _, _)
      rdd.combineByKey(updateStates(zeros1(aggregator), _), updateStates, merge1(aggregator, _, _))
    }

    // TODO We can't reuse this method in the aggTree, so might as well ignore
    def aggByKeyUpStateUncatted1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil,
                                         coveringKeys: K => List[K]): (RDD[(K, S1 :: HNil)], RDD[(K, S1 :: HNil)]) = {
      val firstLevel = aggByKeyState1(aggregator)
      (firstLevel, firstLevel.flatMap(kv => coveringKeys(kv._1).map(_ -> kv._2))
                   .reduceByKey(merge1(aggregator, _, _)))
    }

    // Will include the initial keys
    def aggByKeyUp1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil,
                            coveringKeys: K => List[K]): RDD[(K, R1 :: HNil)] = {
      val (firstLevel, secondLevel) = aggByKeyUpStateUncatted1(aggregator, coveringKeys)
      (firstLevel ++ secondLevel).mapValues(aggsToResults1(aggregator, _))


    }
  }

  implicit class PimpedRDD[V: ClassTag](rdd: RDD[V]) {
    def agg1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): R1 :: HNil = {
      val updateStates: (S1 :: HNil, V) => S1 :: HNil = mutate1(aggregator, _, _)
      aggsToResults1(aggregator, rdd.map(updateStates.curried(zeros1(aggregator))).reduce(merge1(aggregator, _, _)))
    }
  }
}

object MorePimps {

  import sam.aggregations.Aggregator.PimpedPairRDD

  implicit class PimpedPairRDDWithTreeAgg[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    // tree.length is the depth of the tree,
    // K => Nil signals early terminations of the tree

    /** tree is a list of functions from finer granularity keys to lists of coarser granularity keys.
      * User must choose tree for the domain to balance
      * number of stages against amount of data in each stage. See unit tests for examples. */
    def aggTree1[S1 <: {def copy(): S1}, R1, KSuper >: K : ClassTag](aggregator: Aggregator[S1, V, R1] :: HNil,
                                                 tree: List[KSuper => List[KSuper]]): List[RDD[(KSuper, R1 :: HNil)]] = {
      // RDDs are invariant in T, hence horrible asInstanceOf
      val rddKSuper: RDD[(KSuper, V)] = rdd.asInstanceOf[RDD[(KSuper, V)]]

      var treeList = List[RDD[(KSuper, S1 :: HNil)]](rddKSuper.aggByKeyState1(aggregator))
      for (f <- tree) {
        treeList :+=
          treeList.last.flatMap {
            case (key, state) => f(key).map(_ -> (state.head.copy() :: HNil))
          }

//          .reduceByKey(merge1(aggregator, _, _))

          .reduceByKey(merge1(aggregator, _, _))
      }

      //
      //      (if (tree.isEmpty) {
      //        List(rddKSuper.aggByKeyState1(aggregator))
      //      } else if (tree.size == 1) {
      //        List(
      //          rddKSuper.aggByKeyState1(aggregator),
      //
      //          rddKSuper.aggByKeyState1(aggregator).flatMap {
      //            case (key, state) => tree.head(key).map(_ -> state)
      //          }
      //          .reduceByKey(merge1(aggregator, _, _))
      //        )
      //      } else {
      //        List(
      //          rddKSuper.aggByKeyState1(aggregator),
      //
      //          rddKSuper.aggByKeyState1(aggregator).flatMap {
      //            case (key, state) => tree.head(key).map(_ -> state)
      //          }
      //          .reduceByKey(merge1(aggregator, _, _)),
      //
      //          rddKSuper.aggByKeyState1(aggregator)
      //          .flatMap {
      //            case (key, state) => tree.head(key).map(_ -> state)
      //          }
      //          .reduceByKey(merge1(aggregator, _, _))
      //          .flatMap {
      //            case (key, state) => tree(1)(key).map(_ -> state)
      //          }
      //          .reduceByKey(merge1(aggregator, _, _))
      //        )
      //      })

      treeList.map(rdd => rdd.mapValues(aggsToResults1(aggregator, _)))
    }
  }
}
