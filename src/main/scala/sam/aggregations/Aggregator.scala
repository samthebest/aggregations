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
  def mutate(state: S, e: V): Unit
  def mutateAdd(state: S, e: S): Unit
  def result(state: S): R
  def zero: S
}

object Aggregator {
  // TODO an optimizer of some sort, e.g. when we ask for CountHistogram and Count, we can combine these

  implicit class PimpedPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
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

    // tree.length is the depth of the tree,
    // K => Nil signals early terminations of the tree

//    def aggTree1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil,
//                          tree: List[K => List[K]]): RDD[(K, R1 :: HNil)] = {
//
//      tree.foldLeft(Nil)((cum, cur) => {
//        val (lowerLevel, higherLevel) = aggByKeyUpStateUncatted1(aggregator, cur)
//
//      })
//
//      ???
//    }
  }

  implicit class PimpedRDD[V: ClassTag](rdd: RDD[V]) {
    def agg1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): R1 :: HNil = {
      val updateStates: (S1 :: HNil, V) => S1 :: HNil = mutate1(aggregator, _, _)
      aggsToResults1(aggregator, rdd.map(updateStates.curried(zeros1(aggregator))).reduce(merge1(aggregator, _, _)))
    }
  }
}

