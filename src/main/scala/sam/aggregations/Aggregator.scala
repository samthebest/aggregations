package sam.aggregations

import breeze.storage.Zero
import org.apache.spark.{AccumulatorParam, Accumulator}
import org.apache.spark.rdd.RDD
import shapeless._
import HList._
import shapeless.ops.hlist._
//import syntax.std.tuple._
//import Zipper._
//import shapeless.ops.traversable.FromTraversable._

import scala.reflect.ClassTag
import scalaz.{Semigroup, Monoid}

// TODO Reconsider design ... consider Aggregator being a type-class
// Then we can supply case objects, furthermore user need not worry about the complex type params
// Finally, can then just HLists for the state and result (getting toTuple for free and stuff)

// S must be mutable.  If it had custom serialization, we could then have custom serialization for HLists

// Need to redesign this so user only defines
// def mutate(state, e): Unit
// def mutateAdd(state, state): Unit

// To make it clear that user must mutate state

trait Aggregator[S, V, +R] extends Semigroup[S] {
  def mutate(state: S, e: V): S
  def mutateAdd(state: S, e: S): S
  def result(state: S): R
  def zero: S

  def append(f1: S, f2: => S): S = mutateAdd(f1, f2)

  //  type AggOps = T forSome { type T <: AggregatorOps[R, V, T] }

  //  def mkAggOps = new AggregatorOps[R, V, AggOps] {
  //    var s: S = zero
  //    def update(e: V): Unit = s = mutate(s, e)
  //    def update(a: AggOps): Unit = s = mutate(s, a.s)
  //    def r: R = result(s)
  //  }
}

object Aggregator {
  def aggsToResults0(agg: HNil, state: HNil): HNil = HNil

  def aggsToResults1[R1, S1](agg: Aggregator[S1, _, R1] :: HNil, states: S1 :: HNil): R1 :: HNil = agg.head.result(states.head) :: aggsToResults0(agg.tail, states.tail)
  def aggsToResults2[R1, S1, R2, S2](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: HNil, states: S1 :: S2 :: HNil): R1 :: R2 :: HNil = agg.head.result(states.head) :: aggsToResults1(agg.tail, states.tail)
  def aggsToResults3[R1, S1, R2, S2, R3, S3](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: HNil, states: S1 :: S2 :: S3 :: HNil): R1 :: R2 :: R3 :: HNil = agg.head.result(states.head) :: aggsToResults2(agg.tail, states.tail)
  def aggsToResults4[R1, S1, R2, S2, R3, S3, R4, S4](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: Aggregator[S4, _, R4] :: HNil, states: S1 :: S2 :: S3 :: S4 :: HNil): R1 :: R2 :: R3 :: R4 :: HNil = agg.head.result(states.head) :: aggsToResults3(agg.tail, states.tail)
  def aggsToResults5[R1, S1, R2, S2, R3, S3, R4, S4, R5, S5](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: Aggregator[S4, _, R4] :: Aggregator[S5, _, R5] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: HNil): R1 :: R2 :: R3 :: R4 :: R5 :: HNil = agg.head.result(states.head) :: aggsToResults4(agg.tail, states.tail)

  object zeroGetter extends Poly1 {
    implicit def caseZero[S1] = at[Aggregator[S1, _, _]](_.zero)
  }


  def zeros0(agg: HNil): HNil = HNil

  def zeros1[S1](agg: Aggregator[S1, _, _] :: HNil): S1 :: HNil = agg.head.zero :: zeros0(agg.tail)
  def zeros2[S1, S2](agg: Aggregator[S1, _, _] :: Aggregator[S2, _, _] :: HNil): S1 :: S2 :: HNil = agg.head.zero :: zeros1(agg.tail)


  def mutate0[V](agg: HNil, state: HNil, v: V): HNil = HNil

  // We mutate the values in the HList rather than creating a new one
  def mutate1[V, S1](agg: Aggregator[S1, V, _] :: HNil, state: S1 :: HNil, v: V): S1 :: HNil = {
    agg.head.mutate(state.head, v)
    mutate0(agg.tail, state.tail, v)
    state
  }

  def merge0[V](agg: HNil, state: HNil, toMergeIn: HNil): HNil = HNil

  // We mutate the values in the HList rather than creating a new one
  def merge1[V, S1](agg: Aggregator[S1, V, _] :: HNil, state: S1 :: HNil, toMergeIn: S1 :: HNil): S1 :: HNil = {
    agg.head.mutateAdd(state.head, toMergeIn.head)
    merge0(agg.tail, state.tail, toMergeIn.tail)
    state
  }

  //
  //
  //
  //  // We loose type safety, maybe would be nice to recover the MultiAggregator type
  //  def toHList[T](l: Seq[T]): HList = if (l.isEmpty) HNil else l.head :: toHList(l.tail)
  ////
  ////  def updateStateLists[V, State](aggregators: HList)(sList: HList)(e: V): HList = {
  ////    def polymorphicMutate[S](state: S, aggregator: Aggregator[S, V, _]): S = aggregator.mutate(state, e)
  ////
  ////    def polymorphicMutateTupled[S](stateAggregator: (S, Aggregator[S, V, _])): S = polymorphicMutate(stateAggregator._1, stateAggregator._2)
  ////
  ////    type SameStateType = (S, Aggregator[S, V, _]) forSome { type S }
  ////
  ////    sList match {
  ////      case state :: HNil => sList
  ////      case state :: otherStates => aggregators match {
  ////        case aggregator :: otherAggregators =>
  ////          polymorphicMutateTupled((state, aggregator).asInstanceOf[SameStateType]) :: updateStateLists(otherAggregators)(otherStates)(e)
  ////      }
  ////
  ////    }
  ////  }
  //
  //  def addStateLists(l: HList, r: HList) = ???
  //
  //  def updateStateLists[V, State, A <: HList, SL <: HList](aggregators: A)(sList: SL)(e: V): HList = {
  //    def polymorphicMutate[S](state: S, aggregator: Aggregator[S, V, _]): S = aggregator.mutate(state, e)
  //
  //    def polymorphicMutateTupled[S](stateAggregator: (S, Aggregator[S, V, _])): S = polymorphicMutate(stateAggregator._1, stateAggregator._2)
  //
  //    type SameStateType = (S, Aggregator[S, V, _]) forSome { type S }
  //
  //    sList match {
  //      case state :: HNil => sList
  //      case state :: otherStates => aggregators match {
  //        case aggregator :: otherAggregators =>
  //          polymorphicMutateTupled((state, aggregator).asInstanceOf[SameStateType]) :: updateStateLists(otherAggregators)(otherStates)(e)
  //      }
  //
  //    }
  //  }
  //
  //  // TODO updateStates, which should modify states in place.
  //
  //
  //  // Only way I can think to make it type safe is copy and pasting to make many
  //  // This will ultimately mean we have to use a number to indicate the size of the HList :(
  //
  //  // Will have to learn how to do macros to make this worth it. Actually, macros look too complicated, just generate strings
  //
  //  // Doesn't seem like I'm getting much benefit to using HLists here, only nice feature is that we can concatenate HLists
  //  // i.e. we don't have to go to the faff of flattening the tuples
  //
  //
  //
  implicit class PimpedPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def aggsByKey1[S1, R1](aggregator: Aggregator[S1, V, R1] :: HNil): RDD[(K, R1 :: HNil)] = {
//      val zeroList: HList = aggregators.map(_.zero).foldRight(HNil: HList)(_ :: _) //.reverse

      val zeros: S1 :: HNil = zeros1(aggregator)

      val updateStates: (S1 :: HNil, V) => S1 :: HNil = mutate1(aggregator, _, _)
      (rdd.combineByKey(updateStates(zeros, _), updateStates, merge1(aggregator, _, _)): RDD[(K, S1 :: HNil)])
      .mapValues(y => aggsToResults1(aggregator, y))
    }


    def aggsByKeyOld(aggregators: Aggregator[_, V, _]*): RDD[(K, HList)] = {
      val zeroList: HList = aggregators.map(_.zero).foldRight(HNil: HList)(_ :: _) //.reverse

      val updateStates: (HList, V) => HList = ???
      val create: V => HList = ???
      val combineStates: (HList, HList) => HList = ???
      rdd.combineByKey(create, updateStates, combineStates)
    }

    // def aggregateNByKey

    //    def aggsByKey2[Aggs](aggregators: Aggregator[_, V, _]*): RDD[(K, HList)] = {
    //      val zeroList: HList = aggregators.map(_.zero).foldRight(HNil: HList)(_ :: _) //.reverse
    //
    //      val create: V => HList = ???
    //      val updateStates: (HList, V) => HList = ???
    //      val combineStates: (HList, HList) => HList = ???
    //      rdd.combineByKey(create, updateStates, combineStates)
    //    }

    //    def aggByKeyResult[R, A <: AggregatorOps[R, V, A] : ClassTag](createAggregator: V => A): RDD[(K, R)] =
    //      aggByKey[R, A](createAggregator).mapValues(_.result)
  }
}

// should have one for value types that doesn't have Unit return type

/** Aggregator useful for Map-Reduce programming.
  *
  * Unit return types is intentional and to highlight that it is not supposed to be used for pure functional
  * programming. This is because we wish to avoid memory allocation (and the consequent GC). */
//trait AggregatorOps[+R, V, A <: AggregatorOps[R, V, A]] {
//  self: A =>
//
//  def update(e: V): Unit
//  def update(a: A): Unit
//  def r: R
//
//  def +(e: V): A = {
//    this.update(e)
//    this
//  }
//
//  def +(a: A): A = {
//    this.update(a)
//    this
//  }
//
//  def update(e: V*): Unit = Seq(e: _*).foreach(update)
//
////  def &[B <: AggregatorOps[_, V, B]](b: B): MultiAggregatorOps[V] = b match {
////    case ma: MultiAggregatorOps[V] => &&[V, A, MultiAggregatorOps[V]](this, ma)
////    case _ => &&[V, A, MultiAggregatorOps[V]](this, &&[V, B, MultiAggregatorOps[V]](b, MultiAggregatorOpsNil[V]()))
////  }
//}

// TODO Properties for unit testing / CDD, e.g. the noArgs constructor


// TODO Replace this with HLists
//trait MultiResult extends Product with Serializable
//
//case object MultiResultNil extends MultiResult
//
//final case class RR[+H, T <: MultiResult](head: H, tail: T) extends MultiResult
//
//sealed trait MultiAggregatorOps[V] extends Product with Serializable with AggregatorOps[MultiResult, V, MultiAggregatorOps[V]] {
//  override def &[B <: AggregatorOps[_, V, B]](b: B): MultiAggregatorOps[V] = this match {
//    case &&(head: B, tail) => &&(head, tail & b)
//  }
//}
//
//case class MultiAggregatorOpsNil[V]() extends MultiAggregatorOps[V] {
//  def update(e: V): Unit = ()
//  def update(a: MultiAggregatorOps[V]): Unit = ()
//  def result: MultiResult = MultiResultNil
//  override def &[B <: AggregatorOps[_, V, B]](b: B): MultiAggregatorOps[V] = &&(b, this)
//}
//
//final case class &&[V, H <: AggregatorOps[_, V, H], T <: MultiAggregatorOps[V]](head: H, tail: T) extends MultiAggregatorOps[V] {
//  def update(e: V): Unit = {
//    head.update(e)
//    tail.update(e)
//  }
//
//  def update(a: MultiAggregatorOps[V]): Unit = a match {
//    case &&(headA: H, tailA: T) =>
//      head.update(headA)
//      tail.update(tailA)
//  }
//
//  def result: MultiResult = RR(head.result, tail.result)
//}
//
//object AggregatorOps {
//  implicit class PimpedPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
//    def aggByKey[R, A <: AggregatorOps[R, V, A]](createAggregator: V => A): RDD[(K, A)] =
//      rdd.combineByKey(createAggregator, _ + _, _ + _)
//
//    def aggByKeyResult[R, A <: AggregatorOps[R, V, A] : ClassTag](createAggregator: V => A): RDD[(K, R)] =
//      aggByKey[R, A](createAggregator).mapValues(_.result)
//  }
//
//  implicit class PimpedPairRDD2[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
//    def aggsByKey[S](aggregators: Aggregator[_, V, S]*): RDD[(K, MultiResult)] = {
//      val zero = aggregators.map(_.zero).reduce(_ & _)
//
//      rdd.combineByKey(createAggregator, _ + _, _ + _)
//    }
//
//
////    def aggByKeyResult[R, A <: AggregatorOps[R, V, A] : ClassTag](createAggregator: V => A): RDD[(K, R)] =
////      aggByKey[R, A](createAggregator).mapValues(_.result)
//  }
//
//  implicit class PimpedRDD[T: ClassTag](rdd: RDD[T]) {
//    def agg[R, A <: AggregatorOps[R, T, A] : ClassTag](createAggregator: T => A): A = rdd.map(createAggregator).reduce(_ + _)
//
//    def aggResult[R, A <: AggregatorOps[R, T, A] : ClassTag](createAggregator: T => A): R =
//      rdd.map(createAggregator).reduce(_ + _).result
//  }
//
//  // Hairiest hack ever to get at the companion (and thus an instance) without having to use type-classes
//  def noArgsInstance[T: ClassTag]: T =
//    Class.forName(implicitly[ClassTag[T]].runtimeClass.getName + "$")
//    .getField("MODULE$").get(null).asInstanceOf[{def apply(): T}].apply()
//
//  implicit class PimpedRDDWithZero[T: ClassTag](rdd: RDD[T]) {
//    def alsoAgg[R, A <: AggregatorOps[R, T, A] : ClassTag](createAggregator: T => A): (() => A, RDD[T]) = {
//      val zeroAgg: A = noArgsInstance[A]
//      val acc = new Accumulator[A](zeroAgg, new AccumulatorParam[A] {
//        def addInPlace(r1: A, r2: A): A = r1 + r2
//        def zero(initialValue: A): A = zeroAgg
//      }, None)
//
//      (() => acc.value,
//        rdd.map(x => {
//          acc += createAggregator(x)
//          x
//        }))
//    }
//
//    // Todo similar to above, but for groupBy??
//  }
//}
