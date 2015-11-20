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

  // We loose type safety, maybe would be nice to recover the MultiAggregator type
  def toHList[T](l: Seq[T]): HList = if (l.isEmpty) HNil else l.head :: toHList(l.tail)
//
//  def updateStateLists[V, State](aggregators: HList)(sList: HList)(e: V): HList = {
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

  def addStateLists(l: HList, r: HList) = ???

  def updateStateLists[V, State, A <: HList, SL <: HList](aggregators: A)(sList: SL)(e: V): HList = {
    def polymorphicMutate[S](state: S, aggregator: Aggregator[S, V, _]): S = aggregator.mutate(state, e)

    def polymorphicMutateTupled[S](stateAggregator: (S, Aggregator[S, V, _])): S = polymorphicMutate(stateAggregator._1, stateAggregator._2)

    type SameStateType = (S, Aggregator[S, V, _]) forSome { type S }

    sList match {
      case state :: HNil => sList
      case state :: otherStates => aggregators match {
        case aggregator :: otherAggregators =>
          polymorphicMutateTupled((state, aggregator).asInstanceOf[SameStateType]) :: updateStateLists(otherAggregators)(otherStates)(e)
      }

    }
  }

  // Only way I can think to make it type safe is copy and pasting to make many
  // This will ultimately mean we have to use a number to indicate the size of the HList :(

  // Will have to learn how to do macros to make this worth it. Actually, macros look too complicated, just generate strings

  def aggsToResultsGenerator(num: Int): String = {
    (1 to num).map(i => {
      val typeParms = (1 to i).map(j => s"R$j, S$j").mkString(", ")
      val aggType = (1 to i).map(j => s"Aggregator[S$j, _, R$j]").mkString(" :: ")
      val statesType = (1 to i).map(j => s"S$j").mkString(" :: ")
      val returnType = (1 to i).map(j => s"R$j").mkString(" :: ")
      s"def aggsToResults$i[$typeParms](agg: $aggType :: HNil, states: $statesType :: HNil): $returnType :: HNil = " +
        s"agg.head.result(states.head) :: aggsToResults${i - 1}(agg.tail, states.tail)"
    }).mkString("\n")
  }

  // Doesn't seem like I'm getting much benefit to using HLists here, only nice feature is that we can concatenate HLists
  // i.e. we don't have to go to the faff of flattening the tuples

  def aggsToResults0[R1, S1](agg: HNil, state: HNil): HNil = HNil

  def aggsToResults1[R1, S1](agg: Aggregator[S1, _, R1] :: HNil, states: S1 :: HNil): R1 :: HNil = agg.head.result(states.head) :: aggsToResults0(agg.tail, states.tail)
  def aggsToResults2[R1, S1, R2, S2](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: HNil, states: S1 :: S2 :: HNil): R1 :: R2 :: HNil = agg.head.result(states.head) :: aggsToResults1(agg.tail, states.tail)
  def aggsToResults3[R1, S1, R2, S2, R3, S3](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: HNil, states: S1 :: S2 :: S3 :: HNil): R1 :: R2 :: R3 :: HNil = agg.head.result(states.head) :: aggsToResults2(agg.tail, states.tail)
  def aggsToResults4[R1, S1, R2, S2, R3, S3, R4, S4](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: Aggregator[S4, _, R4] :: HNil, states: S1 :: S2 :: S3 :: S4 :: HNil): R1 :: R2 :: R3 :: R4 :: HNil = agg.head.result(states.head) :: aggsToResults3(agg.tail, states.tail)
  def aggsToResults5[R1, S1, R2, S2, R3, S3, R4, S4, R5, S5](agg: Aggregator[S1, _, R1] :: Aggregator[S2, _, R2] :: Aggregator[S3, _, R3] :: Aggregator[S4, _, R4] :: Aggregator[S5, _, R5] :: HNil, states: S1 :: S2 :: S3 :: S4 :: S5 :: HNil): R1 :: R2 :: R3 :: R4 :: R5 :: HNil = agg.head.result(states.head) :: aggsToResults4(agg.tail, states.tail)

  implicit class PimpedPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def aggsByKey(aggregators: Aggregator[_, V, _]*): RDD[(K, HList)] = {
      val zeroList: HList = aggregators.map(_.zero).foldRight(HNil: HList)(_ :: _) //.reverse

      val create: V => HList = ???
      val updateStates: (HList, V) => HList = ???
      val combineStates: (HList, HList) => HList = ???
      rdd.combineByKey(create, updateStates, combineStates)
    }
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
