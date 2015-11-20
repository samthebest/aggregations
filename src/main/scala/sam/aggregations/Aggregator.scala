package sam.aggregations

import breeze.storage.Zero
import org.apache.spark.{AccumulatorParam, Accumulator}
import org.apache.spark.rdd.RDD
import shapeless.{HNil, HList, ::}

import scala.reflect.ClassTag
import scalaz.{Semigroup, Monoid}

// TODO Reconsider design ... consider Aggregator being a type-class
// Then we can supply case objects, furthermore user need not worry about the complex type params
// Finally, can then just HLists for the state and result (getting toTuple for free and stuff)

trait Aggregator[S, V, R] extends Semigroup[S] {
  def mutate(state: S, e: V): S
  def mutate(state: S, e: S): S
  def result(state: S): R
  def zero: S
  
  def append(f1: S, f2: => S): S = mutate(f1, f2)
}

object Aggregator {

  def addStateLists(l: HList, r: HList) = ???

  def updateStateLists[V](sList: HList)(e: V) = ???

  implicit class PimpedPairRDD[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) {
    def aggsByKey(aggregators: Aggregator[_, V, _]*): RDD[(K, HList)] = {
      val zeroList: HList = aggregators.map(_.zero).foldRight(HNil: HList)(_ :: _).reverse

      val create: V => HList = updateStateLists[V](zeroList)
      val updateStates: (HList, V) => HList = ???
      val combineStates: (HList, HList) => HList = ???
      rdd.combineByKey(create, updateStates, combineStates)
    }


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
//  def result: R
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
//  def &[B <: AggregatorOps[_, V, B]](b: B): MultiAggregatorOps[V] = b match {
//    case ma: MultiAggregatorOps[V] => &&[V, A, MultiAggregatorOps[V]](this, ma)
//    case _ => &&[V, A, MultiAggregatorOps[V]](this, &&[V, B, MultiAggregatorOps[V]](b, MultiAggregatorOpsNil[V]()))
//  }
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
