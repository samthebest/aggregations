package sam.aggregations

import scala.collection.mutable

object TypeAliases {
  type Long2 = (Long, Long)
  type MergeStrategy = (mutable.Map[Long2, Long], Int) => mutable.Map[Long2, Long]
}
