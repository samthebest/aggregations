package sam.aggregations

trait Median[T <: Median[T]] {
  def update(e: Long): Unit
  def update(m: T): Unit
  def result: Double
}
