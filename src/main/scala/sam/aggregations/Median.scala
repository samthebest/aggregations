package sam.aggregations

trait Median {
  def update(e: Long): Unit
  def update(m: Median): Unit
  def result: Double
}
