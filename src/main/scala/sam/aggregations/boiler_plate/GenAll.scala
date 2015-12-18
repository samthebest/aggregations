package sam.aggregations.boiler_plate

object GenAll {
  def main(args: Array[String]): Unit = {
    val num = args.headOption.map(_.toInt).getOrElse(22)
    List(
//      AggToResultsCode(num),
      MergeCode(num),
      MutateCode(num),
//      ZerosCode(num),
      CopyCode(num)
    )
    .map(_ + "\n")
    .foreach(println)
  }



}
