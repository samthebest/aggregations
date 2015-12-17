package sam.aggregations

object CodeGeneration {
  // TODO Get a PHd in Scala Macros so I can work out how to do really simple stuff like this using macros
  def aggsToResultsGenerator(num: Int): String =
    (1 to num).map(i => {
      val typeParms = (1 to i).map(j => s"R$j, S$j").mkString(", ")
      val aggType = (1 to i).map(j => s"Aggregator[S$j, _, R$j]").mkString(" :: ")
      val statesType = (1 to i).map(j => s"S$j").mkString(" :: ")
      val returnType = (1 to i).map(j => s"R$j").mkString(" :: ")
      s"def aggsToResults$i[$typeParms](agg: $aggType :: HNil, states: $statesType :: HNil): $returnType :: HNil = " +
        s"agg.head.result(states.head) :: aggsToResults${i - 1}(agg.tail, states.tail)"
    }).mkString("\n")
}
