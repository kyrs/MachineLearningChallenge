import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by vishalkuo on 2016-01-15.
  */
object DataValidation {
  final val merchantIndex = 9
  final val retMerchantIndex = 2

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("Test App").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val transactionsFile = sc.textFile("src/main/resources/datasets/transactions.csv")
    val retCanFile = sc.textFile("src/main/resources/datasets/returnedCancelled.csv")

    val transactionHeader = transactionsFile.first()
    val transactionRows = transactionsFile.filter(line => line != transactionHeader)

    val retCanHeader = retCanFile.first()
    val retCanRows = retCanFile.filter(line => line != retCanHeader)

//    println(transactionRows.count())
//    println(retCanRows.first())

    val transactionFields = transactionRows.map(line => line.split(","))
    val trnsMerchantFieldDistinct = transactionFields.map(lineArr => lineArr(merchantIndex)).distinct().map(x => (x, 1))

    val retFields = retCanRows.map(line => line.split(","))
    val retMerchantFieldDistinct = retFields.map(lineArr => lineArr(retMerchantIndex)).distinct().map(x => (x, 1))

    val merchantsNotIn = trnsMerchantFieldDistinct.rightOuterJoin(retMerchantFieldDistinct).filter(x => x._2._1 == None).count()

    println(trnsMerchantFieldDistinct.count())
    println(retMerchantFieldDistinct.count())
    println(merchantsNotIn)

  }
}
