import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by vishalkuo on 2016-01-14.
  */
object TestSpark {
  def main(args: Array[String]): Unit = {
    //
    //    val data = sc.textFile(url.getPath(), 10).cache()
    //    val url = getClass.getResource("/datasets/profitMetrics.csv")

    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("Test App").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val data = sc.textFile("src/main/resources/datasets/SparkLearnSet.csv")
      .map(line => line.split(",")).map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))
    data.cache()

    val numPurchases = data.count()

    val uniqueUsers = data.map({
      case(user, product, price) => user
    }).distinct().count()

    val totalRevenue = data.map({
      case(user, product, price) => price.toDouble
    }).fold(0)((acc, i) => acc + i)

    val productByPopularity = data.map({
      case (user, product, price) => (product, 1)
    }).reduceByKey(_ + _)
    .collect()
    .sortWith((a, b) => a._2 > b._2 )

    val mostPopular = productByPopularity(0)

    println("num of purchases: " + numPurchases)
    println("unique users: " + uniqueUsers)
    println("total revenue: " + totalRevenue)
    println("Most popular: " + mostPopular)
  }
}
