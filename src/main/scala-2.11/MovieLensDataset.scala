import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by vishalkuo on 2016-01-15.
  *
  * NOTE: This dataset can be found at:
  * www.files.grouplens.org/datasets/movielens/ml-100k.zip
  */


object MovieLensDataset {
  def main (args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[2]").setAppName("Test App").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val userData = sc.textFile("src/main/resources/datasets/ml-100k/u.user")
    val userFields = userData.map(line => line.split("|"))
    val numOfUsers = userData.count()
    val numOfGenders = userData.map(fields => fields(2)).distinct().count()
    println(s"Num of users: $numOfUsers")
    println(numOfGenders)
  }
}
