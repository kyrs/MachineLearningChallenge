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

//    processUsers(sc)

    processMovieDataset(sc)
  }

  def processMovieDataset(sc: SparkContext): Unit ={
    val movieData = sc.textFile("src/main/resources/datasets/ml-100k/u.item")
    val movieFields = movieData.map(lines => lines.split("\\|"))
    val years = movieFields.map(fields => fields(2)).map({
      x => try{
        x.substring(x.length - 4).toInt
      } catch {
        case _: NumberFormatException | _: StringIndexOutOfBoundsException => 1900
      }
    })
    val yearsFiltered = years.filter(x => x != 1900)

    val ageAggregated = yearsFiltered.map(x => 1998 - x).countByValue().toArray.sortBy(_._1)
    for (age <- ageAggregated){
      println(age)
    }
  }

  def processUsers(sc: SparkContext): Unit ={
    val userData = sc.textFile("src/main/resources/datasets/ml-100k/u.user")
    val userFields = userData.map(line => line.split("\\|"))
    val numOfUsers = userFields.count()
    val numOfGenders = userFields.map(fields => fields(2)).distinct().count()
    val numOfOccupations = userFields.map(fields => fields(3)).distinct().count()
    val numOfZipcodes = userFields.map(fields => fields(4)).distinct().count()

    println(s"Num of users: $numOfUsers")
    println(s"Num of genders: $numOfGenders")
    println(s"Num of occuptations: $numOfOccupations")
    println(s"Num of zipcodes: $numOfZipcodes")

    val allAges = userFields.map(fields => fields(1).toInt).collect()
    val ageArrangement = userFields.map({
      fieldArr => (fieldArr(1), 1)
    }).reduceByKey((a, b) => a + b).sortBy(_._2).collect()

    val occupationCount = userFields.map({
      fieldArr => (fieldArr(3), 1)
    }).reduceByKey(_ + _).sortBy(_._2).collect()

    //    for (occupation <- occupationCount){
    //      println(occupation)
    //    }

    //This and previous occupation count will be the exact same, except for sort order
    val occupationCountByValue = userFields.map(fieldArr => fieldArr(3)).countByValue()

    //    for (occupation <- occupationCountByValue){
    //      println(occupation)
    //    }
  }
}
