/**
  * Created by hiren on 11/15/2015.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{FloatType, DateType}
import org.apache.spark.sql.functions._

object sql {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    //mysql connection
    val databaseUsername = "hiren"
    val databasePassword = "hiren"
    val databaseConnectionUrl = "jdbc:mysql://127.0.0.1:3306/sakila?user=" + databaseUsername + "&password=" + databasePassword

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    // Create a SQLContext (sc is an existing SparkContext)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val tweets = sc.textFile("C:\\Users\\hiren\\Desktop\\Intolerance.txt")

    val tweets1 = sqlContext.jsonFile("C:\\Users\\hiren\\Desktop\\Intolerance.txt")
    // Register the created SchemaRDD as a temporary table.
    tweets1.registerTempTable("tweets1")
    //  tweets1.printSchema()
    sqlContext.cacheTable("tweets1") //saved to cache for improving perfomance


    val totaltweets =  sqlContext.sql("SELECT text FROM tweets1 WHERE text <> '' ").map(r => r.getString(0))
    val allcount = totaltweets.count()


    //Query-1// Tweets per language
    val lang = sqlContext.sql("SELECT user.lang, COUNT(text) cnt FROM tweets1 GROUP BY user.lang ORDER BY cnt DESC limit 10")//.collect.foreach(println) //tweets per language
    lang.insertIntoJDBC(databaseConnectionUrl, "languagetweets", true) //result stored to mysql
    println("Query-1 output saved to mysql")

    //         //Query-2 //Tweets percentage by language
    //val dtFunc: (Int => Float) = (arg1: Int) => (arg1 * 100)/totaltweets
    val x = lang.withColumn("share",(lang("cnt") * 100)/allcount) //adding new column using existing column to count percentage
    x.registerTempTable("x")
    val output1 = sqlContext.sql("SELECT * FROM x ORDER BY cnt DESC")
    val selecteddata = output1.select("lang","share")
    selecteddata.insertIntoJDBC(databaseConnectionUrl, "lang", true)  //stored result data to my sql
    println("Query-2 output saved to mysql")


    //   // Query - 3 //Most Famous personality
    //             val query3 = sqlContext.sql("""SELECT t.retweeted_Screen,t.tz, sum(t.retweets) AS total_retweets,count(t.text) AS tweet_count
    //    	                                       FROM (SELECT
    //                                                   retweeted_status.user.screen_name as retweeted_Screen,
    //                                                   retweeted_status.user.time_zone as tz,
    //                                                   retweeted_status.text as text,
    //                                                   max(retweeted_status.retweet_count) as retweets
    //                                                   FROM tweets1 WHERE text <> ''
    //                                                   GROUP BY retweeted_status.user.screen_name,retweeted_status.user.time_zone,retweeted_status.text
    //    	                                             ) t
    //                                            GROUP BY t.retweeted_Screen, t.tz
    //                     	                      ORDER BY total_retweets DESC LIMIT 10 """)//.collect.foreach(println)
    //
    //        val result = query3.select(query3("retweeted_Screen"),query3("tz"),query3("total_retweets"),query3("tweet_count"))
    //
    //        result.insertIntoJDBC(databaseConnectionUrl, "famoustweets", true)


    // Query -4// Get top 20 hash tags

            val Words = tweets.flatMap(line => line.split(" "))
            val hashtags = Words.filter(word => word.startsWith("#"))
            val count = hashtags.map(tag => (tag,1))
                        .reduceByKey(_ + _)

    //   //Query 5//
    val sort = count.map(tuple => (tuple._2, tuple._1))
      .sortByKey(false).top(10)
    sort.foreach(println)

    // query - 6 // get tweets per HOUR PER DAY

    //        val time = sqlContext.sql(""" SELECT dt, count(text) as cntdt FROM
    //                                  (
    //                                    SELECT
    //                                    text,
    //                                    SUBSTRING(created_at,0,13) AS dt
    //                                    FROM tweets1
    //                                   )R
    //                                  GROUP BY dt
    //                                  ORDER BY dt """)//.collect.foreach(println)
    //
    //       time.insertIntoJDBC(databaseConnectionUrl, "tweetperhour", true)
    //       println("Query-6 output saved to mysql")


    //query 7
     val map = sqlContext.sql("SELECT distinct cast(geo.coordinates as string), place.country  FROM tweets1")//.collect.foreach(println)
    map.na.fill(0)
     map.insertIntoJDBC(databaseConnectionUrl, "map", true)
    println("Query-7 output saved to mysql")

    //query 8
        val at = sqlContext.sql("SELECT text  FROM tweets1 where text like '%AamirKhan%' OR text like '%AmirKhan%' OR text like '%@aamir_khan%' ")
        at.registerTempTable("Aamir")
        sqlContext.cacheTable("Aamir")

        val aamir = sqlContext.sql("SELECT count(text) cnt  FROM Aamir ")
        val coder: (Long => String) = (arg: Long) => {if (arg < 100) "Total Count" else "Total Count"}
        val sqlfunc = udf(coder)
        val res = aamir.withColumn("Text", sqlfunc(col("cnt")))
        res.insertIntoJDBC(databaseConnectionUrl, "intolerance", true)


        val snapdeal = sqlContext.sql("SELECT count(text) cnt FROM Aamir WHERE text LIKE '%#BootOutSnapdeal%' ")
        val coder1: (Long => String) = (arg: Long) => {if (arg < 100) "snapdeal Count" else "snapdeal Count"}
        val sqlfunc1 = udf(coder1)
        val snp = snapdeal.withColumn("Text", sqlfunc1(col("cnt")))
        snp.insertIntoJDBC(databaseConnectionUrl, "intolerance", false)

        val intolerance = sqlContext.sql("SELECT count(text) cnt FROM Aamir WHERE text LIKE '%#Intolerance%' OR text like '%#intolerance%' ")
        val coder2: (Long => String) = (arg: Long) => {if (arg < 100) "intolrence Count" else "intolrence Count"}
        val sqlfunc2 = udf(coder2)
        val intol = intolerance.withColumn("Text", sqlfunc2(col("cnt")))
        intol.insertIntoJDBC(databaseConnectionUrl, "intolerance", false)
    //
        val stand = sqlContext.sql("SELECT count(text) cnt FROM Aamir WHERE text LIKE '%#IStandWithAamirKhan%' ")
        val coder3: (Long => String) = (arg: Long) => {if (arg < 100) "support Count" else "support Count"}
        val sqlfunc3 = udf(coder3)
        val support = stand.withColumn("Text", sqlfunc3(col("cnt")))
        support.insertIntoJDBC(databaseConnectionUrl, "intolerance", false)

    //println("Query-8 output saved to mysql")

  }
}