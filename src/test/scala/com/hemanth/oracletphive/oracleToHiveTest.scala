package com.hemanth.oracletphi

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.core.{Config, Constants}
import com.hemanth.oracletohive.oracleToHive.{frameOracleQuerStr, _}
import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}


class oracleToHiveTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers with Logging with Eventually {

  val envMap = Map[String, String](("Xmx", "512m"))
  val sparkConf = new SparkConf()
  sparkConf.set("spark.broadcast.compress", "false")
  sparkConf.set("spark.shuffle.compress", "false")
  sparkConf.set("spark.shuffle.spill.compress", "false")
  sparkConf.set("spark.io.compression.codec", "lzf")
  val sc = new SparkContext("local[*]", "unit test", sparkConf)
  val  hiveContext = new TestHiveContext(sc)

  override def beforeAll(): Unit = {
    super.beforeAll()
  }



  override def afterAll() = {
     sc.stop
  }
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  test ("queryOracle") {

    val Config = new Config
    Config.loadConfig("src/test/resources/test1.config", sc)
    val constants = new Constants(Config)
    println(" output is ---> "+frameOracleQuerStr(sc,hiveContext,constants))

  }
  test("partitioned table append") {

    val Config = new Config
    Config.loadConfig("src/test/resources/test1.config", sc)
    val Constants = new Constants(Config)
    val rawRdd  = sc.textFile("src/test/resources/vip.txt").map(_.split(",")).map(x=> ( x(0),NumberUtils.toInt(x(1)),x(2),x(3),x(4),x(5),x(6)))
    import hiveContext.implicits._
    val rawDf = rawRdd.toDF("usrname","id","brand","viplevel","vipstartdate","vipstatus","datestamp")
    hiveContext.sql("SET hive.support.sql11.reserved.keywords=false;")
    hiveContext.sql("create database IF NOT EXISTS dwh_stg")
    hiveContext.sql("use dwh_stg")
    hiveContext.sql("Drop table if exists tb1")

    val file = new File("src/test/resources/tb1")
    var path = file.getAbsolutePath

    hiveContext.sql(s"""  create EXTERNAL table IF NOT EXISTS  tb1 ( usrname String,id int,brand String,viplevel String,vipstartdate String,vipstatus String )
                            PARTITIONED BY (datestamp String)  stored as orc location '$path' """.stripMargin)

    hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    hiveContext.sql("set hive.exec.dynamic.partition.mode=true")
    rawDf.registerTempTable("temp_table1")
    hiveContext.sql(" insert overwrite table dwh_stg.tb1 partition(datestamp) select * from temp_table1")
    val finalDf = hiveContext.sql("select * from tb1 limit 3").select("usrname","id","brand","viplevel","vipstatus").withColumn("vipstartdate",lit(today("yyyy-MM-dd HH:mm:ss")))
    finalDf.cache()
    loadfunc(finalDf,hiveContext,sc,Constants)
    hiveContext.refreshTable("tb1")
    hiveContext.sql("msck repair table tb1")
    val totalCount = hiveContext.sql("select count(*) from tb1").first().getLong(0)
    val distinctCount = hiveContext.sql("select count(distinct id) from tb1").first().getLong(0)
    val df1 = hiveContext.sql("select * from tb1").join(finalDf,Seq("id","vipstartdate"))
    assert(df1.count()== finalDf.count())
    assert(totalCount == distinctCount)
    val warehouserDir = new File("src/test/resources/tb1/")
    warehouserDir.listFiles().foreach(f => deleteRecursively(f))
    //sc.stop()
    warehouserDir.delete()
  }

  test("non partitioned table overwrite") {

   val config = new Config

   config.loadConfig("src/test/resources/test2.config", sc)
   val Constants = new Constants(config)

    val rawRdd  = sc.textFile("src/test/resources/vip.txt").map(_.split(",")).map(x=> ( x(0),NumberUtils.toInt(x(1)),x(2),x(3),x(4),x(5),x(6)))
    import hiveContext.implicits._
    val rawDf = rawRdd.toDF("usrname","id","brand","viplevel","vipstartdate","vipstatus","datestamp")
    hiveContext.sql("SET hive.support.sql11.reserved.keywords=false;")
    hiveContext.sql("create database IF NOT EXISTS dwh_stg")
    hiveContext.sql("use dwh_stg")
    hiveContext.sql("Drop table if exists tb2")

    val file = new File("src/test/resources/tb2")
    var path = file.getAbsolutePath

    hiveContext.sql(
      s"""create EXTERNAL table IF NOT EXISTS  tb2 ( usrname String,
         |            id int,brand String,viplevel String,vipstartdate String,
         |            vipstatus String ,datestamp String)  stored as orc location '$path' """.stripMargin)

    rawDf.registerTempTable("temp_table2")
    hiveContext.sql(" insert overwrite table dwh_stg.tb2 select * from temp_table2")
    val finalDf = hiveContext.sql("select * from tb2 limit 3").select("usrname","id","brand","viplevel","vipstatus").withColumn("vipstartdate",lit(today("yyyy-MM-dd HH:mm:ss")))
    finalDf.cache()
    loadfunc(finalDf,hiveContext,sc,Constants)
    hiveContext.refreshTable("tb2")
    hiveContext.sql("msck repair table tb2")
    val totalCount = hiveContext.sql("select count(*) from tb2").first().getLong(0)
    val distinctCount = hiveContext.sql("select count(distinct id) from tb2").first().getLong(0)
    val df1 = hiveContext.sql("select * from tb2").join(finalDf,Seq("id","vipstartdate"))
    assert(df1.count()== finalDf.count())
    assert(totalCount == distinctCount)
    val warehouserDir = new File("src/test/resources/tb2/")
    warehouserDir.listFiles().foreach(f => deleteRecursively(f))
    warehouserDir.delete()
  }

  test("non partitioned table append") {

    val config = new Config

    config.loadConfig("src/test/resources/tes3.config", sc)
    val Constants = new Constants(config)

    val rawRdd  = sc.textFile("src/test/resources/vip.txt").map(_.split(",")).map(x=> ( x(0),NumberUtils.toInt(x(1)),x(2),x(3),x(4),x(5),x(6)))
    import hiveContext.implicits._
    val rawDf = rawRdd.toDF("usrname","id","brand","viplevel","vipstartdate","vipstatus","datestamp")
    hiveContext.sql("SET hive.support.sql11.reserved.keywords=false;")
    hiveContext.sql("create database IF NOT EXISTS dwh_stg")
    hiveContext.sql("use dwh_stg")
    hiveContext.sql("Drop table if exists tb3")

    val file = new File("src/test/resources/tb3")
    var path = file.getAbsolutePath

    hiveContext.sql(s"""create EXTERNAL table IF NOT EXISTS  tb3 ( usrname String,
         |            id int,brand String,viplevel String,vipstartdate String,
         |            vipstatus String ,datestamp String)  stored as orc location '$path' """.stripMargin)

    rawDf.registerTempTable("temp_table3")
    hiveContext.sql(" insert overwrite table dwh_stg.tb3 select * from temp_table3")
    val finalDf = hiveContext.sql("select * from tb3 limit 3").select("usrname","id","brand","viplevel","vipstatus").withColumn("vipstartdate",lit(today("yyyy-MM-dd HH:mm:ss")))
    finalDf.cache()
    loadfunc(finalDf,hiveContext,sc,Constants)
    hiveContext.refreshTable("tb3")
    hiveContext.sql("msck repair table tb3")
    hiveContext.sql("select * from tb3").show()
    val totalCount = hiveContext.sql("select count(*) from tb3").first().getLong(0)
    val distinctCount = hiveContext.sql("select count(distinct id) from tb3").first().getLong(0)
    val df1 = hiveContext.sql("select * from tb3").join(finalDf,Seq("id","vipstartdate"))
    //assert(df1.count()== finalDf.count())
    //assert(totalCount == distinctCount)
    val warehouserDir = new File("src/test/resources/tb3/")
    //warehouserDir.listFiles().foreach(f => deleteRecursively(f))
    //warehouserDir.delete()
  }

  def today(dateFmt : String): String = {
    val date = new Date
    val sdf = new SimpleDateFormat(dateFmt)
    sdf.format(date)
  }

}
