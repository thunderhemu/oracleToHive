package com.hemanth.oracletohive


import com.core.{Config, Constants}
import org.apache.commons.lang3.math.NumberUtils
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object  oracleToHive {

  val log = LogManager.getRootLogger



  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("oracle-To-Hive-Locader"))
    val hiveContext = new HiveContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    if (args.length > 0) {
       val configPath = args(0)
       val Config = new Config
       Config.loadConfig(configPath,sc)
       var Constants = new Constants(Config)

      var oracleRawDf = queryOracle(frameOracleQuerStr(sc,hiveContext,Constants),hiveContext,sc,Constants)
      loadfunc( oracleRawDf,hiveContext,sc,Constants)
  }
    log.info(" Exiting the oracle to hive loader Application")
    sc.stop()
  }

  def fileExists(path:String,sc : SparkContext) : Boolean = {
    val hconf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hconf)
    fs.exists(new Path(path))
  }


  def dataSelecttionStr(queryStr : String,rule : String,referenceValue : String, sc : SparkContext ) : String = {
    var returnStr = queryStr
    rule match {
      case "timestamp" => returnStr += " > to_timestamp('" +referenceValue +"','YYYY-MM-DD HH24:MI:SS.FF')"
      case "int"       => returnStr += " > " +NumberUtils.toInt(referenceValue)
      case "double"    => returnStr += " > " +NumberUtils.toDouble(referenceValue)
      case "string"    => returnStr += " > '" + referenceValue +"'"
      case "day"       => returnStr += " > trunc (to_timestamp('" +referenceValue +"','YYYY-MM-DD HH24:MI:SS.FF')) - 1/24/60/60"
      case "month"     => returnStr += " > trunc (last_day (add_months (to_timestamp('" + referenceValue +"','YYYY-MM-DD HH24:MI:SS.FF') - 1, -1)) + 1) - 1/24/60/60"
      case _           =>  log.error("invalide data selection rule the passed value is ---> " +rule) ; sys.exit(-1)
    }
    returnStr
  }

  def queryOracle(queryStr : String , hiveContext: HiveContext, sc : SparkContext , Constants: Constants) : DataFrame = {

    val oracleRawDf = hiveContext.read.format("jdbc").option("url", Constants.URL).option("dbtable", Constants.ORACLE_SCHEMA
    ).option("user", Constants.ORACLE_USER).option("driver", "oracle.jdbc.OracleDriver").option("password", Constants.ORACLE_PASSWORD
    ).option("dbtable", queryStr).load()
    oracleRawDf

  }

  def frameOracleQuerStr(sc : SparkContext,hiveContext: HiveContext,Constants: Constants): String = {

       var queryStr = "select"

       log.info(" JDBC jar location ----> " + Constants.JDBC_JAR_lOCATION)
       if (fileExists(Constants.JDBC_JAR_lOCATION, sc)) {
            sc.addJar(Constants.JDBC_JAR_lOCATION)
            if( Constants.COLUMNS_TOSELECT==null){
                 queryStr += " * "
             } else {
              queryStr += " " + Constants.COLUMNS_TOSELECT + " "
            }
           if(Constants.ORACLE_SCHEMA == null || Constants.ORACLE_TABLE == null){
             log.error(" Invalid oracle schema or table is input. provided inputs are oracle schema ---> "+Constants.ORACLE_SCHEMA +" oracle table name is ----> "+Constants.ORACLE_TABLE)
             sys.exit(-1)
           }
             queryStr += " from " +Constants.ORACLE_TABLE
           if(Constants.OVERWRITE_FLAG.toLowerCase == "false"){
             log.info(" overwrite flag is set  to false")
             if(Constants.UPDATE_COLUMN!=null){
                queryStr += " where " +Constants.UPDATE_COLUMN
             } else {
               log.error(" update column can'n be null")
             }
             if(fileExists(Constants.DATE_FILE_LOCATION,sc)){
               val lasrReferenceValue = sc.textFile(Constants.DATE_FILE_LOCATION).first()
               queryStr = dataSelecttionStr(queryStr=queryStr,rule=Constants.DATA_SELECTION_RULE,referenceValue=lasrReferenceValue,sc=sc )
               } else {
                log.error(" reference file doesn't exists")
             }
              }

           } // end jars exists
       else {
         log.info("jars doesn't exists")
       }
      queryStr
      //df1.write.partitionBy("datestamp").mode("overwrite").format("orc").save("/apps/hive/warehouse/dwh_stg.db/dw_punter_audit_orc")
    }

  def loadfunc(oracleRawDf : DataFrame , hiveContext: HiveContext,sc : SparkContext,Constants: Constants) : Unit = {

    var finalDf = oracleRawDf
    val colList = oracleRawDf.columns
    for ( i <- colList) {
      finalDf = finalDf.withColumnRenamed(i ,i.toLowerCase())
    }

    finalDf.registerTempTable("temp_table_"+Constants.HIVE_TABLE_NAME.split("""\.""")(1))
    val castedDf = hiveContext.sql(Constants.HIVE_QUERY +" temp_table_"+Constants.HIVE_TABLE_NAME.split("""\.""")(1))
    val tempTable = "temp_"+Constants.HIVE_TABLE_NAME.split("""\.""")(1)
      castedDf.registerTempTable(tempTable)
    if(Constants.PARTITION_FLAG.toLowerCase == "true"){

      val duplicatePartitionsDf = hiveContext.sql(s"""SELECT distinct a.${Constants.PARTITION_KEY} FROM ${Constants.HIVE_TABLE_NAME} a ,
                                                     |                                              ${tempTable} b
                                                     |                 WHERE a.${Constants.PRIMARY_KEY}=b.${Constants.PRIMARY_KEY} """.stripMargin)
      for(i <- duplicatePartitionsDf.collect()) {
          log.info("removing the duplicates from partition ---->  "+i)
        val partitionvalue = i.toString().replaceAll("""\[""","").replaceAll("""\]""","")
        val colStruct = duplicatePartitionsDf.schema
        val coldataType = colStruct.fields(0).dataType.toString.toLowerCase

        val tempHiveTable = Constants.HIVE_TABLE_NAME.toString().split("""\.""")(1) +"_" +Constants.PARTITION_KEY +"_"+partitionvalue.replaceAll("-","_")
        var whereStr = ""
        var insertwhereStr = ""
        val primaryKeyArray = Constants.PRIMARY_KEY.split(",")
        for (key  <- primaryKeyArray) {
          whereStr +=" t1." +key.trim() +" = t2." +key.trim +" and "
          insertwhereStr += "t2."+key.trim +"  is null and "
        }
        val finalWhereStr = whereStr.substring(0,whereStr.length-4)
        log.info("finale where str ----> "+finalWhereStr)
        val finaleJoinWhereStr =  insertwhereStr.substring(0,insertwhereStr.length -4)
        log.info("finale where join str ----> "+finaleJoinWhereStr)

        hiveContext.sql("USE "+Constants.HIVE_TEMP_DATABASE)
        hiveContext.sql("set hive.exec.dynamic.partition=true")
        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        hiveContext.setConf("spark.sql.hive.convertMetastoreOrc", "false")

        if( coldataType.contains("int")     ||
            coldataType.contains("double")  ||
            coldataType.contains("decimal") ||
            coldataType.contains("long")    ||
            coldataType.contains("bool")     ){
            // creating temp hive table with the partition that has old data to be removed
            hiveContext.sql(s""" CREATE TABLE ${tempHiveTable} AS
                               |          SELECT * FROM ${Constants.HIVE_TABLE_NAME} WHERE ${Constants.PARTITION_KEY}=$partitionvalue""".stripMargin)

            hiveContext.sql(s"""INSERT OVERWRITE TABLE ${Constants.HIVE_TABLE_NAME} PARTITION(${Constants.PARTITION_KEY}=$partitionvalue)
                               |                                           SELECT t1.* FROM ${tempHiveTable} t1
                               |                                                       LEFT OUTER JOIN ${tempTable} t2
                               |                                                       ON  $finalWhereStr
                               |                                                       WHERE $finaleJoinWhereStr    """.stripMargin)

            hiveContext.sql("DROP TABLE "+tempHiveTable)

        } // end if loop coldataType validation
        else {


          hiveContext.sql(s""" CREATE TABLE ${tempHiveTable} AS
                             |                    SELECT * FROM ${Constants.HIVE_TABLE_NAME} WHERE ${Constants.PARTITION_KEY}='$partitionvalue'""".stripMargin)

          hiveContext.sql(s"""INSERT OVERWRITE TABLE ${Constants.HIVE_TABLE_NAME} PARTITION(${Constants.PARTITION_KEY}='$partitionvalue')
                             |                                                SELECT t1.* FROM ${tempHiveTable} t1
                             |                                                            LEFT OUTER JOIN ${tempTable} t2
                             |                                                            ON  $finalWhereStr
                             |                                                            WHERE $finaleJoinWhereStr  """.stripMargin)

          hiveContext.sql("DROP TABLE "+tempHiveTable)
        }
      }// end for loop
       hiveContext.refreshTable(Constants.HIVE_TABLE_NAME)
      Constants.OVERWRITE_FLAG.toLowerCase match {
          case "false" => castedDf.write.partitionBy(Constants.PARTITION_KEY).mode(SaveMode.Append).format(Constants.HIVE_TABLE_FORMAT).insertInto(Constants.HIVE_TABLE_NAME)
          case "true"  => castedDf.write.partitionBy(Constants.PARTITION_KEY).mode(SaveMode.Overwrite).format(Constants.HIVE_TABLE_FORMAT).insertInto(Constants.HIVE_TABLE_NAME)
          case _       => log.error("invalid overwrite falg is set. provided input is  "+Constants.OVERWRITE_FLAG)
      }

    }// end if partition flag
    else {
       Constants.OVERWRITE_FLAG.toLowerCase match {
          case "false" => {   //castedDf.registerTempTable("temp_casted_table")
                             // hiveContext.sql(s""" insert into table ${Constants.HIVE_TABLE_NAME} select * from temp_casted_table""")
                              castedDf.write.mode(SaveMode.Append).format(Constants.HIVE_TABLE_FORMAT).insertInto(Constants.HIVE_TABLE_NAME)
                              hiveContext.refreshTable(Constants.HIVE_TABLE_NAME)
                              var colList = castedDf.columns
                              var columnsStr = ""
                              for (col <- colList) {
                                columnsStr += col + ","
                              }
                              log.info("\n\n\n\n\n col list is \n\n\n\n -> "+columnsStr)
                              columnsStr = columnsStr.substring(0,columnsStr.length-1)
                              castedDf.show(10)
                              castedDf.registerTempTable("temp_casted_table")
                              log.info("\n\n\n\n\n\n\n\n queried data is  \n\n\n\n\n\n\n" +hiveContext.sql("select * from temp_casted_table").show(100))

                             // hiveContext.sql(s""" insert into table ${Constants.HIVE_TABLE_NAME} select * from temp_casted_table""")
                             // castedDf.write.mode(SaveMode.Append).format(Constants.HIVE_TABLE_FORMAT).insertInto(Constants.HIVE_TABLE_NAME)
                              hiveContext.sql("select * from "+Constants.HIVE_TABLE_NAME).show(100)
                              hiveContext.sql(s"""INSERT OVERWRITE TABLE ${Constants.HIVE_TABLE_NAME} SELECT ${columnsStr} FROM (
                                   |                SELECT $columnsStr ,MAX(${Constants.UPDATE_COLUMN}) OVER (PARTITION BY ${Constants.PARTITION_KEY}) AS max_updated_value
                                   |                FROM ${Constants.HIVE_TABLE_NAME} t ) a where a.${Constants.UPDATE_COLUMN} = a.max_updated_value """.stripMargin)

                              }
          case "true"  => castedDf.write.mode(SaveMode.Overwrite).insertInto(Constants.HIVE_TABLE_NAME)
          case _       => log.error("invalid overwrite falg is set. provided input is  "+Constants.OVERWRITE_FLAG)
       }
    }

    hiveContext.sql("msck repair table "+Constants.HIVE_TABLE_NAME)
  }// end load func
}
