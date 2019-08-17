package com.core

  class Constants(config: Config)  {

       val JDBC_JAR_lOCATION      = config.get("jdbc_location",null)
       val URL                    = config.get("jdbc.url",null)
       val ORACLE_SCHEMA          = config.get("oracle.schema",null)
       val ORACLE_USER            = config.get("oracle.user.name",null)
       val ORACLE_PASSWORD        = config.get("oracle.user.password",null)
       val COLUMNS_TOSELECT       = config.get("oracle.select.columns",null)
       val PRIMARY_KEY            = config.get("primarykey",null)
       val PARTITION_KEY          = config.get("hive.partition.column",null)
       val ORACLE_TABLE           = config.get("oracle.table.name",null)
       val UPDATE_COLUMN          = config.get("oracle.update.column",null)
       val UPDATE_COLUMN_DATATYPE = config.get("oracle.update.column.type",null)
       val DATA_SELECTION_RULE    = config.get("oracle.data.selection.rule",null)
       val OVERWRITE_FLAG         = config.get("hive.overwrite.flag",null)
       val PARTITION_FLAG         = config.get("hive.partition.flag",null)
       val DATE_FILE_LOCATION     = config.get("hdfs.date.file.location",null)
       val HIVE_QUERY             = config.get("hive.query.with.columns.casted")
       val HIVE_TABLE_LOCATION    = config.get("hive.table.location")
       val HIVE_TABLE_FORMAT      = config.get("hive.table.storage.format")
       val HIVE_TABLE_NAME        = config.get("hive.table.name.withschema")
       val HIVE_TEMP_DATABASE     = config.get("hive.temp.db")

}
