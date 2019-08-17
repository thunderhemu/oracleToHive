
Purpose : To bring the data from oracle to hive, it bring the continuous data and full load. it checks for if there any update
         if there any updates it keeps the data in sync with rdbms


     jdbc_location   ---> jdbc jar location
     jdbc.url   --> jdbc url
     oracle.schema  --> schema name
     oracle.user.name --> user name
     oracle.user.password --> password
     oracle.select.columns  --> [optional] columns to select , leave it to null if needed to bring all the columns
     primarykey --> primary key[optional]
     hive.partition.column --> [optional] name of column on which table needs to be partitioned
     oracle.table.name  --> oracle table name
     oracle.update.column  --> [optional] for continuous load provide oracle update column
     oracle.update.column.type --> [optional] for continuous load provide oracle update column data type
     oracle.data.selection.rule  --> data selection rule from oracle
     hive.overwrite.flag --> true/false true--> to overwrite , false --> to append
     hive.partition.flag --> true/false true--> to partition , false --> to not partition
     hdfs.date.file.location --> delta file location to store delta value for continuous load
     hive.query.with.columns.casted --> if any columns needs to casted provide query
     hive.table.location --> hive table location
     hive.table.storage.format  --> hive table storage format
     hive.table.name.withschema --> hive table name with db
     hive.temp.db --> provide a tem db to create temp table to handle update load