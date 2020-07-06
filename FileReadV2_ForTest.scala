package com.ncia.spark

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.log4j.{Level, LogManager, Logger}

import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.Dataset

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import breeze.macros.expand.args
import org.apache.http.util.Args
import org.apache.zookeeper.server.ExitCode



object FileReadV2_ForTest {
//trait FileReadV2 {
   var Errmsg = ""
   
   def main(args: Array[String] ) {

    val log = Logger.getLogger("org").setLevel(Level.FATAL)
    
    init(args)
    
   }

   
   // 파일을 읽어서 테이블에 저장
   def init(args: Array[String]):String= {
  
   
    
    val spark = SparkSession.builder().master("local").appName("ValidationFrameWork").getOrCreate()
    val cst   = spark.read.format("csv")
                .option("header", "true")
                .option("delimiter",",")
                .option("inferSchema","true")
                .load("C:/Kakao/KakaoPay/src/customer.csv")
   
      
    var cstSize = cst.count()
    
    
     
    val trs   = spark.read.format("csv")
                .option("header", "true")
                .option("delimiter",",")
                .option("inferSchema","true")
                .load("C:/Kakao/KakaoPay/src/transaction.csv")  
     
     var trsSize = trs.count()
     
     if(trsSize == 0 || cstSize == 0) {
      Errmsg="customer or transaction File 내용없음"
     } else {
       Errmsg="customer File size : "+cstSize + "   transaction File size :" +trsSize
     }
     
    cst.createOrReplaceTempView("CUSTOMER")
    trs.createOrReplaceTempView("TRANSACTION")
   
    //tmp_table(spark,args)
    
     
   return Errmsg
   }
   
   
   // tmp 테이블생성(TMP1_TABLE)
   def tmp_table(spark:SparkSession,args: Array[String]):String={
     
     val loc_var=args(1)
    //val loc_var = "2019-04"
    var sqlstr =  "          SELECT A.ID                                                  "+
                  "                 , '"+loc_var+"'  AS GIJUN_MON                         "+
                  "                 , SUBSTRING(TRANSACTION_DATE,1,7) AS YYYYMM           "+
                  "                 , SUM(B.AMOUNT) AS AMT                                "+
                  "            FROM CUSTOMER A                                            "+
                  "       INNER JOIN TRANSACTION B                                        "+
                  "             ON A.ID = B.CUSTOMER_ID                                   "+
                  "            AND SUBSTRING(TRANSACTION_DATE,1,7)  BETWEEN  DATE_FORMAT(ADD_MONTHS(TO_DATE('"+loc_var+"-01'),-3),'yyyy-MM') AND '"+loc_var+"'"+  
                  "        GROUP BY A.ID                                                                                                                                                              "+
                  "               , A.NAME                                                 "+
                  "               , SUBSTRING(TRANSACTION_DATE,1,7)                        "        
              
           
     val sqlDF = spark.sql(sqlstr)
     val properties = new Properties()
         properties.put("user", "root")
         properties.put("password", "root")
         
         sqlDF.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://localhost:3306/woodb?characterEncoding=UTF-8&serverTimezone=UTC", "TMP1_TABLE", properties)
     
        if(loc_var == null) {
          Errmsg ="날짜 값이 없습니다."
        } else {
          Errmsg ="날짜 : "+args(1)
        }
        return Errmsg
       //  insert_table(spark)    
     }
   
   // table을 읽어서 저장
   def insert_table(spark:SparkSession):String={
     
     val sql= """     SELECT  ROW_NUMBER() OVER (ORDER BY A.GIJUN_MON)              AS ID                                  
                           ,  A.GIJUN_MON  AS BASE_YYMM                                                          
                           , A.ID          AS CUSTOMER_ID                                                       
                           , A.RESULT      AS RISK_SCORE                                                         
                           , B.GRADE       AS RISK_GRADE                                                        
                       FROM (  SELECT GIJUN_MON                                                                                                                                                           
                                     , ID                                                                                                                                                                             
                                     , SUM(AMT) AMT                                                                                                                                                                  
                                     , ROUND(((AVG(AMT) - MIN(AMT)) / (MAX(AMT) - MIN(AMT))),2)*100 AS RESULT                                                                                                       
                                 FROM TMP1_TABLE                                                                                                                                                                               
                             GROUP BY GIJUN_MON                                                                                                                                                                     
                                     , ID                                                                                      
                             ) A  
                   LEFT JOIN GRADE_BASE B                                                                  
                           ON A.RESULT BETWEEN MIN_SCORE AND MAX_SCORE  """
       //println(sql)
      
      val jdbcDF = spark.read
                  .format("jdbc") 
                  .option("url", "jdbc:mysql://localhost:3306/woodb?characterEncoding=UTF-8&serverTimezone=UTC")
                  .option("dbtable",  s"( $sql ) t")
                  .option("user", "root")
                  .option("password", "root")
                  .load()
         // jdbcDF.show()
          
          jdbcDF.write
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/woodb?characterEncoding=UTF-8&serverTimezone=UTC")
                .option("user", "root")
                .option("password", "root")
                .option("dbtable", "woodb.CUSTOMER_RISK_RESULT")
                .mode("append")           
                .save("CUSTOMER_RISK_RESULT")
                
    
          return jdbcDF.show().toString()
                
    }
   
   // result 테이블 확인
   def show_result(spark:SparkSession){
     
      val result_sql= """ SELECT  * FROM  CUSTOMER_RISK_RESULT """
      
      val jdbcDF = spark.read
                  .format("jdbc") 
                  .option("url", "jdbc:mysql://localhost:3306/woodb?characterEncoding=UTF-8&serverTimezone=UTC")
                  .option("dbtable",  s"( $result_sql ) t")
                  .option("user", "root")
                  .option("password", "root")
                  .load()
          //jdbcDF.show()
   }

}