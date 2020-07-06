package com.ncia.spark

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}


import java.util.Arrays
import java.util.Scanner
import org.junit.Test


import java.util.Arrays
import java.util.Scanner
import org.junit.Test

import junit.framework.TestCase
import junit.framework.Assert.assertEquals
import junit.framework.Assert.fail

import com.ncia.spark.FileReadV2_ForTest;
import org.apache.log4j.{Level, LogManager, Logger}


object ScalaTest  {
 def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local").appName("ValidationFrameWork").getOrCreate()
    
    val log = Logger.getLogger("org").setLevel(Level.FATAL)
    
  
   if(args(0).equals("init")) {
     val msg= init_test(args)
     println("메시지 : "+msg) 
   }
    
   
   if(args(0).equals("tmp_table")) {
              init_test(args)
     val msg= tmp_table_test(spark,args)
     println("메시지 : "+msg) 
   }
    
   
   if(args(0).equals("insert_table")) {
              init_test(args)
              tmp_table_test(spark,args)
      val msg= insert_table_test(spark)
      println("메시지 : "+msg) 
   }
     
     
   }
 
 
 def init_test(args: Array[String]):String ={
    FileReadV2_ForTest.init(args)
  }
 
 def tmp_table_test(spark:SparkSession,args: Array[String]):String ={
    FileReadV2_ForTest.tmp_table(spark,args)
  }
 
 
 def insert_table_test(spark:SparkSession):String ={
    FileReadV2_ForTest.insert_table(spark)
  }


}