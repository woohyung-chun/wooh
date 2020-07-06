**1. 프로그램 목적**

  • 고객의 거래금액에 따른 등급산정 BATCH 프로그램 개발
  • HDFS의 파일을 읽어 MYSQL DABASE에 INSERT
    > HDFS 구성은 로컬 파일을 읽는것으로 대체

- - -
**2. 프로그램 개발 환경**

  • Scala IDE build of Eclipse SDK
  • OS : Windows 7

- - -  
**3. 개발 프레임워크 **

   • spark (spark-3.0.0-bin-hadoop2.7)
   • MYSQL 8.0
   • Hadop for winodw(winutils.exe)
   • SCALA
     
**4. 테이블 구성 ** 

   • Table: customer_risk_result
            Columns:
            ID decimal(20,0) 
            BASE_YYMM text 
            CUSTOMER_ID int 
            RISK_SCORE decimal(26,2) 
            RISK_GRADE text
            

   • Table: grade_base
     Columns:
            ID varchar(10) 
            GRADE varchar(2) 
            MIN_SCORE int 
            MAX_SCORE int


    • Table: tmp1_table
      Columns:
            ID int 
            GIJUN_MON text 
            YYYYMM text 
            AMT bigint
