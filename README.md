**1. 프로그램 목적**

  • 고객의 거래금액에 따른 등급산정 BATCH 프로그램 개발
  
  • HDFS의 파일을 읽어 MYSQL DABASE에 INSERT
  
    > HDFS 구성은 로컬 파일을 읽는것으로 대체

- - -
**2. 프로그램 개발 환경**

  • Scala IDE build of Eclipse SDK
  
  • OS : Windows 7

- - -  

**3. 개발 프레임워크**

   • spark (spark-3.0.0-bin-hadoop2.7)
   
   • MYSQL 8.0
   
   • Hadop for winodw(winutils.exe)
   
   • SCALA
   
- - -
**4. 테이블 구성** 

   **• Table: customer_risk_result**
  
            Columns:
            ID decimal(20,0)          #ID
            BASE_YYMM text            #기준년월
            CUSTOMER_ID int           #고객ID
            RISK_SCORE decimal(26,2)  #위험점수
            RISK_GRADE text           #위험등급
            

  ** • Table: grade_base**
  
     Columns:
            ID     varchar(10)    #ID
            GRADE  varchar(2)     #등급
            MIN_SCORE int         #최저점수
            MAX_SCORE int         #최고점수
            






**• Table: tmp1_table  ** (customer와 transaction 의 데이터 중간 집계 테이블)

      Columns:
            ID int          #CustmerID 
            GIJUN_MON text  #기준년월(batch)
            YYYYMM text     #거래년월
            AMT bigint      #거래금액
            
            
            
