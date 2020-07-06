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
            

  **• Table: grade_base**
  
     Columns:
            ID     varchar(10)    #ID
            GRADE  varchar(2)     #등급
            MIN_SCORE int         #최저점수
            MAX_SCORE int         #최고점수
            






  **• Table: tmp1_table** (customer와 transaction 의 데이터 중간 집계 테이블)

      Columns:
            ID int          #CustmerID 
            GIJUN_MON text  #기준년월(batch)
            YYYYMM text     #거래년월
            AMT bigint      #거래금액
            
            
- - -


**5. 빌드 및 실행** 
    command 에서 아래과 같이 실행
    • C:\Kakao\KakaoPay\bin\Kpay.bat [yyyy-mm]
     eg> yyyy-mm= 2019-03

**6. 프로그램 설명** 

   •  main(args: Array[String])
   
     - 로그 Level 설정 및 프로그램 시작 
   
   •  init(args: Array[String])
   
      - customer.csv transaction.csv 파일 load
      - 각 load 된 파일로  temp  View 생성

   •  tmp_table(spark:SparkSession,args: Array[String])
   
      - init에서 생성된 cusstomer , transaction View를  활용하여 MySQL 임시테이블(TMP1_TABLE)    에 Insert
      - 아래 SQL
        SELECT A.ID
           , 'loc_var+"'  AS GIJUN_MON
           , SUBSTRING(TRANSACTION_DATE,1,7) AS YYYYMM
           , SUM(B.AMOUNT) AS AMT
          FROM CUSTOMER A
     INNER JOIN TRANSACTION B
           ON A.ID = B.CUSTOMER_ID
          AND SUBSTRING(TRANSACTION_DATE,1,7)  BETWEEN  DATE_FORMAT(ADD_MONTHS(TO_DATE('loc_var+"-01'),-3),'yyyy-MM') AND 'loc_var+'
      GROUP BY A.ID
             , A.NAME
             , SUBSTRING(TRANSACTION_DATE,1,7)

               
               
   •  insert_table(spark:SparkSession)


     - tmp_table 에서 생성한 임시 테이터를 spark의 jdbc connection 을 활용하여 
        활용하여 결과 테이블 insert
     -  아래 SQL 
           SELECT  ROW_NUMBER() OVER (ORDER BY A.GIJUN_MON)    AS ID
               ,  A.GIJUN_MON                                AS BASE_YYMM
               , A.ID                                        AS CUSTOMER_ID
               , A.RESULT                                    AS RISK_SCORE
               , B.GRADE                                     AS RISK_GRADE
       FROM (  SELECT GIJUN_MON
                     , ID
                     , SUM(AMT) AMT
                     , ROUND(((AVG(AMT) - MIN(AMT)) / (MAX(AMT) - MIN(AMT))),2)*100 AS RESULT
                 FROM TMP1_TABLE
             GROUP BY GIJUN_MON
                     , ID
             ) A   LEFT JOIN GRADE_BASE B
           ON A.RESULT BETWEEN MIN_SCORE AND MAX_SCORE


    
       

  •  show_result(spark:SparkSession)
    
       - 결과 테이블 확인용 개발의 편의를 위해 생성함
