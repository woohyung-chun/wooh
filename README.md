2.1. 헤더Headers
1.Project명

 고객의 거래금액에 따른 통계를 구하는 시스템에 따른 대용량 Batch 프로젝트

2.프로젝트 정보

 .설치정보
   1. Scala 
   2. Hadop for winodw(winutils.exe)
   3. spark (spark-3.0.0-bin-hadoop2.7)
   4. mysql 8.0
   5. eclipe (Scala IDE build of Eclipse SDK)
   6.테이블 정보
      CREATE TABLE GRADE_BASE
   (
      ID        VARCHAR(10)   #ID
    , GRADE     VARCHAR(2)     #등급
    , MIN_SCORE INT(4)        #최저점수
    , MAX_SCORE INT(4)        #최고점수
   );

    INSERT INTO GRADE_BASE (ID,GRADE,MIN_SCORE,MAX_SCORE) VALUES ('1','A',71,100);
    INSERT INTO GRADE_BASE (ID,GRADE,MIN_SCORE,MAX_SCORE) VALUES ('2','B',31,70);
    INSERT INTO GRADE_BASE (ID,GRADE,MIN_SCORE,MAX_SCORE) VALUES ('3','C',0,30);


     CREATE TABLE CUSTOMER_RISK_RESULT
     (
        ID            INT(4)   #ID
      , BASE_YYMM     VARCHAR(8)     기준년월
      , CUSTOMER_ID   VARCHAR(10)        #고객ID
      , RISK_SCORE   INT(4)        #위험점수
      , RISK_GRADE VARCHAR(1)    #위험등급
     );
  
  .사용 방법 
  
   업로드된 파일을 다운받아 spark 환경에서 batch 파일을 실행
   
   본 프로그램이 샐행 되기 위해서 위의 설치 정보와 같은 환경이 세팅 되어야 합니다.
 

3. 코딩
  .1주일 정도의 시간으로 코딩된 것으로 performance 및 버그존재시 업데이트 예정 
  





