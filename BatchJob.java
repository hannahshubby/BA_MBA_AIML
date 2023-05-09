package sqlBatch;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.RequiredArgsConstructor;
import sqlBatch.dto.SqlBatExeVO;
import sqlBatch.dto.SqlBatTargetSearchVO;

/*
 * Sql 배치 구동 Job
 * 
 * @author 20220327
 */
@Configuration
@RequiredArgsConstructor
public class BatchJob extends DefaultBatchConfigurer {

	@Autowired
    public JobBuilderFactory jobBuilderFactory;
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	// 접근 DB 별 서비스 호출
	@Autowired
	DatabaseConnService dbConnService;
	
	private Logger logger = LoggerFactory.getLogger(BatchJob.class);
	
	// 14자리, 8자리 날짜 포맷 변수
	SimpleDateFormat dtmFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	SimpleDateFormat dtFormat = new SimpleDateFormat("yyyyMMdd");
	// 현재 시간
	Date dateNow = new Date(System.currentTimeMillis());
	
	// Spring Boot 기본 배치 작업내역 Skip을 위한 설정
	@Override
    public void setDataSource(DataSource dataSource) {
        // 여기를 비워놓는다
    }
	
	/*
	 * Sql 배치 Job 설정
	 * @param Step
	 * @return Job
	 */
	@Bean
    public Job sqlBatchJob(Step targetStep, Step readWriteDataStep) {
        logger.info("sql배치프로세스 시작");
        return jobBuilderFactory.get("sqlBatchJob")
                .start(targetStep)
                .next(readWriteDataStep)
                .listener(new CustomStepExecutionListener())
                .build();
    }
    
	/*
	 * Sql 배치 Target Step
	 * @param Tasklet
	 * @return Step
	 */
    @Bean
    public Step targetStep(Tasklet targetSearchTasklet) {
    	return stepBuilderFactory.get("targetStep")
                .tasklet(targetSearchTasklet)
                .build();
    }
    
    /*
	 * Sql 배치 read & write Step
	 * @param Tasklet
	 * @return Step
	 */
    @Bean
    public Step readWriteDataStep(Tasklet readWriteDataTasklet) {
    	return stepBuilderFactory.get("readWriteDataStep")
                .tasklet(readWriteDataTasklet)
                .build();
    }
    
    public class CustomStepExecutionListener implements StepExecutionListener {
        
    	@Override
        public void beforeStep(StepExecution stepExecution) {
            String stepName = stepExecution.getStepName();
            logger.info("stepName = " + stepName+ " start");
            
            Connection con = null;
    		Statement stmt = null;
    		BasicDataSource ds = new BasicDataSource();
    		String sql = "set collation_connection = 'utf8mb4_bin'";
            
            try {
            	
            	con = ds.getConnection();
            	stmt = con.createStatement();
            	stmt.execute(sql);
            	
            } catch (SQLException e) {
    			e.printStackTrace();
    		} finally{
				try {
					if(stmt != null) stmt.close();
					if(con != null) con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
    		}
        }

        @Override
        public ExitStatus afterStep(StepExecution stepExecution) {
            String stepName = stepExecution.getStepName();
            ExitStatus exitStatus = stepExecution.getExitStatus();
            
            logger.info("stepName = " + stepName + " end " + " exitStatus : "+ exitStatus);
            // exitStatus 조작 가능
            //return ExitStatus.FAILED
            logger.info("readLine =============> "+ stepExecution.getReadCount());
            logger.info("Summary =============> "+ stepExecution.getSummary());
            logger.info("insertCnt =============> "+ stepExecution.getWriteCount());
            return null;
        }
    }
    
    /*
	 * Sql 배치 Target Tasklet
	 * @param 
	 * @return Tasklet
	 */
    @Bean
    @StepScope
    public Tasklet targetSearchTasklet(@Value("#{jobParameters[sqlBatId]}") String sqlBatId) {
    	return (contribution, chunkContext)->{
    		
    		logger.info("jobParameters=============>"+sqlBatId);
    		
    		// target 전달용 context 변수
    		StepContext stepContext = chunkContext.getStepContext();
    		
    		ExecutionContext jobExeCtx = stepContext.getStepExecution().getJobExecution().getExecutionContext();
    		
    		SqlBatTargetSearchVO targetSearchVo = new SqlBatTargetSearchVO();
    		
    		// 배치 실행대상 조회
    		targetSearchVo.setSqlBatId(sqlBatId);
    		
    		try {
    			dbConnService.setConnDbmsInfo("C_DDS_nethru");
        		
        		List<Map<String, Object>> targetList = dbConnService.targetListSearch(targetSearchVo);
        		
        		logger.info("targetList=============>"+targetList.toString());
        		
        		//타겟 전달
        		jobExeCtx.put("targetList", targetList);
    		} catch (Exception e) {
    			e.printStackTrace();
				logger.info("targetList Search Error = " + e.getMessage());
    		}
    		
            return RepeatStatus.FINISHED;
    	};
    }
    
    //<-- first layer
    
    // --> second layer
    /*
	 * Sql 배치 read & write Tasklet
	 * @param 
	 * @return Tasklet
	 */
    @Bean
    public Tasklet readWriteDataTasklet() {
    	Tasklet tasklet = new Tasklet() {
    		
    		@Override
    		public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
    			logger.info("readWriteDataTasklet 시작");
        		
        		// target 수신용 context 변수
        		ExecutionContext jobExeCtx = chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext();
        		
        		// 변수 초기화
        		String batchType = "";
        		String readDb = "";
        		String readSql = "";
        		String writeDb = "";
        		String writeSql = "";
        		String writeTblNm = "";
        		int[] writeCount = null;
        		int initCount = 0;
        		String strTrgDb = "";
        		
        		// 14자리, 8자리 날짜 포맷 변수
        		SimpleDateFormat dtmFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        		SimpleDateFormat dtFormat = new SimpleDateFormat("yyyyMMdd");
        		
        		// 타겟 수신
        		List<Map<String, Object>> targetList = (List<Map<String, Object>>) jobExeCtx.get("targetList");
        		
        		// Sql 배치 실행내역 VO
        		SqlBatExeVO sqlBatExeVo = new SqlBatExeVO();
        		 // 배치 최초 실행내역 등록
        		sqlBatExeVo.setBtchPgmId(targetList.get(0).get("btchPgmId").toString());
        		sqlBatExeVo.setBtchPgmExeDate(dtFormat.format(dateNow));
        		// 배치실행일련번호 구하기
        		String maxSqno = "";
        		
    			try {
    				maxSqno = dbConnService.searchSqlBatExeSqno(sqlBatExeVo);
    			} catch (Exception e) {
    				e.printStackTrace();
    				logger.info("sqlBatExeSqno Error = " + e.getMessage());
    			}
    			
        		sqlBatExeVo.setBtchPgmExeSqno(maxSqno);
        		sqlBatExeVo.setBtchPgmExeStepDvCd("01");
        		sqlBatExeVo.setBtchPgmStrtDtm(dtmFormat.format(dateNow));
        		
        		dbConnService.addSqlBatExeData(sqlBatExeVo);
        		
        		// logger.info("수신 targetList=============>"+targetList.toString());
        		
        		// 타겟 Row수 만큼 read & write
        		for(int i=0; i<targetList.size(); i++) {
        			// readList 변수
        			List<Map<String, Object>> readList = new ArrayList<Map<String, Object>>();
        			
        			// 배치 구동 타입
        			batchType = (String) targetList.get(i).get("btchPrcsTypCd");
        			// source DB ID
        			readDb = (String) targetList.get(i).get("srcConnInfoId");
        			// source DB SQL
        			readSql = (String) targetList.get(i).get("srcSqlDtCtnt");
        			// target DB ID
        			writeDb = (String) targetList.get(i).get("trgtConnInfoId");
        			// target DB SQL
        			writeSql = (String) targetList.get(i).get("trgtSqlDtCtnt");
        			// target TABLE NAME
        			writeTblNm = (String) targetList.get(i).get("trgtTblNm");
        			
        			logger.info("배치 구동 타입 =================>"+ batchType);
        			
    				try {
    					// readDB 연결
        				dbConnService.setConnDbmsInfo(readDb);
        				// read데이터 저장
        				readList = dbConnService.searchReadData(readSql, null);
        				
        				// 조회 일자
        				Date dateInq = new Date(System.currentTimeMillis());
        				
    					// 배치 원천데이터 조회완료 실행내역 등록
    					sqlBatExeVo.setBtchPgmExeStepDvCd("02");
    					sqlBatExeVo.setSrcDataInqrCmplDtm(dtmFormat.format(dateInq));
    					sqlBatExeVo.setSrcDataInqrCnt(readList.size());
    	        		
    	        		dbConnService.modifySqlBatExeData(sqlBatExeVo);
    	        		
    				} catch (Exception e) {
    					// 배치 원천데이터 조회 실패 내역 등록
    					sqlBatExeVo.setErrYn("Y");
    					if(e.getMessage() != null) {
    						if(e.getMessage().length() > 100) {
        						sqlBatExeVo.setErrCtnt("sourceDb 연결 실패!!!!!!"+e.getMessage().substring(0, 100));
        					} else {
        						sqlBatExeVo.setErrCtnt("sourceDb 연결 실패!!!!!!"+e.getMessage());
        					}
    					} else {
    						sqlBatExeVo.setErrCtnt("sourceDb 연결 실패!!!!!!");
    					}
    					
    					
    					dbConnService.modifySqlBatExeData(sqlBatExeVo);
    					continue;
    				}
    				
    				
    				try {
    					
        				// writeDb 연결
        				dbConnService.setConnDbmsInfo(writeDb);
        				
        				// 조회 일자
        				Date dateInq = new Date(System.currentTimeMillis());
        				
        				// 배치 타겟데이터 반영시작 실행내역 등록
    					sqlBatExeVo.setBtchPgmExeStepDvCd("03");
    					sqlBatExeVo.setTrgtDataApplStrtDtm(dtmFormat.format(dateInq));
    					dbConnService.modifySqlBatExeData(sqlBatExeVo);
    	        		
    				} catch (Exception e) {
    					// 배치 타겟데이터 반영시작 실패 내역 등록
    					sqlBatExeVo.setErrYn("Y");
    					if(e.getMessage() != null) {
    						if(e.getMessage().length() > 100) {
        						sqlBatExeVo.setErrCtnt("targetDb 연결 실패!!!!!!"+e.getMessage().substring(0, 100));
        					} else {
        						sqlBatExeVo.setErrCtnt("targetDb 연결 실패!!!!!!"+e.getMessage());
        					}
    					} else {
    						sqlBatExeVo.setErrCtnt("targetDb 연결 실패!!!!!!");
    					}
    					
    					dbConnService.modifySqlBatExeData(sqlBatExeVo);
    					continue;
    				}
    				
    				try {
    					int finalWriteCnt = 0;
    					// write 데이터 저장 변수
        				List<Map<String,Object>> paramMap = new ArrayList<Map<String, Object>>();
        				// DB connection에 따른 delete or truncate 위한 변수
        				strTrgDb = writeDb.substring(0, 1);
            			
        				
        				//23.05.10 OOM으로 stream방식으로 수정
        				// read 저장 데이터 write 변수에 저장
            			//for(int j=0; j<readList.size(); j++) {
            			//	//logger.info(readList.get(j).toString());
            			//	paramMap.add(readList.get(j));
            			//	
            			//}
            			
        				// read 저장 데이터 write 변수에 저장
            			readList.stream()
            		    	.forEach(readData -> paramMap.add(readData));
            			
            			
            			//logger.info("paramMap =========>"+ paramMap);
            			
            			// 배치 구동 타입별 구분
            			if("01".equals(batchType)) { // 컬럼단위
            				// write 데이터 저장
                			writeCount = dbConnService.addWriteColumnData(writeTblNm, paramMap);
            			} else if("02".equals(batchType) || "05".equals(batchType)) { // 테이블단위
            				
            				// DW DB는 오라클을 사용하여 truncate 권한이 없음
            				// 기존 데이터를 지우기 위해 delete 쿼리 사용
            				if("D".equals(strTrgDb)) {
            					logger.info("Oracle Write!!!!!!!!!!!");
            					
            					// 최초 기동시에만 delete 처리
            					int delCnt = dbConnService.initWriteDataDelete(writeTblNm, null);
            					
            					writeCount = dbConnService.addWriteTableData(writeTblNm, paramMap);
                			} else {
                				logger.info("MySql Write!!!!");
                				
                				// 최초 기동시에만 truncate 처리
                				String truncStatus = dbConnService.initWriteDataTrunc(writeTblNm, null);
                				
                				writeCount = dbConnService.addWriteTableData(writeTblNm, paramMap);

                			}
            				
            			} else {
            				// 초기화
            				logger.info("Oracle Init!!!!!!!!!!!");
            				dbConnService.setConnDbmsInfo(writeDb);
            				
            				if("04".equals(batchType)) {
            					String truncStatus = dbConnService.initTableDataTrunc(writeSql, null);
            					
            				} else {
            					initCount = dbConnService.initTableData(writeSql, null);
            				}
            			}
            			
            			// 반영 시간
                    	Date dateAppl = new Date(System.currentTimeMillis());
                    	// 배치 타겟데이터 반영완료 실행내역 등록
            			sqlBatExeVo.setBtchPgmExeStepDvCd("04");
            			sqlBatExeVo.setBtchPgmEndDtm(dtmFormat.format(dateAppl));
            			if("01".equals(batchType)) { // 컬럼단위
            				sqlBatExeVo.setTrgtDataApplCnt(writeCount.length);
            			} else if("02".equals(batchType) || "05".equals(batchType)) { // 테이블단위
            				sqlBatExeVo.setTrgtDataApplCnt(writeCount.length);
            			} else {
            				sqlBatExeVo.setTrgtDataApplCnt(initCount);
            			}
            			
            			dbConnService.modifySqlBatExeData(sqlBatExeVo);
    				} catch (Exception e) {
    					// 배치 타겟데이터 반영 실패 내역 등록
    					sqlBatExeVo.setErrYn("Y");
    					if(e.getMessage() != null) {
    						if(e.getMessage().length() > 200) {
        						sqlBatExeVo.setErrCtnt("target데이터 반영 실패!!!!!!"+e.getMessage().substring(0, 200));
        					} else {
        						sqlBatExeVo.setErrCtnt("target데이터 반영 실패!!!!!!"+e.getMessage());
        					}
    					} else {
    						sqlBatExeVo.setErrCtnt("target데이터 반영 실패!!!!!!");
    					}
    					
    					dbConnService.modifySqlBatExeData(sqlBatExeVo);
    					continue;
    				}
        			
    			}
        		
                return RepeatStatus.FINISHED;
    		}
    	};
    	
    	return tasklet;
    }
}