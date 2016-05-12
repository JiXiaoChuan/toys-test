package com.toys.util;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class BulkProcessorUtil {

	private static BulkProcessor bulkProcessor;
	private BulkProcessorUtil(){}
	
	/**
	 * @Description: �������ģʽ
	 * @param client
	 * @return   
	 * BulkProcessor  
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 ����5:36:03
	 */
	public static synchronized BulkProcessor getBulkProccessor(Client client){
		
		if(bulkProcessor==null){
			initProcessor(client);
		}
		return bulkProcessor;
		
	}
	
	/**
	 * @Description: ��ʼ��
	 * @param client   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 ����5:26:55
	 */
	public static void initProcessor(Client client){
		
		bulkProcessor=BulkProcessor.builder(client, new BulkProcessor.Listener() {
			public void beforeBulk(long executionId, BulkRequest request) {
	
			}
			
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				
			}
			
			public void afterBulk(long executionId, BulkRequest request,
					BulkResponse response) {
				
			}
		}).setBulkActions(20000).setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
		.setConcurrentRequests(10).setFlushInterval(TimeValue.timeValueMillis(10000))
		.build();
		
	}

	/**
	 * @Description: �ر�   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 ����5:30:05
	 */
	public static void closeProcessor(){
		
		try {
			bulkProcessor.awaitClose(1, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
	
	
}
