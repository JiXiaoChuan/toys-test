package com.toys.service;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import com.toys.util.FileUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.toys.bean.Label;
import com.toys.util.BulkProcessorUtil;


public class ElasticsearchService {

	private Client client;

	/**
	 * @return ʵ�����ڵ�
	 * @throws UnknownHostException 
	 */
	public void init() throws UnknownHostException{
		
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "toys-cluster")
				.put("client.transport.sniff","true").build();
		client = TransportClient.builder().settings(settings).build()
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.5.250"), 9301)); 
	
	}
	
	/**
	 * @Description: ��vpn�м�Ⱥ�����������
	 * @throws UnknownHostException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 ����3:29:47
	 */
	public void init2() throws UnknownHostException{
		
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "audience")
				.put("client.transport.sniff","true").build();
		client = TransportClient.builder().settings(settings).build()
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.0.120"), 9300)); 
	
	}
	
	/**
	 * @param �رսڵ�
	 */
	public void close(){
		client.close();
	}
	
	/**
	 * @Description: ����������ע�͵Ĵ�������ͬһ�������´�����ͬ��type
	 * @param indexName
	 * @param indexType
	 * @param indexAliase
	 * @throws IOException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-26 ����2:57:27
	 */
	public void createIndex(String indexName, String indexType, String indexAliase, String mapping) throws IOException{
		
		if(client==null){ 
			System.out.println("�ڵ�û���ҵ���");
		}else{
			if(existIndex(indexName)){
				System.out.println("�����Ѵ��ڣ�");
				/*String path = ElasticsearchService.class.getClassLoader().getResource("iau.mapping").getPath();
				String mappingSource = FileUtils.readFileToString(new File(path)).trim();
				PutMappingRequest request = Requests.putMappingRequest(indexName).type(indexType).source(mappingSource);
				client.admin().indices().putMapping(request).actionGet();*/
			}else{
				String path = ElasticsearchService.class.getClassLoader().getResource(mapping).getPath();
				String mappingSource = FileUtils.readFileToString(new File(path)).trim();
				client.admin().indices().prepareCreate(indexName).execute().actionGet();
				Settings settings = Settings.builder().put("index.number_of_replicas",1)
						.put("index.refresh_interval","-1")
				.build();
				client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
				client.admin().indices().prepareAliases().addAlias(indexName, indexAliase).execute().actionGet();
				PutMappingRequest request = Requests.putMappingRequest(indexName).type(indexType).source(mappingSource);
				client.admin().indices().putMapping(request).actionGet();				
				System.out.println("������"+indexName+"�ɹ���");
			}
		}
		
	}
	
	/**
	 * @Description: ����û�и�����Ƭ������
	 * @param indexName
	 * @param indexType
	 * @param indexAliase
	 * @throws IOException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-26 ����2:57:50
	 */
	public void createIndex2(String indexName, String indexType, String indexAliase) throws IOException{
		
		if(client==null){ 
			System.out.println("�ڵ�û���ҵ���");
		}else{
			if(existIndex(indexName)){
				System.out.println("�����Ѵ��ڣ�");
			}else{
				String path = ElasticsearchService.class.getClassLoader().getResource("iau.mapping").getPath();
				String mappingSource = FileUtils.readFileToString(new File(path)).trim();
				client.admin().indices().prepareCreate(indexName).execute().actionGet();
				Settings settings = Settings.builder().put("index.number_of_replicas",1).build();
				client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
				client.admin().indices().prepareAliases().addAlias(indexName, indexAliase).execute().actionGet();
				PutMappingRequest request = Requests.putMappingRequest(indexName).type(indexType).source(mappingSource);
				client.admin().indices().putMapping(request).actionGet();				
				System.out.println("������"+indexName+"�ɹ���");
			}
		}
		
	}
	
	/**
	 * @Description: ��ȡ������ĵ���ʹ��XCount
	 * @param indexName
	 * @param indexType
	 * @param file
	 * @throws IOException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016��4��26�� ����10:40:02
	 */
	public void bulkFile(String indexName, String indexType, File file) throws IOException{
		
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
		BufferedReader reader = new BufferedReader(new InputStreamReader(bis));
		String line = null;
		BulkProcessor bulk = BulkProcessorUtil.getBulkProccessor(client);
		while((line=reader.readLine())!=null){
			Label label = new Label();
			label.setUserId();
			label.setAudienceIds(line.split(","));
			label.setTimestamp();
			bulk.add(new IndexRequest(indexName, indexType).source(XContentFactory.jsonBuilder().startObject()
					.field("userId",label.getUserId())
					.field("audienceIds",label.getAudienceIds())
					.field("timestamp",label.getTimestamp()).endObject()));
		}
		reader.close();
		bis.close();
		
	}
	
	/**
	 * @Description: �����ļ�,setConcurrentRequests����Ϊ1��ʱ���ĵ���������ȷ��,�ǵ�flush,�������ڵ��߳�������ȷ�ģ����Զ��߳�setConcurrentRequests=1�ɹ�������
	 * һ��Ҫ��flush��   Ȼ����������setConcurrentRequestsΪ4������������ȷ���������ٶ�һ��Ҫͳ�Ƴ���    612745  ����7133932      �м�û�б��� ȫ����ȷ   
	 * 8,1���ڵ㣬907831  ����������󣬰�flushʱ��ӳ�
	 * 6,1���ڵ㣬Ҳ�ᱨ������������Բ��ԣ����ܻ��ж�ʧ���ݣ���ʧ��10�������� 791332 
	 * 4��1  û�б���  931239 
	 * ��һ�������ĵ�����һ����ʱʱ�䣬�������᲻����ִ���setConcurrentRequests�������ں˵�����
	 * 
	 * 6.1 735811  ��ʱʱ�������˻��Ƕ�ʧ��������
	 * 
	 * 4,1  7133420 ����500������ ��ˢ��ʱ��ȥ����  ���ݶ�ʧ��
	 * 4,1 638952 �洢����Ҳ��ȷ���Ұ�timeout�����ˣ���������ɾ����      ����һ�δ�,���ݶ�ʧ��
	 * 4,1 686141   ���ݶ�ʧ��
	 * ���ϵ������Լ�winds������Եģ���һ���˽ڵ㣬���setConcurrentRequests ����Ϊ10�Ļ���������ǲ�֪���᲻�ᶪ���ݣ�   ����һ��Ҫ�����  �ٶȺ�setConcurrentRequests��ֵ
	 * 4��27�Ų��Ե�ʱ����������������֪��ԭ��
	 * ����4��û�б���ʱ�����е��
	 * 
	 * 6��1  812452 û�б���  ���Ƕ�ʧ��������   2W������,  ��ʧ�����������ڻ������涪ʧ��
	 * 
	 * 509914    4 15000 5MB
	 * 504831    2 15000 5mb
	 * 549136    1 15000 5mb
	 * 
	 * 
	 * �ܽ᣺��������4,1,����������ȷ��û�ж�ʧ���ݣ������û��಻���̰߳�ȫ�ģ�����id���п����ظ���Ҫ�����̰߳�ȫ�ص㣬���ǻ�����
	 * ʹ��idΪ�ַ���uuid�Ͳ�����id�ظ������������  663617ʱ��
	 * 
	 * ����
	 * 
	 * 
	 * ��̨����������2���ڵ� 3 concurrentrequest 523480
	 * 542287    6  �ޱ���
	 * 529514    5 �ޱ���
	 * 			 4 518749�ޱ���
	 * 
	 * 459023  3 4̨������ 
	 * 493158	2	��̨������
	 * 504754   2
	 * 
	 * 445214   3
	 * 
	 * 453573   12
	 * 			16
	 * 383219   50000 8 10
	 * 439299   80000 8 8
	 * 388804   50000 8 8
	 * 344614   50000 8 10
	 * 
	 * �������ĵ����ٶ�һֱ��߲��ˣ�setConcurrentrequest ������2-5֮��������������ߣ�
	 * �ҵĶ��߳�д�������⣬�Ѵ�����˿϶��ܹ���������ٶȣ���һ�������ļ����ԭ�����ĸ��߳�ȥ��ȡ�ļ���ÿ���̶߳�һ���ļ����ڶ�ȡ�ļ���һ�����к����Ե��ٶ����
	 * 
	 * 
	 * setConcurrentRequests���������Ҫ��threadpool�̳߳����ʹ�õģ�����߷�����cpu��ʹ�ã��ӿ������ٶ�
	 * 
	 * 
	 * @param indexName   
	 * @param indexType
	 * @param file
	 * @throws IOException   
	 * void  
	 * @throws InterruptedException 
	 * @throws
	 * @author Toys
	 * @date 2016-4-27 ����4:48:47
	 */
	public void bulkFile3(String alias, String indexType, File file) throws IOException, InterruptedException{
		
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
		BufferedReader reader = new BufferedReader(new InputStreamReader(bis));
		String line = null;

		ObjectMapper om = new ObjectMapper();
		BulkProcessor bulk = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {}	
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				System.out.println("�����ˣ�"+failure.getCause());
			}
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					BulkResponse response) {}
		})
		.setBulkActions(50000).setBulkSize(new ByteSizeValue(8, ByteSizeUnit.MB))
		.setConcurrentRequests(10)
		.build();
		
		while((line = reader.readLine())!=null){
			Label label = new Label();
			label.setUserId();
			label.setAudienceIds(line.split(","));
			label.setTimestamp();
			bulk.add(new IndexRequest(alias, indexType).source(om.writeValueAsBytes(label)).timeout(TimeValue.timeValueSeconds(1)));
		}
		bulk.flush();
		bulk.awaitClose(3, TimeUnit.SECONDS);
		reader.close();
		bis.close();
		
	}
	
	
	/**
	 * @Description: �����ļ�
	 * @param indexName
	 * @param indexType
	 * @param file
	 * @throws JsonProcessingException
	 * @throws IOException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-27 ����1:57:27
	 */
	public void bulk(String indexName, String indexType, File file) throws JsonProcessingException, IOException{
		
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file));
		BufferedReader reader = new BufferedReader(new InputStreamReader(bis),10*1024*1024);
		String line = null;
		ObjectMapper om = new ObjectMapper();
		BulkRequestBuilder bulk = client.prepareBulk();
		int i = 0;
		while((line=reader.readLine())!=null){
			Label label = new Label();
			label.setUserId();
			label.setAudienceIds(line.split(","));
			label.setTimestamp();
			bulk.add(new IndexRequest(indexName, indexType).source(om.writeValueAsBytes(label)));
			i++;
			if(i%20000==0){
				bulk.execute().actionGet();
			}
		}
		bulk.execute().actionGet();
		reader.close();
		bis.close();
		
	}
	
	/**
	 * @Description: �޸�mapping��������������α�ķ������Դﵽ��ͣ��������������Ŀ��
	 * @param indexName
	 * @param indexType   
	 * void   size 3000 243498 710W ,�ﵽ����3W���������ٶȣ������ڱ�����250���棬��������̨�������ļ�Ⱥ����ȴ��1W���ﲻ������Ⱥ�������,����size�Ļ�ӿ��ٶ�
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 ����10:19:46
	 */
	public void reindex(String alias, String oldType, String newIndex, String newType, String fileName) throws IOException, InterruptedException{
		
		String path = ElasticsearchService.class.getClassLoader().getResource(fileName).getPath();
		String mappingSource = FileUtils.readFileToString(new File(path)).trim();	
		client.admin().indices().prepareCreate(newIndex).execute().actionGet();
		Settings settings = Settings.builder()
				.put("index.number_of_replicas",0)
				.build();
		client.admin().indices().prepareUpdateSettings(newIndex).setSettings(settings).execute().actionGet();
		PutMappingRequest request = Requests.putMappingRequest(newIndex).type(newType).source(mappingSource);
		client.admin().indices().putMapping(request).actionGet();
		SearchResponse response = client.prepareSearch(alias).setTypes(oldType).setSearchType(SearchType.QUERY_AND_FETCH).setSize(3000)
		.setScroll(TimeValue.timeValueMillis(6000)).execute().actionGet();
		BulkProcessor bulk = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {}	
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				System.out.println("�����ˣ�"+failure.getCause());
			}
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					BulkResponse response) {}
		})
		.setBulkActions(15000).setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
		.setConcurrentRequests(3)
		.build();
		while(true){
			SearchHit[] hits = response.getHits().getHits();
	        if(hits.length > 0){
	        	for (SearchHit searchHit : hits) {
	        		bulk.add(new IndexRequest(newIndex, newType).source(searchHit.getSource()));
	            }
	        }
	        response = client.prepareSearchScroll(response.getScrollId())
	        		.setScroll(TimeValue.timeValueMillis(2000)).execute().actionGet();
	        if(response.getHits().getHits().length == 0) {
	        	break;
	        }
	    }
		bulk.flush();
		bulk.awaitClose(5, TimeUnit.SECONDS);
		
    }
	
	/**
	 * @Description: �޸ĸ�����Ƭ
	 * @param indexName
	 * @param primaryCount   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 ����2:47:10
	 */
	public void update(String indexName, int primaryCount){
		
		Settings settings = Settings.builder()
				.put("number_of_replicas",primaryCount).build();
		client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
		
	}
	
	/**
	 * @Description: �޸�settings
	 * @param indexName
	 * @param refresh   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 ����3:51:14
	 */
	public void updateSetting(String indexName, String refreshTime){
		Settings settings = Settings.builder()
				.put("number_of_replicas",refreshTime).build();
		client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
	}
	
	/**
	 * @Description: reindex�ڶ������Ƴ����������½�����ָ�������� ����ֱ��ɾ������������
	 * @param fromIndex
	 * @param toIndex
	 * @param alias   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 ����10:38:50
	 */
	public void replaceIndex(String deleteIndex, String keepIndex, String alias){
		
		DeleteIndexResponse response = client.admin().indices().prepareDelete(deleteIndex).execute().actionGet();
		if(response.isAcknowledged()){
			client.admin().indices().prepareAliases().addAlias(keepIndex, alias).execute().actionGet();
			System.out.println("��ӱ����ɹ���");
		}else{
			System.out.println("ɾ������ʧ�ܣ�");
		}
		
	}
	
	/**
	 * @Description: ɾ��ָ������
	 * @param indexName   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-4 ����4:12:51
	 */
	public void deleteIndex(String deleteIndex){
		
		if(existIndex(deleteIndex)){
			DeleteIndexResponse response = client.admin().indices().prepareDelete(deleteIndex).execute().actionGet();
			if(response.isAcknowledged()){
				System.out.println("ɾ�������ɹ���");
			}else{
				System.out.println("ɾ������ʧ�ܣ�");
			}
		}else{
			System.out.println("���������ڣ�");
		}
		
	}
		
	/**
	 * @Description: �ж������Ƿ����
	 * @param indexName
	 * @return   
	 * boolean  
	 * @throws
	 * @author Toys
	 * @date 2016-5-4 ����3:52:10
	 */
	public boolean existIndex(String indexName){
		
		IndicesExistsResponse response = client.admin().indices().prepareExists(indexName).execute().actionGet();
		return response.isExists();

	}
	
	/**
	 * @Description: ���������ĵ����ڲ���ȡ���ĵ�id������²�ȡ�α�ķ�ʽһ�߶�ȡһ����������    δ���,setQuery��setSource������   δ���XXXXXXXXXXXXXXXXXXXXXXXXXXX	
	 * @param indexName
	 * @param indexType
	 * @param searchJson   
	 * void  
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws
	 * @author Toys
	 * @date 2016-5-4 ����9:45:53
	 */
	public void bulkUpdate(String alias, String indexType, String searchJson) throws InterruptedException, IOException{
		
		SearchResponse response = client.prepareSearch(alias).setQuery(searchJson)
				.setScroll(TimeValue.timeValueMillis(6000)).execute().actionGet();
		BulkProcessor bp = new BulkProcessor.Builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {}
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				System.out.println("�����ˣ�"+failure.getMessage());
			}
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					BulkResponse response) {}
		}).setBulkActions(15000).setConcurrentRequests(3)
		.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)).build();
		while(true){
			SearchHit[] searchHits = response.getHits().getHits();
			if(searchHits.length>0){
				for(SearchHit searchHit : searchHits){
					bp.add(new UpdateRequest(alias, indexType, searchHit.getId()).doc(XContentFactory.jsonBuilder().startObject()
							.field("timestamp", 1111111111111l)
							.endObject()));
				}
			}
			response = client.prepareSearchScroll(response.getScrollId())
			.setScroll(TimeValue.timeValueMillis(5000)).execute().actionGet();
			if(response.getHits().getHits().length==0){
				break;
			}
		}
		bp.flush();
		bp.awaitClose(5, TimeUnit.SECONDS);
		
	}
	
	/**
	 * @Description: ָ����ѯ����������ɾ���ĵ�, ����7,133,932, ���ݲ�ѯ����ɾ��5655937,   �����
	 * Elasticsearch�Ѿ���Ǿ��ĵ�Ϊɾ���������һ�����������ĵ����ɰ汾�ĵ�����������ʧ������Ҳ����ȥ��������Elasticsearch���������������������ʱ����ɾ�����ĵ���
	 * @param alisa
	 * @param indexType
	 * @param query   
	 * void      
	 * @throws InterruptedException 
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 ����3:58:39
	 */
	public void bulkDelete(String alias, String indexType, String searchJson) throws InterruptedException{
		
		SearchResponse response = client.prepareSearch(alias).setTypes(indexType).setQuery(searchJson).setSize(1000)
				.setScroll(TimeValue.timeValueMillis(3000))
				.execute().actionGet();
		BulkProcessor bp = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {}
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				System.out.println("�����ˣ�"+failure.getMessage());
			}
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					BulkResponse response) {}
		}).setBulkActions(10000).setConcurrentRequests(2)
		.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB)).build();
		while(true){
			SearchHit[] searchHits = response.getHits().getHits();
			if(searchHits.length>0){
				for(SearchHit searchHit : searchHits){
					bp.add(new DeleteRequest(alias, indexType, searchHit.getId()));
				}
			}
			response = client.prepareSearchScroll(response.getScrollId())
					.setScroll(TimeValue.timeValueMillis(6000)).execute().actionGet(); 
			if(response.getHits().getHits().length==0){
				break;
			}
		}
		bp.flush();
		bp.awaitClose(5, TimeUnit.SECONDS);
		/*client.admin().indices().prepareFlush(alias).execute().actionGet();*/
		
	}
		
	/**
	 * @Description: ������ǩ
	 * @param alias
	 * @param indexType
	 * @param file
	 * @throws InterruptedException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 ����2:32:05
	 */
	public void bulkReportFile(String alias, String indexType, File file) throws InterruptedException {
		
		BulkProcessor bp = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {}	
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				System.out.println("������"+failure.getMessage());
			}
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					BulkResponse response) {}
		}).setBulkActions(6000).setBulkSize(new ByteSizeValue(8, ByteSizeUnit.MB))
		.setConcurrentRequests(8).build();
		List<Map<String, Object>> results = FileUtil.readLogFile(file, "\\t");
		for (Map<String, Object> map : results) {
			try {
				bp.add(new IndexRequest(alias, indexType).source(jsonBuilder(map)));
			} catch (IOException e) {	
				e.printStackTrace();
			}
		}
		bp.flush();
		bp.awaitClose(2, TimeUnit.SECONDS);
		
	}
	
	/**
	 * @Description: �ѽ��ַ�������������ת��Ϊjson
	 * @param map
	 * @return
	 * @throws IOException   
	 * XContentBuilder  
	 * @throws
	 * @author joy
	 * @date 2016��1��12�� ����10:25:58
	 */
	public XContentBuilder jsonBuilder(Map<String, Object> map)
			throws IOException {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
		Iterator<Entry<String, Object>> iterator = map.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, Object> entry = iterator.next();
			builder = builder.field(entry.getKey(), entry.getValue());
		}
		return builder.endObject();
	}
	
	
	/**
	 * @Description: ɾ���������ĸ�type�е���������  ����δ���,Ӧ�û��м򵥵ķ���ȥ������Դ�뿴�Ĳ��Ǻܶ�XXXXXXXXX
	 * @param alias
	 * @param deleteType   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-4 ����3:08:52
	 */
	/*public void deleteIndexType(String alias, String deleteType){
		
		DeleteIndexResponse response = client.admin().indices().prepareDelete(new String[]{alias, deleteType}).execute().actionGet();
		if(response.isAcknowledged()){
			System.out.println("ɾ�������ɹ���");
		}else{
			System.out.println("ɾ������ʧ�ܣ�");
		}
		
	}*/
	
	
	
//	--------------------------------------------------------------------�����ǲ�ѯ�Ľӿ�
	
	
	/**
	 * @Description: ֱ��ʹ��json��ѯ,addFields���Ƿ����ֶ�
	 * @param indexName
	 * @param indexType
	 * @param searchJson   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-5 ����10:15:29
	 */
	public void searchDocs(String alias, String indexType, String searchJson){
	
		SearchResponse response = client.prepareSearch(alias).setTypes(indexType).addFields("userId","timestamp").setSize(30).setSource(searchJson)
				.execute().actionGet();
		SearchHit[] searchHits = response.getHits().getHits();
		for(SearchHit searchHit : searchHits){
			System.out.println(searchHit.field("timestamp").getValue());
		}
		
	}
	
	/**
	 * @Description: ����bool������ѯes
	 * @param alias
	 * @param type
	 * @param fieldsAndValues   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 ����7:00:46
	 */
	public void searchDocsByConditions(String alias, String type, Map<String, String> fieldsAndValues){
		
		SearchRequestBuilder srb = client.prepareSearch(alias).setTypes(type).setSize(1);
		BoolQueryBuilder bqb = new BoolQueryBuilder();
		bqb = boolConditions(bqb, fieldsAndValues);
		System.out.println(bqb);
		SearchResponse response = srb.setQuery(bqb).execute().actionGet();
		SearchHit[] searchHits = response.getHits().getHits();
		System.out.println("��ʱ��"+response.getTookInMillis()+"������"+response.getHits().getTotalHits());
		for(SearchHit searchHit : searchHits){
			System.out.println(searchHit.getSourceAsString());
		}
		
	}
	
	/**
	 * @Description: .setSearchType(SearchType.QUERY_AND_FETCH)��������ڷ����ʱ��ʹ��
	 * @param alias
	 * @param type
	 * @param aggsName   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-10 ����9:42:40
	 */
	public void searchDocsByAggs(String alias, String type, List<String> aggsList){
		
		SearchRequestBuilder srb = client.prepareSearch(alias).setTypes(type).setSize(0).setSearchType(SearchType.QUERY_AND_FETCH);
		TermsBuilder tbs = aggsConditions(aggsList);
		SearchResponse response = srb.addAggregation(tbs).execute().actionGet();
		System.out.println(response.getTookInMillis());
		StringTerms terms = (StringTerms) response.getAggregations().asMap().get(aggsList.get(0));
	 	aggsList.remove(0);
	 	Stack<Bucket> stack = new Stack<Bucket>(); 
	 	Iterator<Bucket> it = terms.getBuckets().iterator();
	 	while(it.hasNext()){
	 		Bucket bucket = it.next();
	 		stack.clear();
	 		itNode(aggsList, bucket, stack);
	 	}
	 	
	}

	/**
	 * @Description: ƴbool���������������
	 * @param bqb
	 * @param fieldsAndValues
	 * @return   
	 * BoolQueryBuilder  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 ����7:01:20
	 */
	public BoolQueryBuilder boolConditions(BoolQueryBuilder bqb, Map<String, String> requestParameters){
		
		Iterator<String> it = requestParameters.keySet().iterator();
		String rangeField = null;
		String gte = null;
		String lte = null;
		while(it.hasNext()){
			String field = it.next();
			if(field.startsWith("must_")){
				bqb.must(QueryBuilders.termQuery(field.substring(5), requestParameters.get(field)));
			}
			if(field.startsWith("range_")){
				rangeField = requestParameters.get(field);
			}
			if(field.startsWith("from_")){
				gte = requestParameters.get(field);
			}
			if(field.startsWith("to_")){
				lte = requestParameters.get(field);
			}
		}
		if(StringUtils.isNotBlank(rangeField)&&StringUtils.isNotBlank(gte)&&StringUtils.isNotBlank(lte)){
			bqb.must(QueryBuilders.rangeQuery(rangeField).gte(gte).lte(lte));
		}
		return bqb;
		
	}
	
	/**
	 * @Description: ƴ�ӷ�������
	 * @param srb
	 * @param fieldList   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-10 ����9:39:54
	 */
	public TermsBuilder aggsConditions(List<String> aggsList){
		
		TermsBuilder tb = null;
		if(aggsList.size()<=1){
			tb = AggregationBuilders.terms(aggsList.get(0)).field(aggsList.get(0).substring(5)).size(10)
					.subAggregation(AggregationBuilders.avg("avgs_AdUnitWidth").field("AdUnitWidth"));
		}else{
			List<TermsBuilder> TermsBuilderlist = new ArrayList<TermsBuilder>();
			for(String aggs : aggsList){
				if(aggs.equals(aggsList.get(aggsList.size()-1))){
					TermsBuilderlist.add(AggregationBuilders.terms(aggs).field(aggs.substring(5)).size(20)
							.subAggregation(AggregationBuilders.avg("avgs_AdUnitWidth").field("AdUnitWidth"))
							);
				}else{
					TermsBuilderlist.add(AggregationBuilders.terms(aggs).field(aggs.substring(5)).size(20));
				}
			}
			for(int i=TermsBuilderlist.size()-1;i>0;i--){
				tb = TermsBuilderlist.get(i-1).subAggregation(TermsBuilderlist.get(i));
			}
		}
		return tb;
	
	}

	/**
	 * @Description: ����StringTerms������һ�����νṹ
	 * @param aggsList
	 * @param terms
	 * @param stack   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-11 ����5:03:16
	 */
	public void itNode(List<String> aggsList, Bucket bucket, Stack<Bucket> stack){
			
		stack.push(bucket);
		if(stack.size()==aggsList.size()+1){		
			List<Bucket> buckets = new ArrayList<Bucket>();
			Iterator<Bucket> it = stack.iterator();
			while(it.hasNext()){
				Bucket bt = it.next();
				buckets.add(bt);
			}
			print(buckets);
		}else{
			StringTerms stringTerms = (StringTerms) bucket.getAggregations().asMap().get(aggsList.get(stack.size()-1));
			Iterator<Bucket> it = stringTerms.getBuckets().iterator();
			while(it.hasNext()){
				Bucket bt = it.next();
				itNode(aggsList, bt, stack);
				stack.pop();
			}
		}
		
	}
	
	/**
	 * @Description: �ѽ�����������̨
	 * @param buckets   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-10 ����2:04:38
	 */
	public void print(List<Bucket> buckets){

		for(int i=0;i<buckets.size();i++){
			System.out.print(buckets.get(i).getKey()+"-");
		}
		Avg avg = buckets.get(buckets.size()-1).getAggregations().get("avgs_AdUnitWidth");
		System.out.print("�ĵ���:"+buckets.get(buckets.size()-1).getDocCount()+"-");
		System.out.print("ƽ�����:"+avg.getValue());
		System.out.println();
		
	}
	
}
