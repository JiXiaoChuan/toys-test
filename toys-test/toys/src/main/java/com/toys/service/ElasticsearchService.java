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
	 * @return 实例化节点
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
	 * @Description: 往vpn中集群里面添加索引
	 * @throws UnknownHostException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 下午3:29:47
	 */
	public void init2() throws UnknownHostException{
		
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "audience")
				.put("client.transport.sniff","true").build();
		client = TransportClient.builder().settings(settings).build()
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.0.120"), 9300)); 
	
	}
	
	/**
	 * @param 关闭节点
	 */
	public void close(){
		client.close();
	}
	
	/**
	 * @Description: 创建索引，注释的代码是在同一个索引下创建不同的type
	 * @param indexName
	 * @param indexType
	 * @param indexAliase
	 * @throws IOException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-26 下午2:57:27
	 */
	public void createIndex(String indexName, String indexType, String indexAliase, String mapping) throws IOException{
		
		if(client==null){ 
			System.out.println("节点没有找到！");
		}else{
			if(existIndex(indexName)){
				System.out.println("索引已存在！");
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
				System.out.println("索引："+indexName+"成功！");
			}
		}
		
	}
	
	/**
	 * @Description: 创建没有副本分片的索引
	 * @param indexName
	 * @param indexType
	 * @param indexAliase
	 * @throws IOException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-26 下午2:57:50
	 */
	public void createIndex2(String indexName, String indexType, String indexAliase) throws IOException{
		
		if(client==null){ 
			System.out.println("节点没有找到！");
		}else{
			if(existIndex(indexName)){
				System.out.println("索引已存在！");
			}else{
				String path = ElasticsearchService.class.getClassLoader().getResource("iau.mapping").getPath();
				String mappingSource = FileUtils.readFileToString(new File(path)).trim();
				client.admin().indices().prepareCreate(indexName).execute().actionGet();
				Settings settings = Settings.builder().put("index.number_of_replicas",1).build();
				client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
				client.admin().indices().prepareAliases().addAlias(indexName, indexAliase).execute().actionGet();
				PutMappingRequest request = Requests.putMappingRequest(indexName).type(indexType).source(mappingSource);
				client.admin().indices().putMapping(request).actionGet();				
				System.out.println("索引："+indexName+"成功！");
			}
		}
		
	}
	
	/**
	 * @Description: 读取并添加文档，使用XCount
	 * @param indexName
	 * @param indexType
	 * @param file
	 * @throws IOException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016年4月26日 上午10:40:02
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
	 * @Description: 索引文件,setConcurrentRequests设置为1的时候文档数量是正确的,记得flush,而且是在单线程下是正确的，试试多线程setConcurrentRequests=1成功！！！
	 * 一定要加flush，   然后试试设置setConcurrentRequests为4，看看数量正确不，还有速度一定要统计出来    612745  数量7133932      中间没有报错 全部正确   
	 * 8,1个节点，907831  还会出个错误，把flush时间加长
	 * 6,1个节点，也会报错，看看最后结果对不对，可能会有丢失数据，丢失了10万条数据 791332 
	 * 4，1  没有报错  931239 
	 * 给一个索引文档加上一个超时时间，看看还会不会出现错误，setConcurrentRequests不超过内核的数量
	 * 
	 * 6.1 735811  超时时间设置了还是丢失的有数据
	 * 
	 * 4,1  7133420 少了500多条， 吧刷新时间去掉了  数据丢失了
	 * 4,1 638952 存储数量也正确，我把timeout加上了，把输出语句删除，      报了一次错,数据丢失了
	 * 4,1 686141   数据丢失了
	 * 以上的是在自己winds上面测试的，就一个人节点，如果setConcurrentRequests 设置为10的话会出错，但是不知道会不会丢数据，   明天一定要测出来  速度和setConcurrentRequests的值
	 * 4月27号测试的时候服务器会断网，不知道原因
	 * 两次4都没有报错，时间差的有点大
	 * 
	 * 6，1  812452 没有报错  但是丢失的有数据   2W条以上,  丢失的数据像是在缓存里面丢失的
	 * 
	 * 509914    4 15000 5MB
	 * 504831    2 15000 5mb
	 * 549136    1 15000 5mb
	 * 
	 * 
	 * 总结：我现在是4,1,数据总量正确，没有丢失数据，但是用户类不是线程安全的，所以id会有可能重复，要试试线程安全重点，还是基础差
	 * 使用id为字符串uuid就不会有id重复的现象存在了  663617时间
	 * 
	 * 试试
	 * 
	 * 
	 * 单台服务器上面2个节点 3 concurrentrequest 523480
	 * 542287    6  无报错
	 * 529514    5 无报错
	 * 			 4 518749无报错
	 * 
	 * 459023  3 4台服务器 
	 * 493158	2	四台服务器
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
	 * 我索引文档的速度一直提高不了，setConcurrentrequest 设置在2-5之间数据完整性最高，
	 * 我的多线程写的有问题，把代码改了肯定能够提高索引速度，上一次索引文件快的原因是四个线程去读取文件，每个线程读一个文件，在读取文件这一步就有很明显的速度提高
	 * 
	 * 
	 * setConcurrentRequests这个参数是要和threadpool线程池配合使用的，能提高服务器cpu的使用，加快索引速度
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
	 * @date 2016-4-27 下午4:48:47
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
				System.out.println("出错了？"+failure.getCause());
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
	 * @Description: 索引文件
	 * @param indexName
	 * @param indexType
	 * @param file
	 * @throws JsonProcessingException
	 * @throws IOException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-27 下午1:57:27
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
	 * @Description: 修改mapping后进行重索引，游标的方法可以达到不停服务重新索引的目的
	 * @param indexName
	 * @param indexType   
	 * void   size 3000 243498 710W ,达到将近3W的重索引速度，这是在本机和250上面，但是在四台服务器的集群上面却连1W都达不到，集群搭建有问题,设置size的会加快速度
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 上午10:19:46
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
				System.out.println("出错了？"+failure.getCause());
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
	 * @Description: 修改副本分片
	 * @param indexName
	 * @param primaryCount   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 下午2:47:10
	 */
	public void update(String indexName, int primaryCount){
		
		Settings settings = Settings.builder()
				.put("number_of_replicas",primaryCount).build();
		client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
		
	}
	
	/**
	 * @Description: 修改settings
	 * @param indexName
	 * @param refresh   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 下午3:51:14
	 */
	public void updateSetting(String indexName, String refreshTime){
		Settings settings = Settings.builder()
				.put("number_of_replicas",refreshTime).build();
		client.admin().indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
	}
	
	/**
	 * @Description: reindex第二步，移除旧索引，新建索引指定别名， 这是直接删除整个索引的
	 * @param fromIndex
	 * @param toIndex
	 * @param alias   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 上午10:38:50
	 */
	public void replaceIndex(String deleteIndex, String keepIndex, String alias){
		
		DeleteIndexResponse response = client.admin().indices().prepareDelete(deleteIndex).execute().actionGet();
		if(response.isAcknowledged()){
			client.admin().indices().prepareAliases().addAlias(keepIndex, alias).execute().actionGet();
			System.out.println("添加别名成功！");
		}else{
			System.out.println("删除索引失败！");
		}
		
	}
	
	/**
	 * @Description: 删除指定索引
	 * @param indexName   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-4 下午4:12:51
	 */
	public void deleteIndex(String deleteIndex){
		
		if(existIndex(deleteIndex)){
			DeleteIndexResponse response = client.admin().indices().prepareDelete(deleteIndex).execute().actionGet();
			if(response.isAcknowledged()){
				System.out.println("删除索引成功！");
			}else{
				System.out.println("删除索引失败！");
			}
		}else{
			System.out.println("索引不存在！");
		}
		
	}
		
	/**
	 * @Description: 判断索引是否存在
	 * @param indexName
	 * @return   
	 * boolean  
	 * @throws
	 * @author Toys
	 * @date 2016-5-4 下午3:52:10
	 */
	public boolean existIndex(String indexName){
		
		IndicesExistsResponse response = client.admin().indices().prepareExists(indexName).execute().actionGet();
		return response.isExists();

	}
	
	/**
	 * @Description: 批量更新文档，在不能取得文档id的情况下采取游标的方式一边读取一边批量更新    未完成,setQuery和setSource有区别   未完成XXXXXXXXXXXXXXXXXXXXXXXXXXX	
	 * @param indexName
	 * @param indexType
	 * @param searchJson   
	 * void  
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws
	 * @author Toys
	 * @date 2016-5-4 上午9:45:53
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
				System.out.println("出错了："+failure.getMessage());
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
	 * @Description: 指定查询条件的批量删除文档, 总数7,133,932, 根据查询条件删除5655937,   已完成
	 * Elasticsearch已经标记旧文档为删除并添加了一个完整的新文档。旧版本文档不会立即消失，但你也不能去访问它。Elasticsearch会在你继续索引更多数据时清理被删除的文档。
	 * @param alisa
	 * @param indexType
	 * @param query   
	 * void      
	 * @throws InterruptedException 
	 * @throws
	 * @author Toys
	 * @date 2016-4-25 下午3:58:39
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
				System.out.println("出错了："+failure.getMessage());
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
	 * @Description: 索引标签
	 * @param alias
	 * @param indexType
	 * @param file
	 * @throws InterruptedException   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 下午2:32:05
	 */
	public void bulkReportFile(String alias, String indexType, File file) throws InterruptedException {
		
		BulkProcessor bp = BulkProcessor.builder(client, new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {}	
			@Override
			public void afterBulk(long executionId, BulkRequest request,
					Throwable failure) {
				System.out.println("出错了"+failure.getMessage());
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
	 * @Description: 把截字符串出来的数据转化为json
	 * @param map
	 * @return
	 * @throws IOException   
	 * XContentBuilder  
	 * @throws
	 * @author joy
	 * @date 2016年1月12日 上午10:25:58
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
	 * @Description: 删除索引下哪个type中的所有数据  错误未完成,应该会有简单的方法去操作，源码看的不是很懂XXXXXXXXX
	 * @param alias
	 * @param deleteType   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-4 下午3:08:52
	 */
	/*public void deleteIndexType(String alias, String deleteType){
		
		DeleteIndexResponse response = client.admin().indices().prepareDelete(new String[]{alias, deleteType}).execute().actionGet();
		if(response.isAcknowledged()){
			System.out.println("删除索引成功！");
		}else{
			System.out.println("删除索引失败！");
		}
		
	}*/
	
	
	
//	--------------------------------------------------------------------下面是查询的接口
	
	
	/**
	 * @Description: 直接使用json查询,addFields中是返回字段
	 * @param indexName
	 * @param indexType
	 * @param searchJson   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-5 上午10:15:29
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
	 * @Description: 根据bool条件查询es
	 * @param alias
	 * @param type
	 * @param fieldsAndValues   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 下午7:00:46
	 */
	public void searchDocsByConditions(String alias, String type, Map<String, String> fieldsAndValues){
		
		SearchRequestBuilder srb = client.prepareSearch(alias).setTypes(type).setSize(1);
		BoolQueryBuilder bqb = new BoolQueryBuilder();
		bqb = boolConditions(bqb, fieldsAndValues);
		System.out.println(bqb);
		SearchResponse response = srb.setQuery(bqb).execute().actionGet();
		SearchHit[] searchHits = response.getHits().getHits();
		System.out.println("耗时："+response.getTookInMillis()+"数量："+response.getHits().getTotalHits());
		for(SearchHit searchHit : searchHits){
			System.out.println(searchHit.getSourceAsString());
		}
		
	}
	
	/**
	 * @Description: .setSearchType(SearchType.QUERY_AND_FETCH)这个条件在分组的时候使用
	 * @param alias
	 * @param type
	 * @param aggsName   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-10 上午9:42:40
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
	 * @Description: 拼bool条件，有区间过滤
	 * @param bqb
	 * @param fieldsAndValues
	 * @return   
	 * BoolQueryBuilder  
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 下午7:01:20
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
	 * @Description: 拼接分组条件
	 * @param srb
	 * @param fieldList   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-10 上午9:39:54
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
	 * @Description: 解析StringTerms对象，是一个树形结构
	 * @param aggsList
	 * @param terms
	 * @param stack   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-4-11 下午5:03:16
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
	 * @Description: 把结果输出到控制台
	 * @param buckets   
	 * void  
	 * @throws
	 * @author Toys
	 * @date 2016-5-10 下午2:04:38
	 */
	public void print(List<Bucket> buckets){

		for(int i=0;i<buckets.size();i++){
			System.out.print(buckets.get(i).getKey()+"-");
		}
		Avg avg = buckets.get(buckets.size()-1).getAggregations().get("avgs_AdUnitWidth");
		System.out.print("文档数:"+buckets.get(buckets.size()-1).getDocCount()+"-");
		System.out.print("平均宽度:"+avg.getValue());
		System.out.println();
		
	}
	
}
