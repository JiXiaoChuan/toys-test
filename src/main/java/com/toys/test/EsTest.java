package com.toys.test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import com.toys.service.ElasticsearchService;
import com.toys.thread.EsThread;

public class EsTest {
	
	public static void main(String[] args) throws IOException, InterruptedException{
		
			System.out.println("开始时间："+System.currentTimeMillis());
			ElasticsearchService es = new ElasticsearchService();
			es.init();
			/*es.updateSetting("log.v1", "1");  测试git*/ 
			/*es.createIndex("test","data","test.v1","iau.mapping");
			File dir = new File("E:\\label-store");
			File[] files = dir.listFiles();
			EsThread esThread = new EsThread(new AtomicInteger(0),files,es,"test.v1","data");
			Thread t1 = new Thread(esThread);
			Thread t2 = new Thread(esThread);
			t1.start();
			t2.start();	
			try {
				t1.join();
				t2.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			es.close();
			System.out.println("结束时间："+System.currentTimeMillis());*/
			
			
			es.reindex("test.v1", "data", "test2", "data2", "iau.mapping");
			es.close();
			/*es.update("test1", 1);*/
			/*es.replaceIndex("test", "test1", "test.v1");*/
			/*es.bulkUpdate("test", "data",
					"{\"query\":{\"term\":{\"timestamp\":1462416819909}}}");*/
			/*es.searchDocs("test.v1", "data",
					"{\"filter\":{\"term\":{\"timestamp\":1462431816220}}}");*/
			/*es.bulkDelete("test.v1", "data",
					"{\"query\":{\"filtered\":{\"filter\":{\"range\":{\"timestamp\":{\"gte\":1462500184000}}}}}}");*/
			/*es.existIndex("test5");*/
			/*es.createIndex("test", "data2", "test.v1");*/
			/*es.deleteIndexType("test", "data2");*/
			/*es.deleteIndex("test1");*/
			/*es.bulkIndex("students6.1", "6nianji1ban1");
			/*es.reindex("students6.1", "6nianji1ban1", "shiyanzhongxue2", "6nianji1ban2", "student.mapping");*/
			/*es.replaceIndex("shiyanzhongxue1", "shiyanzhongxue2", "student6.1");*/
			/*es.reindex("version1", "tau4", "iau2", "tau2", "iau.mapping");*/
			
			
	}
	
}
