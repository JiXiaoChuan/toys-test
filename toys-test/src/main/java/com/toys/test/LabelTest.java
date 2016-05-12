package com.toys.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.toys.service.ElasticsearchService;
import com.toys.thread.ReportThread;

public class LabelTest {

	/**
	 * @Description: TODO
	 * @param args   
	 * void  
	 * @throws IOException 
	 * @throws
	 * @author Toys
	 * @date 2016-5-9 上午9:22:29
	 */
	public static void main(String[] args) throws IOException {
		
		System.out.println("开始时间："+System.currentTimeMillis());
		ElasticsearchService es = new ElasticsearchService();
		es.init();
		Map<String, String> requestParameters = new HashMap<String, String>();
		requestParameters.put("must_DeviceBrand", "Xiaomi");
		requestParameters.put("range_ActionRequestTime", "ActionRequestTime");
		requestParameters.put("from_ActionRequestTime", "1451926800369");
		requestParameters.put("to_ActionRequestTime", "1451926850369");
		es.searchDocsByConditions("log.v1", "logdata", requestParameters);
		/*List<String> aggsList = new ArrayList<String>();
		aggsList.add("aggs_ActionPlatform");
		aggsList.add("aggs_DeviceOs");
		aggsList.add("aggs_DeviceBrand");
		aggsList.add("aggs_AgentType");
		aggsList.add("aggs_AdUnitLocation");
		aggsList.add("aggs_DeviceModel");
		aggsList.add("aggs_AdUnitViewType");
		aggsList.add("aggs_DeviceType");
		es.searchDocsByAggs("log.v1", "logdata", aggsList);*/
		
		/*es.createIndex("log","logdata","log.v1","newlogmapping.json");
		File dir = new File("E:\\data");
		File[] files = dir.listFiles();
		AtomicInteger tag = new AtomicInteger(0);
		ReportThread rt = new ReportThread(es, files, tag, "log.v1", "logdata");
		Thread t1 = new Thread(rt);
		Thread t2 = new Thread(rt);
		Thread t3 = new Thread(rt);
		Thread t4 = new Thread(rt);
		t1.start();
		t2.start();
		t3.start();
		t4.start();
		try {
			t1.join();
			t2.join();
			t3.join();
			t4.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
		es.close();
		System.out.println("结束时间："+System.currentTimeMillis());
		
	}
	

}
