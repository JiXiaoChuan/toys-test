package com.toys.service;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticsearchClient {
	
	/**
	 * @return 实例化节点
	 * @throws UnknownHostException 
	 */
	public static Client getClient() throws UnknownHostException{
		
		Settings settings = Settings.settingsBuilder()
				.put("cluster.name", "toys-cluster")
				.put("client.transport.sniff","true").build();
		Client client = TransportClient.builder().settings(settings).build()
		        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.5.250"), 9301)); 
		return client;
		
	}
	
	/**
	 * @param 关闭节点
	 */
	public static void closeClient(Client esClient){
		
		if(esClient!=null){
			esClient.close();
		}
		
	}
	
}
