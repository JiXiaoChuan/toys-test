package com.toys.thread;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import com.toys.service.ElasticsearchService;

public class EsThread implements Runnable{
 	
	private AtomicInteger tag;
	private File[] files;
	private String alias;
	private String indexType;
	private ElasticsearchService es;
	
	public EsThread(AtomicInteger tag, File[] files, ElasticsearchService es, String alias, String indexType){
		this.tag=tag;
		this.files=files;
		this.es=es;
		this.alias=alias;
		this.indexType=indexType;
	}
	
	public void run() {
		
		int index=0;
		try {
			System.out.println(Thread.currentThread().getName()+System.currentTimeMillis());
			if((index=tag.getAndAdd(1))<files.length){
				es.bulkFile3(alias, indexType, files[index]);
			}
			System.out.println(Thread.currentThread().getName()+System.currentTimeMillis());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}

}
