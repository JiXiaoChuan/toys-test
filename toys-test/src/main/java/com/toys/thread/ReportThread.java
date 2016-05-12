package com.toys.thread;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import com.toys.service.ElasticsearchService;

public class ReportThread implements Runnable{

	private File[] files;
	private AtomicInteger tag;
	private String alias;
	private String indexType;
	private ElasticsearchService es;
	
	public ReportThread(ElasticsearchService es, File[] files, AtomicInteger tag, String alias, String indexType){
		this.es=es;
		this.files=files;
		this.tag=tag;
		this.alias=alias;
		this.indexType=indexType;
	}
	
	@Override
	public void run() {
		int index = 0;
		while((index = tag.getAndIncrement()) < files.length){
			try {
				es.bulkReportFile(alias, indexType, files[index]);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
