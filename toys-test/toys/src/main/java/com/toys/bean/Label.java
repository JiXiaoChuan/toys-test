package com.toys.bean;

import java.util.Date;
import java.util.UUID;

public class Label {
	
	private String userId;
	private int[] audienceIds;
	private Long timestamp;

	public int[] getAudienceIds() {
		return audienceIds;
	}
	public void setAudienceIds(String[] audienceIds) {
		int[] ia=new int[audienceIds.length];
		for(int i=0;i<audienceIds.length;i++){
			if(!"".equals(audienceIds[i])){
				 ia[i]=Integer.parseInt(audienceIds[i]);
			}
		}
		this.audienceIds = ia;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId() {
		this.userId = UUID.randomUUID().toString().replace("-", "");
	}
	public Long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp() {
		this.timestamp = new Date().getTime();
	}

}

