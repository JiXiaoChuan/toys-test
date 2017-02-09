package com.toys.bean;

import com.toys.util.StudentUtil;

public class Student {


	private static int addId = 0;
	private int id;
	private String name;
	private String[] family;
	private int[] score;
	private String description;
	
	public int getId() {
		return id;
	}
	public void setId() {
		this.id = addId;
		addId++;
	}
	public String[] getFamily() {
		return family;
	}
	public void setFamily() {
		this.family = StudentUtil.getFamily();
	}
	public int[] getScore() {
		return score;
	}
	public void setScore() {
		this.score = StudentUtil.getScore();
	}
	public String getDescription() {
		return description;
	}
	public void setDescription() {
		this.description = StudentUtil.getDescription();
	}
	public String getName() {
		return name;
	}
	public void setName() {
		this.name = StudentUtil.getName();
	}
	
}
