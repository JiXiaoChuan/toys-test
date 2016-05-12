package com.toys.util;

public class StudentUtil {

	private static final String[] FAMILY={"爷爷","奶奶","姥姥","姥爷","爸爸","妈妈","哥哥","姐姐","我","妹妹"};
	private static final String[] DESCRIPTION={
			"却不知从何学起","却不知从何学起","你是否还未觉醒","爱上文学，不做无用功，赢在起跑线。",
			"爱上文学，不做无用功，赢在起跑线。","带你全面快速地透析中考，系统生动地了解文化，",
			"带你全面快速地透析中考，系统生动地了解文化，","初一初二考题已向中考全面靠拢，",
			"初一初二考题已向中考全面靠拢，","吴思榕老师将带你从初一开始，直面中考、爱上文学，不做无用功，赢在起跑线。"
	};
	private static final String[] NAME={"张三","李四","王五","赵六","姬一","刘二",
			"秦九","童八","杨七","赵十"};
	
	public static String[] getFamily(){
		String[] family = new String[5];
		for(int i=0;i<5;i++){
			int num = (int) (Math.random()*10);
			family[i]=FAMILY[num];
		}
		return family;
	}
	
	public static int[] getScore(){
		int[] score = new int[8];
		for(int i=0;i<8;i++){
			int num = (int) (Math.random()*10);
			score[i]=num*num;
		}
		return score;
	}
	
	public static String getDescription(){
		int num = (int) (Math.random()*10);
		String description = DESCRIPTION[num];
		return description;
	}
	
	public static String getName(){
		int num = (int) (Math.random()*10);
		String name = NAME[num];
		return name;
	}
	
	
}
