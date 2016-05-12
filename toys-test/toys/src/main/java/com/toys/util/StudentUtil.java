package com.toys.util;

public class StudentUtil {

	private static final String[] FAMILY={"үү","����","����","��ү","�ְ�","����","���","���","��","����"};
	private static final String[] DESCRIPTION={
			"ȴ��֪�Ӻ�ѧ��","ȴ��֪�Ӻ�ѧ��","���Ƿ�δ����","������ѧ���������ù���Ӯ�������ߡ�",
			"������ѧ���������ù���Ӯ�������ߡ�","����ȫ����ٵ�͸���п���ϵͳ�������˽��Ļ���",
			"����ȫ����ٵ�͸���п���ϵͳ�������˽��Ļ���","��һ�������������п�ȫ�濿£��",
			"��һ�������������п�ȫ�濿£��","��˼����ʦ������ӳ�һ��ʼ��ֱ���п���������ѧ���������ù���Ӯ�������ߡ�"
	};
	private static final String[] NAME={"����","����","����","����","��һ","����",
			"�ؾ�","ͯ��","����","��ʮ"};
	
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
