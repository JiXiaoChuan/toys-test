package com.toys.util;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;


public class FileUtil {
	
	/**
	 * @Description: ����ĵ�
	 * @param file
	 * @param splitCharactor
	 * @return   
	 * List<Map<String,Object>>  
	 * @throws
	 * @author joy
	 * @date 2016��1��18�� ����9:27:51
	 */
	public static List<Map<String, Object>> readFile(File file,String splitCharactor) {
			
		List<String> lines = null;
		try {
			lines = FileUtils.readLines(file, "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Map<String, Object>> lineList = new ArrayList<Map<String, Object>>(lines.size()); 
		for(int i=0;i<lines.size();i++){
			String[] values = lines.get(i).split(splitCharactor);
			//���30���Լ�ָ���ģ�ֻ��ȡ����ǰ30���ֶΣ���Map�������С��Ϊ�˽�ʡ�ռ䣬��Ϊ��ָ�����ȵĻ���jvm���Լ������㷨��Mapһ����С
			Map<String, Object> map = new HashMap<String, Object>(30);
			for(int j=0;j<30;j++){
				map.put(String.valueOf(j), values[j]);	
			}
			lineList.add(map);
		}
		
		return lineList;
	}
	
	/**
	 * @Description: ���µķָ�����  
	 * @param file
	 * @param splitCharactor
	 * @return   
	 * List<Map<String,Object>>  
	 * @throws
	 * @author joy
	 * @date 2016��2��26�� ����6:10:25
	 */
	public static List<Map<String, Object>> readLogFile(File file,String splitCharactor) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmssSSS");
		byte[] m = {
			1,0,2,1,1,0,4,0,0,0,
			0,0,0,0,0,1,1,0,0,0,
			0,0,0,0,1,0,0,1,0,0,
			0,0,0,0,2,0,0,0,0,0,
			0,1,1,1,1,1,0,0,0,0,
			2,2,2,2,2,2,2,0,0,2,
			2,2,1,0,0,0,2,2,1,1,
			1,1,1,0,0,0,0,0,0,0,
			0,0,0,0,0,0,0,0,0
		};
		
		List<Map<String, Object>> lineList= null;
		List<String> lines= null;
		try {
			lines = FileUtils.readLines(file, "UTF-8");
			lineList = new ArrayList<Map<String,Object>>(lines.size());
			String logfields = FileUtils.readFileToString(new File("E:\\logfield.txt"));
			String[] fields = logfields.trim().split(",");
			for(String line : lines){
				String[] values = line.split(splitCharactor);
				//��ĳ�����
				Map<String, Object> fieldMap = new HashMap<String, Object>(50);
				for(int i=0;i<fields.length;i++){
					if(m[i]==1){
						fieldMap.put(fields[i], StringUtils.isBlank(values[i]) ? null : String.valueOf(values[i]));
					}
					if(m[i]==2){
						fieldMap.put(fields[i], StringUtils.isBlank(values[i]) ? null : Integer.parseInt(values[i]));
					}
					/*if(m[i]==3){
						fieldMap.put(fields[i], StringUtils.isBlank(values[i]) ? null : Float.parseFloat(values[i]));
					}*/
					if(m[i]==4){
						try {
							fieldMap.put(fields[i], sdf.parse(values[i]).getTime());
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
				}
				lineList.add(fieldMap);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lineList;
	}
	
	
}
