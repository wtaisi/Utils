package com.date.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;

public final class DateUtils {
	private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd");
	private static final SimpleDateFormat MDF = new SimpleDateFormat("yyyy-MM");
	
	private static final Logger log = Logger.getLogger(DateUtils.class);
	private static final SimpleDateFormat DSDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static final SimpleDateFormat SDF2 = new SimpleDateFormat("yyyyMMdd");
	
	public static Date getDateFromString(String ds){
		try {
			return SDF.parse(ds);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	public static String getStringFromDate(Date date){
		return SDF.format(date);
	}
	public static String getStringFromDate2(Date date){
		return SDF2.format(date);
	}
	public static Date getDateTimeFromString(String ds){
		try {
			return DSDF.parse(ds);
		} catch (ParseException e) {
			log.info("字符串格式错误!");
			e.printStackTrace();
		}
		return null;
	}
	
	public static String getStringFromDateTime(Date d){
		return DSDF.format(d);
	}
	public static long getValue(String ds){
		try {
			return DSDF.parse(ds).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	public static String fillDay(String month){
		String result = month;
		String m = month.substring(5);
		int mi = Integer.parseInt(m);//瑞年2月29天
		String year = month.substring(0,4);
		int y = Integer.parseInt(year);
		boolean isRn = false;
		if(y % 400 == 0){
			isRn = true;
		}else if( y % 4 ==0 && y % 100 != 0){
			isRn = true;
		}
		switch(mi){
			case 1: 
				result=month+"-31";
				break;
			case 2:
				result = (isRn?(month+"-29"):(month+"-28"));
				break;
			case 3:
				result = month+"-31";
				break;
			case 4:
				result = month+"-30";
				break;
			case 5:
				result = month+"-31";
				break;
			case 6:
				result = month+"-30";
				break;
			case 7:
				result = month+"-31";
				break;
			case 8:
				result = month+"-31";
				break;
			case 9:
				result = month+"-30";
				break;
			case 10:
				result = month+"-31";
				break;
			case 11:
				result = month+"-30";
				break;
			case 12:
				result = month+"-31";
				break;
			default:
				result="";
				break;
		}
		return result;
	}
	
	/**
	 * 日期 移动 年-月-日
	 * @param forward 负值向前移动，正直向后移动
	 * */
	public static String moveDay(String d, int forward){
		Calendar cd = new GregorianCalendar();
		cd.setTime(getDateFromString(d));
		cd.add(Calendar.MONTH, forward);
		return getStringFromDate(cd.getTime());
	}
	/**
	 * 日期 移动 年-月
	 * @param forward 负值向前移动，正直向后移动
	 * */
	public static String moveMonth(String d, int forward){
		Calendar cd = new GregorianCalendar();
		cd.setTime(getDateByMonth(d));
		cd.add(Calendar.MONTH, forward);
		return getStringByMonth(cd.getTime());
	}
	
	/**
	 * 日期 移动  年-月 -日 
	 * @param forward 负值向前移动，正直向后移动
	 */
	public static String changeDate(String d, int forward) {
		Calendar cd = new GregorianCalendar();
		cd.setTime(getDateFromString(d));
		cd.add(Calendar.DATE, forward);
		return getStringFromDate(cd.getTime());
	}
	
	public static Date getDateByMonth(String ds){
		try {
			return MDF.parse(ds);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	public static String getStringByMonth(Date date){
		return MDF.format(date);
	}
	
	//根据日期获取季度,公历划分
	public static Object[] getQuarter(String date){
		Object[] obj = new Object[2];
		String[] tmp = date.split("-");
		int month = Integer.parseInt(tmp[1]);
		if(month > 0 && month<4 ) {//1-3 一季度
			obj[0] = tmp[0] + "/一季度";
			obj[1] = tmp[0] + "-01-01~" + tmp[0] + "-03-31";
		} else if(month >3 && month <7 ){//4-6 二季度
			obj[0] = tmp[0] + "/二季度";
			obj[1] = tmp[0] + "-04-01~" + tmp[0] + "-06-30";
		} else if(month >6 && month <10) {//7-9 三季度
			obj[0] = tmp[0] + "/三季度";
			obj[1] = tmp[0] + "-07-01~" + tmp[0] + "-09-30";
		}else if(month >9 && month <13) {//10-12 四季度
			obj[0] = tmp[0] + "/四季度";
			obj[1] = tmp[0] + "-10-01~" + tmp[0] + "-12-31";
		}
		return obj;
	} 
}
