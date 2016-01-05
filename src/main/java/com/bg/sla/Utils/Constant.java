package com.bg.sla.Utils;

public class Constant {

	
	
	public static String MYSQL_DRIVER="com.mysql.jdbc.Driver";
	public static String MYSQL_URL ="jdbc:mysql://suixingpay190:3306/order?useUnicode=true&characterEncoding=utf-8";
	public static String MYSQL_USERNAME="root";
	public static String MYSQL_PASSWORD="123";
	
	public static String REDIS_URL="suixingpay199";
	public static int REDIS_PORT=6379;
	
	
	public static String LOCKS_ORDER = "/locks/order";
	public static String ZK_HOST_PORT = "192.168.120.194:2181,192.168.120.195:2181,192.168.120.196:2181,192.168.120.197:2181,192.168.120.198:2181";
	public static String KAFKA_BROKER_HOST_PORT = "suixingpay192:9092,suixingpay193:9092,suixingpay198:9092,suixingpay199:9092";
	
	
}
