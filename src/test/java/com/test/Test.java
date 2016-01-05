package com.test;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {
	@org.junit.Test
	public void TestLogger(){
	
		Logger logger = LoggerFactory.getLogger(Test.class);
		logger.info("ssssssssssssssss");
	}
	@org.junit.Test
	public void TestRandom(){
	
		Logger logger = LoggerFactory.getLogger(Test.class);
		Random random = new Random();
		for (int i = 0; i < 1000; i++) {
			logger.info(random.nextInt()+"");
		}
		
		
	
	}

}
