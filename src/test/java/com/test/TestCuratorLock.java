package com.test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.retry.ExponentialBackoffRetry;

public class TestCuratorLock {

 /**
  * @param args
  * @throws InterruptedException
  */
 public static void main(String[] args) throws InterruptedException {
  // TODO Auto-generated method stub

  CountDownLatch latch = new CountDownLatch(5);

  String zookeeperConnectionString = "suixingpay194:2181,suixingpay195:2181,suixingpay196:2181";
  RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
  CuratorFramework client = CuratorFrameworkFactory.newClient(
    zookeeperConnectionString, retryPolicy);
  client.start();
  System.out.println("�ͻ���������������");
  ExecutorService exec = Executors.newCachedThreadPool();

  for (int i = 0; i < 5; i++) {
   exec.submit(new MyLock("client" + i, client, latch));
  }

  exec.shutdown();
  latch.await();
  System.out.println("��������ִ�����");

  client.close();

  System.out.println("�ͻ��˹رա�������");

 }

 static class MyLock implements Runnable {

  private String name;

  private CuratorFramework client;

  private CountDownLatch latch;

  public MyLock(String name, CuratorFramework client, CountDownLatch latch) {
   this.name = name;
   this.client = client;
   this.latch = latch;
  }

  public String getName() {
   return name;
  }

  public void setName(String name) {
   this.name = name;
  }

  @Override
  public void run() {
   // TODO Auto-generated method stub
   InterProcessMutex lock = new InterProcessMutex(client,
     "/test_group");
   try {
    if (lock.acquire(120, TimeUnit.SECONDS)) {
     try {
      // do some work inside of the critical section here
      System.out.println("----------" + this.name
        + "�����Դ----------");
      System.out.println("----------" + this.name
        + "���ڴ�����Դ----------");
      Thread.sleep(10 * 1000);
      System.out.println("----------" + this.name
        + "��Դʹ�����----------");
      latch.countDown();
     } finally {
      lock.release();
      System.out.println("----------" + this.name
        + "�ͷ�----------");
     }
    }
   } catch (Exception e) {
    // TODO Auto-generated catch block
    e.printStackTrace();
   }

  }

 }

}