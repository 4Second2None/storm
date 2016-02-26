package storm.starter.tools;
/**
 * 
 * Description: Rankable除了继承Comparable接口, 还增加getObject()和getCount()接口
 * Copyright:   ©2015 Vbill Payment Co. Ltd. All rights reserved.
 * Created on:  2016年2月26日 下午8:45:26 
 * @author bbaiggey
 */
public interface Rankable extends Comparable<Rankable> {

  Object getObject();

  long getCount();

  /**
   * Note: We do not defensively copy the object wrapped by the Rankable.  It is passed as is.
   *
   * @return a defensive copy
   */
  Rankable copy();
}
