# storm
storm实时数据流处理
storm的一些经典实现
1.wc
2.RPC DRPC
3.ACK Fail
4.几种Grouping
5.红黄蓝例子【并行度调整】
6.内部消息通讯机制
7.Trident编程
8.订单实时校验项目【kafka + storm + memcached/redis+mysql+zookpper分布式事务锁】
zookeeper分布式锁的使用

添加storm.starter  滑动窗口计算TopN排名类似于spark的window函数 

starter storm.starter.RollingTopWords  
使用fieldsGrouping【单个字段多个task进行并行计算】和globalGrouping【多个task向一个task进行汇集进行全局分组】
