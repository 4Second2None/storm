package com.bg.Trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.planner.ProcessorNode;
import storm.trident.planner.processor.EachProcessor;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import storm.trident.util.TridentUtils;
/**
 * 
 * @author bbaiggey
 * @date 2015��4��27������3:35:19
 * @DESC  BaseFunction�Ƕ�Spout��Bolt�ĳ���
 *       TridentTopologyʵ������StormTopology��builder��
 *       
 *       ������    ��       ������
 *�������ĸ���stream��function
 *�����each�������� ���ݵ��β�   �ֱ��ʾ��  ���������   ����    ���ص�����    ����ĺ������ε���
 *  @Override
    public Stream each(Fields inputFields, Function function, Fields functionFields) {
        projectionValidation(inputFields);
        return _topology.addSourcedNode(this,
                new ProcessorNode(_topology.getUniqueStreamId(),
                    _name,
                    TridentUtils.fieldsConcat(getOutputFields(), functionFields),
                    functionFields,
                    new EachProcessor(inputFields, function)));
    }
 */

public class TridentWordCount {
	
	/**
	 * 
	 *BaseFunction�Ƕ�Spout��Bolt�ĳ���
	 */
  public static class Split extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
      String sentence = tuple.getString(0);
      for (String word : sentence.split(" ")) {
        collector.emit(new Values(word));
      }
    }
  }

  /**
   *  TridentTopologyʵ������StormTopology��builder��
   * @param drpc
   * @return
   */
  public static StormTopology buildTopology(LocalDRPC drpc) {
	  
    FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
        new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
        new Values("how many apples can you eat"), new Values("to be or not to be the person"));
    spout.setCycle(true);
    
/*TridentTopologyʵ������StormTopology��builder��   <=>
    final TopologyBuilder builder = new TopologyBuilder();  
    builderbuilder.createTopology()*/
    TridentTopology topology = new TridentTopology();
	//topology.newStream("spout1", spout)������һ��������Ҫ�������stream  ���ò��ж�λ16
    TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
        new Split(), new Fields("word")).groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(),
        new Count(), new Fields("count")).parallelismHint(16);

    topology.newDRPCStream("words", drpc).each(new Fields("args"), new Split(), new Fields("word")).groupBy(new Fields(
        "word")).stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count")).each(new Fields("count"),
        new FilterNull()).aggregate(new Fields("count"), new Sum(), new Fields("sum"));
    return topology.build();
  }

  public static void main(String[] args) throws Exception {
    Config conf = new Config();
    conf.setMaxSpoutPending(20);
    if (args.length == 0) {
      LocalDRPC drpc = new LocalDRPC();
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
      for (int i = 0; i < 2; i++) {
    	   System.err.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped"));
        Thread.sleep(1000);
      }
    }
    else {
      conf.setNumWorkers(3);
      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
    }
  }
}
