package storm.starter.bolt;

import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;
import storm.starter.tools.Rankable;
import storm.starter.tools.RankableObjectWithFields;

/**
 * This bolt ranks incoming objects by their count.
 * <p/>
 * It assumes the input tuples to adhere to the following format: (object, object_count, additionalField1,
 * additionalField2, ..., additionalFieldN).
 * 
 * storm.starter.bolt.IntermediateRankingsBolt 这个bolt作用就是对于中间结果的排序, 为什么要增加这步, 应为数据量比较大, 如果直接全放到一个节点上排序, 会负载太重   
	所以先通过IntermediateRankingsBolt, 过滤掉一些   
	这里仍然使用, 对于obj进行fieldsGrouping, 保证对于同一个obj, 不同时间段emit的统计数据会被发送到同一个task
	
	IntermediateRankingsBolt继承自AbstractRankerBolt(参考下面)   
	并实现了updateRankingsWithTuple,
 */
public final class IntermediateRankingsBolt extends AbstractRankerBolt {

  private static final long serialVersionUID = -1369800530256637409L;
  private static final Logger LOG = Logger.getLogger(IntermediateRankingsBolt.class);

  public IntermediateRankingsBolt() {
    super();
  }

  public IntermediateRankingsBolt(int topN) {
    super(topN);
  }

  public IntermediateRankingsBolt(int topN, int emitFrequencyInSeconds) {
    super(topN, emitFrequencyInSeconds);
  }

  @Override
  void updateRankingsWithTuple(Tuple tuple) {
    Rankable rankable = RankableObjectWithFields.from(tuple);
    super.getRankings().updateWith(rankable);
  }

  @Override
  Logger getLogger() {
    return LOG;
  }
}
