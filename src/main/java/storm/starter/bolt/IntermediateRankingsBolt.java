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
 * storm.starter.bolt.IntermediateRankingsBolt ���bolt���þ��Ƕ����м���������, ΪʲôҪ�����ⲽ, ӦΪ�������Ƚϴ�, ���ֱ��ȫ�ŵ�һ���ڵ�������, �Ḻ��̫��   
	������ͨ��IntermediateRankingsBolt, ���˵�һЩ   
	������Ȼʹ��, ����obj����fieldsGrouping, ��֤����ͬһ��obj, ��ͬʱ���emit��ͳ�����ݻᱻ���͵�ͬһ��task
	
	IntermediateRankingsBolt�̳���AbstractRankerBolt(�ο�����)   
	��ʵ����updateRankingsWithTuple,
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
    Rankable rankable = RankableObjectWithFields.from(tuple);//�߼��ܼ�, ��Tupleת��Rankable, 
    super.getRankings().updateWith(rankable);//������Rankings�б�   �ο�AbstractRankerBolt, ��bolt�ᶨʱ��Ranking�б�emit��ȥ
  }

  @Override
  Logger getLogger() {
    return LOG;
  }
}