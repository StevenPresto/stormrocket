package storm.rocketmq.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhangfengguang on 2017/11/21.
 */
public class RocketMQBolt implements IRichBolt {

	private static final long serialVersionUID = 7591260982890048043L;

	private static final Logger LOG  = LoggerFactory.getLogger(RocketMQBolt.class);

	private OutputCollector collector;
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
	                    TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		Object msgObj = input.getValue(0);
		Object msgStat = input.getValue(1);
		try {
			System.out.println(msgObj + "==============>>>>>>>>>>>>>" + msgStat);
			LOG.info("Messages:" + msgObj + "\n statistics:" + msgStat);

		} catch (Exception e) {
			collector.fail(input);
			return;
			//throw new FailedException(e);
		}
		collector.ack(input);
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
