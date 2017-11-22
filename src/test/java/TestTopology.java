import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import storm.rocketmq.bolt.RocketMQBolt;
import storm.rocketmq.config.RocketMQConfig;
import storm.rocketmq.spout.RocketMQSpout;
import storm.rocketmq.utils.ConfigUtils;

/**
 * Created by zhangfengguang on 2017/11/21.
 */
public class TestTopology {

//	private static final Logger LOG = LoggerFactory.getLogger(TestTopology.class);

	private static final String BOLT_NAME = "rocketmqBolt";
	private static final String PROP_FILE_NAME = "mqspout.test.prop";

	private static Config conf = new Config();
	private static boolean isLocalMode = true;

	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = builder(ConfigUtils.init(PROP_FILE_NAME));
		submitTopology(builder);
	}

	private static TopologyBuilder builder(Config config) throws Exception {
		TopologyBuilder build = new TopologyBuilder();
		int boltParallel = 1;
		int spoutParallel = 1;

		BoltDeclarer testBolt = build.setBolt(BOLT_NAME, new RocketMQBolt(), boltParallel);
		RocketMQSpout spout = new RocketMQSpout();
		RocketMQConfig mqConfig = (RocketMQConfig) config.get(ConfigUtils.CONFIG_ROCKETMQ);
		spout.setConfig(mqConfig);

		String id = (String) config.get(ConfigUtils.CONFIG_TOPIC);
		build.setSpout(id, spout, spoutParallel);
		testBolt.shuffleGrouping(id);
		return build;
	}

	private static void submitTopology(TopologyBuilder builder) {
		try {
			String topologyName = String.valueOf(conf.get("topology.name"));
			StormTopology topology = builder.createTopology();

			if (isLocalMode == true) {
				LocalCluster cluster = new LocalCluster();
				conf.put(Config.STORM_CLUSTER_MODE, "local");

				cluster.submitTopology(topologyName, conf, topology);

				Thread.sleep(50000);

//				cluster.killTopology(topologyName);
//				cluster.shutdown();
			} else {
				conf.put(Config.STORM_CLUSTER_MODE, "distributed");
				StormSubmitter.submitTopology(topologyName, conf, topology);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
