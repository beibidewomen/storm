package com.lst.storm;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class MainClass {

	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {

		// SpoutConfig spoutConfig = new SpoutConfig(zkHosts,
		// topic,"","/data/zookeeper-3.5.4-beta/data") ;
		Config config = new Config();
	      config.setDebug(true);
	      config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
	      String zkConnString = "10.10.92.233:2181";
	      String topic = "topic-test";
	      BrokerHosts hosts = new ZkHosts(zkConnString);
	      SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/data/zookeeper-3.5.4-beta/data" + topic,    
	         UUID.randomUUID().toString());
	      kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
	      kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
	      kafkaSpoutConfig.socketTimeoutMs = 100 * 1000 ;
	      kafkaSpoutConfig.ignoreZkOffsets = true;
	      kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		// 创建一个TopologyBuilder
		TopologyBuilder tb = new TopologyBuilder();
//		 tb.setSpout("SpoutBolt", new SpoutBolt(), 1);
		tb.setSpout("kafkaSpout", new KafkaSpout(kafkaSpoutConfig), 1);
		tb.setBolt("SplitBolt", new SplitBolt(), 1).shuffleGrouping("kafkaSpout");
		tb.setBolt("CountBolt", new CountBolt(), 1).fieldsGrouping("SplitBolt", new Fields("word"));
		// 创建配置
		Config conf = new Config();
		conf.setDebug(false);
		// 设置worker数量
		conf.setNumWorkers(2);
		// 提交任务
		// 集群提交
		// StormSubmitter.submitTopology("myWordcount", conf, tb.createTopology());
		// 本地提交
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf, tb.createTopology());
		} else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("myWordcount", conf, tb.createTopology());
			Utils.sleep(3600000);
			localCluster.shutdown();
		}
	}
}