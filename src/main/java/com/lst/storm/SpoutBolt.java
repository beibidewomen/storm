package com.lst.storm;

import java.util.Map;

import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SpoutBolt extends BaseRichSpout{

    SpoutOutputCollector collector;
    Boolean isFirst = true;

	/**
     * 初始化方法
     */
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
    	System.out.println("TopologyContext======================================"+context);
    	System.out.println("######################################2222######################################");
        this.collector = collector;
    }

    /**
     * 重复调用方法
     */
    public void nextTuple() {
    	System.out.println("######################################3333######################################");
    	String value = "hello world this is a test";
    	String[] values = value.split(" ");
    	if(isFirst) {
        	collector.emit(new Values(value));
        	isFirst = false;
    	}
    }

    /**
     * 输出
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	System.out.println("######################################1111######################################");
        declarer.declare(new Fields("test"));
    }

}
