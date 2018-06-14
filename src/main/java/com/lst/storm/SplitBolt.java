package com.lst.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class SplitBolt extends BaseRichBolt{

    OutputCollector collector;

    /**
     * 初始化
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	System.out.println("######################################5555######################################");
        this.collector = collector;
    }

    /**
     * 执行方法
     */
    public void execute(Tuple input) {
    	System.out.println("######################################6666######################################");
        String line = input.getString(0);
        System.out.println("line================================"+line);
        collector.emit(new Values(line));
    }

    /**
     * 输出
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	System.out.println("######################################4444######################################");
        declarer.declare(new Fields("word"));
    }

}
