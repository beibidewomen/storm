package com.lst.storm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class CountBolt extends BaseRichBolt{

    OutputCollector collector;
    Map<String, Integer> map = new HashMap<String, Integer>();

    /**
     * 初始化
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    	System.out.println("######################################8888######################################");
        this.collector = collector;
    }

    /**
     * 执行方法
     */
    public void execute(Tuple input) {
    	System.out.println("######################################9999######################################");
        String word = input.getString(0);
//        if(map.containsKey(word)){
//            Integer c = map.get(word);
//            map.put(word, c+1);
//        }else{
//            map.put(word, 1);
//        }
//        //测试输出
        System.out.println("结果:"+word);
    }

    /**
     * 输出
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	System.out.println("######################################7777######################################");
    }

}
