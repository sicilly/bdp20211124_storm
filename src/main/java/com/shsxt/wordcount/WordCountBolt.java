package com.shsxt.wordcount;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseBasicBolt{

    //声明一个map来存放以前的统计结果
    private Map<String,Integer> map=new HashMap<>();

    /**
     * 处理数据的业务逻辑
     * @param input
     * @param collector
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // 获取单词
        String word=input.getStringByField("word");
        // 判断这个单词是否在map里
        if(map.containsKey(word)){
            // 最新统计的数量为
            int count=map.get(word)+1;
            // 重新设置到map
            map.put(word,count);
        }else{
            // 将新单词放进map
            map.put(word,1);
        }
        System.out.println(this+"本次执行完，单词"+word+"的数量为："+map.get(word));
    }

    /**
     * 如果需要向下传递数据，需要提前定义数据的格式
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {


    }
}