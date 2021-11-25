package com.shsxt.wordcount;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class LineSplitBolt extends BaseBasicBolt{


    /**
     * 处理数据的业务逻辑
     * @param input
     * @param collector
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //首先获取到一行数据
        String line=input.getStringByField("line");
        System.out.println("接收到line为"+line);

        //对line进行切分
        if(line!=null&&line.length()>0){
            // 用空格切分
            String[] words = line.split(" ");
            // 发送到下一个Bolt
            for (String word : words) {
                //继续向后发送数据
                collector.emit(new Values(word));
            }
        }
    }

    /**
     * 如果需要向下传递数据，需要提前定义数据的格式
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}