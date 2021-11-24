package com.shsxt.number;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class NumberBolt extends BaseBasicBolt{

    //声明一个统计器
    private static int count;

    /**
     * 处理数据的业务逻辑
     * @param input
     * @param collector
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("1.NumberBolt从上游获取数据为： " + input);
        System.out.println("NumberBolt打印出数据: "+ input.getInteger(0)+"--"+ input.getIntegerByField("num"));
        count += input.getInteger(0);
        System.out.println("截至到本次,NumberBolt共获取数据和为： "+count);
        //继续向后发送数据
        collector.emit(new Values(count));
    }

    /**
     * 如果需要向下传递数据，需要提前定义数据的格式
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("count"));
    }
}