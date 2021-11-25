package com.shsxt.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class WordCountSpout extends BaseRichSpout{

    //声明一个SpoutOutputCollector对象，用于发送数据
    private SpoutOutputCollector collector;
    //首先创建一个数组存放要发送的数据
    private String[] array = {
            "Look I really sorry about that telephone call",
            "I hope it is not too long",
            "I do hope you are all right"
    };
    //创建Random对象
    private Random random = new Random();
    /**
     * 当我们执行任务的时候，用于初始化对象
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        //获取初始化的发送器
        this.collector = collector;
    }

    /**
     * 重复调用这个方法从源数据获取一条记录
     * 我们根据业务需求进行封装，然后通过SpoutOutputCollector发送给下一个Bolt
     */
    @Override
    public void nextTuple() {
        //获取本次要发送的字符串
        String line = array[random.nextInt(array.length)];
        //将数据发送给下一个Bolt
        System.out.println("本次发送的数据："+line);
        this.collector.emit(new Values(line));
        try {
            //限制传输速度 1s传一个
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 定义你输出值的属性
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }
}