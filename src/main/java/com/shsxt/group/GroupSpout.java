package com.shsxt.group;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

public class GroupSpout extends BaseRichSpout{

    //声明一个SpoutOutputCollector对象，用于发送数据
    private SpoutOutputCollector collector;
    //计数器
    private int count;
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
        //将数据发送给下一个Bolt
        System.out.println("NumberSpout开始发送数据-------");
        this.collector.emit(new Values(count++));
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
        declarer.declare(new Fields("num"));
    }
}