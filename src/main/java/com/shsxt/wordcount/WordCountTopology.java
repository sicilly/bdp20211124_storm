package com.shsxt.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class WordCountTopology {

    public static void main(String[] args) {
        //创建任务的拓扑图
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        //设置拓扑关系（Spout)
        topologyBuilder.setSpout("WordCountSpout",new WordCountSpout());
        //设置拓扑关系(Bolt)---切分行
        topologyBuilder.setBolt("LineSplitBolt",new LineSplitBolt(),2).shuffleGrouping("WordCountSpout");
        //设置拓扑关系(Bolt)---对单词数量进行统计，这里不能用shuffleGrouping，否则统计会出错
//        topologyBuilder.setBolt("WordCountBolt",new WordCountBolt(),4).shuffleGrouping("LineSplitBolt");
        topologyBuilder.setBolt("WordCountBolt",new WordCountBolt(),4).fieldsGrouping("LineSplitBolt",new Fields("word"));

        //启动Togology
        Config conf = new Config();
        //创建一个togology
        StormTopology topology = topologyBuilder.createTopology();
        //本地模式启动集群
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("WordCountTopology",conf,topology);

    }
}