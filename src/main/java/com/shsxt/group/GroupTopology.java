package com.shsxt.group;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class GroupTopology {

    public static void main(String[] args) {
        //创建任务的拓扑图
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //设置拓扑关系（Spout)
        topologyBuilder.setSpout("GroupSpout",new GroupSpout());

        // 开始测试分区策略
        topologyBuilder.setBolt("ShuffleBolt",new ShuffleBolt(),3).shuffleGrouping("GroupSpout");
//        topologyBuilder.setBolt("FieldsBolt",new FieldsBolt(),3).fieldsGrouping("GroupSpout",new Fields("word"));
//        topologyBuilder.setBolt("AllBolt",new AllBolt(),2).allGrouping("GroupSpout");


        //启动Togology
        Config conf = new Config();
        conf.setNumWorkers(2); // 设置两个进程

        //创建一个togology
        StormTopology topology = topologyBuilder.createTopology();

        //本地模式启动集群
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("GroupTopology",conf,topology);

    }
}