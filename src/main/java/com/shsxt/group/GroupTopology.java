package com.shsxt.group;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;


public class GroupTopology {

    public static void main(String[] args) {
        //创建任务的拓扑图
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //设置拓扑关系（Spout)
        topologyBuilder.setSpout("GroupSpout",new GroupSpout());

        //设置拓扑关系(Bolt)
        //topologyBuilder.setBolt("GroupBolt",new GroupBolt()).shuffleGrouping("GroupSpout");

        // 开启两个线程,默认每个线程有1个task----创建两个对象
//        topologyBuilder.setBolt("GroupBolt",new GroupBolt(),2).shuffleGrouping("GroupSpout");

        // 开启一个线程，设置task为2----创建两个对象
//        topologyBuilder.setBolt("GroupBolt",new GroupBolt()).setNumTasks(2).shuffleGrouping("GroupSpout");

        // 开启两个线程，里面有两个任务----还是创建两个对象（关键是看有几个task）
        topologyBuilder.setBolt("GroupBolt",new GroupBolt(),2).setNumTasks(2).shuffleGrouping("GroupSpout");


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