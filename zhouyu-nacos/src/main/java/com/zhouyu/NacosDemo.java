package com.zhouyu;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * 大都督周瑜
 * 微信ID: dadudu6789
 * 专注帮助程序员提升技术实力，升职涨薪，面试跳槽.
 */
public class NacosDemo {

    public static void main(String[] args) throws NacosException, IOException {

        String serverAddr = "localhost:8848";
//        ConfigService configService = NacosFactory.createConfigService(serverAddr);
//        configService.publishConfig("test.properties", "DEFAULT_GROUP", "k1=v3", ConfigType.PROPERTIES.getType());
//
//        configService.addListener("test.properties", "DEFAULT_GROUP", new Listener() {
//            @Override
//            public Executor getExecutor() {
//                return null;
//            }
//
//            @Override
//            public void receiveConfigInfo(String configInfo) {
//                System.out.println("配置变更了1!");
//                System.out.println(configInfo);
//            }
//        });



        // 初始化配置中心的Nacos Java SDK
        NamingService namingService = NacosFactory.createNamingService(serverAddr);
        Instance instance = new Instance();
        instance.setIp("127.0.0.1");
        instance.setPort(8848);
        instance.setClusterName("DEFAULT");
        instance.setEphemeral(true);
        namingService.registerInstance("com.zhouyu.UserService", instance);
//
//        // 获取服务的全部实例
        List<Instance> allInstances = namingService.selectInstances("com.zhouyu.UserService", true);
//
//        namingService.deregisterInstance("com.zhouyu.UserService", instance);

        System.in.read();
    }
}
