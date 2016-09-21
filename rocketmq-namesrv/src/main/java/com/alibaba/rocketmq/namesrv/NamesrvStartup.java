/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.namesrv;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.remoting.netty.NettySystemConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author shijia.wxr
 */
public class NamesrvStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion)); // 设置版本号属性


        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketSndbufSize)) {
            NettySystemConfig.socketSndbufSize = 4096; //  如果发送缓冲区为空   默认为4M
        }


        if (null == System.getProperty(NettySystemConfig.SystemPropertySocketRcvbufSize)) {
            NettySystemConfig.socketRcvbufSize = 4096; //  如果接受缓冲区为空   默认为4M
        }

        try {
            //PackageConflictDetect.detectFastjson(); // 检测包冲突,因为rocketmq 配置都是用的fastJson作为序列化工具,所以最低使用的是1.2.3版本,如果版本过低会抛出异常

            Options options = ServerUtil.buildCommandlineOptions(new Options());
            commandLine =
                    ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options),
                            new PosixParser());
            if (null == commandLine) {
                System.exit(-1);
                return null;
            }


            final NamesrvConfig namesrvConfig = new NamesrvConfig(); // 初始化Namesrv 全局配置文件
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            nettyServerConfig.setListenPort(9876); // 设置namesrv监听端口
            if (commandLine.hasOption('c')) { // 根据命令行参数,加载外部namesrvConfig及nettyServerConfig全局配置文件
                String file = commandLine.getOptionValue('c');
                if (file != null) {
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);
                    MixAll.properties2Object(properties, namesrvConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    System.out.println("load config properties file OK, " + file);
                    in.close();
                }
            }


            if (commandLine.hasOption('p')) { // 打印namesrvConfig及nettyServerConfig配置信息
                MixAll.printObjectProperties(null, namesrvConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                System.exit(0);
            }

            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig); //将命令行指定的propertis文件 解析加载到namesrvConfig配置中

            if (null == namesrvConfig.getRocketmqHome()) {
                System.out.println("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                        + " variable in your environment to match the location of the RocketMQ installation");
                System.exit(-2);
            } // 这里必须要设置rocketMq home路径,因为要加载conf配置文件

            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml"); // 初始化logback日志工厂,rocketMq默认使用logback作为日志输出
            final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);


            MixAll.printObjectProperties(log, namesrvConfig); // 打印namesrv配置参数
            MixAll.printObjectProperties(log, nettyServerConfig);


            final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig); // 初始化服务控制对象
            boolean initResult = controller.initialize();
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);


                @Override
                public void run() {
                    synchronized (this) {
                        log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long begineTime = System.currentTimeMillis();
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - begineTime;
                            log.info("shutdown hook over, consuming time total(ms): " + consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));


            controller.start(); // 启动Netty及相关服务

            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.println(tip);

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
