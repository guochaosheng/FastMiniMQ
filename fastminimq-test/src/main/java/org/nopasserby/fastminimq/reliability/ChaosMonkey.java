/*
 * Copyright 2020 Guo Chaosheng
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.nopasserby.fastminimq.reliability;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.channel.ClientChannel;
import org.apache.sshd.client.channel.ClientChannelEvent;
import org.apache.sshd.client.session.ClientSession;
import org.apache.sshd.common.util.io.NoCloseOutputStream;

public class ChaosMonkey {

    final static String configurationFile = System.getProperty("monkey.configurationFile", "/chaos-monkey.properties");
    
    final static int default_port = 22;
    
    final static int timeout_millis = 10 * 1000;
    
    Node[] nodes;
    
    Random rnd = new Random();
    
    SSH ssh = new SSH();
    
    long period = TimeUnit.MINUTES.toMillis(3);
    
    long downDuration = TimeUnit.SECONDS.toMillis(30);
    
    int batch;
    
    public void start() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            int[] cnt = shuffleArray(nodes.length, rnd);
            {
                System.out.println("shuffle count " + Arrays.toString(cnt));
            }
            @Override
            public void run() {
                int count = batch % nodes.length == 0 ? cnt[batch / nodes.length] + 1 : 1;
                parallelRandomRebootNode(batch, count);
                batch++;
            }
        }, period, period);
        
        try {
            Thread.sleep(nodes.length * nodes.length * period);
        } catch (InterruptedException e) {} // ignore
        
        timer.cancel();
        
        try {
            Thread.sleep(period);
        } catch (InterruptedException e) {} // ignore
        
        printAggregationReport();
    }
    
    /**
    * Print Example:
    * 
    *  Report (category:node stat, date:2020-12-28) 
    * +-----------+---------------+------------+
    * |node name  |node ip        |down count  |
    * +-----------+---------------+------------+
    * |broker1    |47.92.76.189   |10          |
    * |broker2    |39.98.198.13   |14          |
    * |consumer1  |39.100.17.56   |13          |
    * |consumer2  |39.100.47.199  |15          |
    * |consumer3  |39.98.82.212   |8           |
    * |consumer4  |39.98.51.67    |12          |
    * |producer1  |47.92.106.53   |8           |
    * |producer2  |39.98.212.95   |12          |
    * +-----------+---------------+------------+
    * 
    *  Report (category:batch stat, date:2020-12-28) 
    * +-------+-------+-------------+-------------+-------------+-------------+-------------+-------------+-------------+-------------+
    * |batch  |count  |broker1      |broker2      |consumer1    |consumer2    |consumer3    |consumer4    |producer1    |producer2    |
    * +-------+-------+-------------+-------------+-------------+-------------+-------------+-------------+-------------+-------------+
    * |0      |1      |             |             |             |12/28 21:04  |             |             |             |             |
    * |...    |...    |...          |...          |...          |...          |...          |...          |...          |...          |
    * |63     |1      |             |             |12/28 23:05  |             |             |             |             |             |
    * +-------+-------+-------------+-------------+-------------+-------------+-------------+-------------+-------------+-------------+
    */
    public void printAggregationReport() {
        LocalDate now = LocalDate.now();
        
        String[][] nodeStat = new String[nodes.length + 1][3];
        nodeStat[0] = new String[] {"node name", "node host", "down count"};
        for (int i = 0; i < nodes.length; i++) {
            nodeStat[i + 1][0] = nodes[i].name;
            nodeStat[i + 1][1] = nodes[i].sshHost;
            nodeStat[i + 1][2] = nodes[i].logs.size() + "";
        }
        System.out.printf("%n Report (category:node stat, date:%s) %n", now);
        PrintUtil.print(nodeStat);
        
        
        String[][] batchStat = new String[batch + 1][nodes.length + 2];
        int[] batchDownCnt = new int[batch];
        List<String> header = new ArrayList<String>(Arrays.asList("batch", "count"));
        for (int i = 0; i < nodes.length; i++) {
            header.add(nodes[i].name);
            for (int j = 0; j < nodes[i].logs.size(); j++) {
                LogInfo logInfo = nodes[i].logs.get(j);
                batchStat[logInfo.batch + 1][i + 2] = logInfo.downtime.substring(5, 16); // 2021/12/28 20:26:540 ==> 12/28 20:26
                batchDownCnt[logInfo.batch]++;
            }
        }
        batchStat[0] = header.toArray(new String[nodes.length + 2]);
        for (int i = 0; i < batch; i++) {
            batchStat[i + 1][0] = i + "";
            batchStat[i + 1][1] = batchDownCnt[i] + "";
        }
        System.out.printf("%n Report (category:batch stat, date:%s) %n", now);
        PrintUtil.print(batchStat);
    }
    
    int[] shuffleArray(int n, Random rng) {
        int[] arr = new int[n];
        for (int i = 0; i < n; i++) {
            arr[i] = i;
        }
        for (int i = n - 1; i > 0; i--) {
            int j = rng.nextInt(i + 1);
            int tmp = arr[i]; arr[i] = arr[j]; arr[j] = tmp; // swap(i, j)
        };
        return arr;
    }
    
    public void parallelRandomRebootNode(int batch, int count) {
        Thread[] threads = new Thread[count];
        int[] shuffleArray = shuffleArray(nodes.length, rnd);
        for (int i = 0; i < count; i++) {
            threads[i] = new RandomRebootNodeThread(batch, nodes[shuffleArray[i]]);
        }
        for (int i = 0; i < count; i++) {
            threads[i].start();
        }
        for (int i = 0; i < count; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {} // ignore
        }
    }
    
    class RandomRebootNodeThread extends Thread {
        volatile Exception ex;
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY/MM/dd HH:mm:SSS");
        int batch;
        Node node;
        RandomRebootNodeThread(int batch, Node node) {
            this.batch = batch;
            this.node = node;
        }
        @Override
        public void run() {
            LogInfo logInfo = new LogInfo(batch);
            try {
                ssh.exec(node.sshHost, node.sshUsername, node.sshPassword, node.rebootSystemCmd);
                logInfo.downtime = sdf.format(new Date());
                System.out.println(logInfo.downtime + ",batch:" + batch + "," + node.name + ",exec: " + node.rebootSystemCmd);
                
                Thread.sleep(downDuration);
                
                ssh.exec(node.sshHost, node.sshUsername, node.sshPassword, node.startupApplicationCmd);
                logInfo.uptime = sdf.format(new Date());
                System.out.println(logInfo.uptime + ",batch:" + batch + "," + node.name + ",exec: " + node.startupApplicationCmd);
            } catch (Exception ex) {
                new RuntimeException(node.name + " node exception ", ex).printStackTrace();
            }
            node.logs.add(logInfo);
        }
    }
    
    public void load(String configurationFile) {
        Properties props = new Properties();
        File config = new File(configurationFile);
        try {
            props.load(config.isFile() ? new FileInputStream(config) : this.getClass().getResourceAsStream(configurationFile));
        } catch (IOException e) {
            throw new RuntimeException("load chaos monkey properties error", e);
        }
        List<Node> nodeList = new ArrayList<Node>();
        nodeList.addAll(createNodeList(NodeType.BROKER, props));
        nodeList.addAll(createNodeList(NodeType.CONSUMER, props));
        nodeList.addAll(createNodeList(NodeType.PRODUCER, props));
        nodes = nodeList.toArray(new Node[nodeList.size()]);
    }
    
    List<Node> createNodeList(NodeType nodetype, Properties props) {
        List<Node> nodeList = new ArrayList<Node>();
        String[] nodenames = props.getProperty((nodetype + ".label").toLowerCase()).split(",");
        for (String nodename: nodenames) {
            Node node = new Node();
            node.name = nodename;
            node.type = nodetype;
            node.sshHost = props.getProperty(nodename + ".ssh.host");
            node.sshUsername = props.getProperty(nodename + ".ssh.username");
            node.sshPassword = props.getProperty(nodename + ".ssh.password");
            node.rebootSystemCmd = props.getProperty(nodename + ".system.reboot");
            node.startupApplicationCmd = props.getProperty(nodename + ".application.startup");
            nodeList.add(node);
        }
        return nodeList;
    }
    
    static class SSH {
        public void exec(String host, String username, String password, String command) throws Exception {
            SshClient client = SshClient.setUpDefaultClient();
            client.start();
            ClientSession session = client.connect(username, host, default_port).verify(timeout_millis).getSession();
            session.addPasswordIdentity(password);
            
            if (!session.auth().verify(timeout_millis).isSuccess()) {
                throw new Exception("auth faild");
            }
            
            ClientChannel channel = session.createExecChannel(command);
            channel.setOut(new NoCloseOutputStream(System.out));
            channel.setErr(new NoCloseOutputStream(System.err));
            
            if (!channel.open().verify(timeout_millis).isOpened()) {
                throw new Exception("open faild");
            }
            
            List<ClientChannelEvent> list = new ArrayList<ClientChannelEvent>();
            list.add(ClientChannelEvent.CLOSED);
            channel.waitFor(list, timeout_millis);
            channel.close(false);
            session.close(false);
            client.stop();
            client.close(false);
        }
    }
    
    static enum NodeType {
        BROKER, PRODUCER, CONSUMER
    }
    
    static class Node {
        String name;
        NodeType type;
        String sshHost;
        String sshUsername; 
        String sshPassword;
        String rebootSystemCmd;
        String startupApplicationCmd;
        
        List<LogInfo> logs = Collections.synchronizedList(new ArrayList<LogInfo>());
    }
    
    static class LogInfo {
        final int batch;
        String downtime;
        String uptime;
        public LogInfo(int batch) {
            this.batch = batch;
        }
    }
    
    public static void main(String[] args) throws Exception {
        ChaosMonkey chaosMonkey =  new ChaosMonkey();
        chaosMonkey.load(configurationFile);
        chaosMonkey.start();
    }
    
}
