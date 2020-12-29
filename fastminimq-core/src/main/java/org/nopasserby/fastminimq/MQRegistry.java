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

package org.nopasserby.fastminimq;

import static org.nopasserby.fastminimq.MQUtil.toInetAddress;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MQRegistry {

    public static class MQClusterMetaData {
        
        private String name;
        
        private Map<String, MQBrokerMetaData> brokerMetaDataMap = Collections.synchronizedMap(new LinkedHashMap<String, MQBrokerMetaData>());
        
        public MQClusterMetaData(String name) {
            this.name = name;
        }

        public List<MQBrokerMetaData> metaData() {
            return new ArrayList<MQBrokerMetaData>(brokerMetaDataMap.values());
        }
        
        public void addBrokerMetaData(MQBrokerMetaData brokerMetaData) {
            brokerMetaDataMap.put(brokerMetaData.name(), brokerMetaData);
        }
        
        public String name() {
            return name;
        }
        
    }
    
    public static class MQBrokerMetaData {
        
        private String name;
        
        private String address;
        
        public MQBrokerMetaData(String name, String address) {
            this.name = name;
            this.address = address;
        }
        
        public String name() {
            return name;
        }
        
        public String address() {
            return address;
        }
        
        public SocketAddress socketAddress() {
            return toInetAddress(address());
        }
        
    }

    /**
     * @param metadata 
     * format: clusterName::brokerName@host:port;brokerName@host:port;brokerName@host:port;
     * example: cluster-test::broker-test-1@127.0.0.1:6001;broker-test-2@127.0.0.1:6002;broker-test-3@127.0.0.1:6003;
     * 
     * */
    public static MQClusterMetaData loadClusterMetaData(String metadata) {
        String[] metadataInfo = metadata.split("::");
        String clusterName = metadataInfo[0];
        String brokersInfo = metadataInfo[1];
        MQClusterMetaData clusterMetaData = new MQClusterMetaData(clusterName);
        for (String brokerInfo: brokersInfo.split(";")) {
            String[] info = brokerInfo.split("@");
            clusterMetaData.addBrokerMetaData(new MQBrokerMetaData(info[0], info[1]));
        }
        return clusterMetaData;
    }
    
}
