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

import static org.nopasserby.fastminimq.MQConstants.COMMIT_TX;
import static org.nopasserby.fastminimq.MQConstants.GLOBAL_ID_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MAGIC;
import static org.nopasserby.fastminimq.MQConstants.NON_TX;
import static org.nopasserby.fastminimq.MQConstants.PRE_TX;
import static org.nopasserby.fastminimq.MQConstants.RETRY;
import static org.nopasserby.fastminimq.MQConstants.ROLLBACK_TX;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.SERVER_DECODE_MAX_FRAME_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.PRODUCE;
import static org.nopasserby.fastminimq.MQUtil.checkArgument;
import static org.nopasserby.fastminimq.MQUtil.crc;
import static org.nopasserby.fastminimq.MQUtil.nextId;
import static org.nopasserby.fastminimq.MQUtil.nextUUID;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.nopasserby.fastminimq.MQClient.MQSender;
import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQExecutor.ChannelDelegate;
import org.nopasserby.fastminimq.MQExecutor.MQDispatch;
import org.nopasserby.fastminimq.MQRegistry.MQBrokerMetaData;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQFuture;
import org.nopasserby.fastminimq.MQResult.MQRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;

public class MQProducer {
    
    private static Logger logger = LoggerFactory.getLogger(MQProducer.class);
    
    private volatile MQClusterQueues clusterQueues = new MQClusterQueues();
    
    private Map<Long, MQFutureMetaData> futures = new ConcurrentHashMap<Long, MQFutureMetaData>(1024 * 1024);
    
    private MQProducerCfg producerCfg;
    
    private MQDispatch dispatch = new MQDispatch() {
        @Override
        protected void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData)
                throws Exception {
            MQProducer.this.dispatch(channel, commandCode, commandId, commandData);
        }
    };
    
    private MQClient client = new MQClient(dispatch);
    
    private MQRoundRobinRouter router = new MQRoundRobinRouter();
    
    public MQProducer(MQProducerCfg producerCfg) {
        this.producerCfg = producerCfg;
    }
    
    public MQProducer(MQProducerCfg producerCfg, MQClusterMetaData clusterMetaData) {
        this(producerCfg);
        this.metaDataLoad(clusterMetaData.metaData());
    }
    
    void metaDataLoad(List<MQBrokerMetaData> brokerMetaDataList) {
        metaDataLoad(client, brokerMetaDataList);
    }
    
    void metaDataLoad(MQClient client, List<MQBrokerMetaData> brokerMetaDataList) {
        if (brokerMetaDataList.isEmpty()) {
            return;
        }
        
        Map<String, MQBrokerSender> brokerSenderMapOld = this.clusterQueues.brokerSenderMap;
        Map<String, MQBrokerSender> brokerSenderMap = new LinkedHashMap<String, MQBrokerSender>();
        List<MQBrokerSender> brokerSenderRemoveList = new ArrayList<MQBrokerSender>();
        boolean isUpdate = false;
        for (MQBrokerMetaData brokerMetaData: brokerMetaDataList) {
            MQBrokerSender brokerSenderOld = brokerSenderMapOld.get(brokerMetaData.name());
            if (brokerSenderOld == null) {
                isUpdate = true;
                brokerSenderMap.put(brokerMetaData.name(), new MQBrokerSender(client, brokerMetaData, SERVER_DECODE_MAX_FRAME_LENGTH));
            } else if (!brokerSenderOld.brokerMetaData.address().equals(brokerMetaData.address())) { // if update address on restart
                isUpdate = true;
                brokerSenderRemoveList.add(brokerSenderOld);
                brokerSenderMap.put(brokerMetaData.name(), new MQBrokerSender(client, brokerMetaData, SERVER_DECODE_MAX_FRAME_LENGTH));
            } else {
                brokerSenderMap.put(brokerMetaData.name(), brokerSenderOld);
            }
        }
        
        if (isUpdate) {
            MQClusterQueues clusterQueues = new MQClusterQueues();
            clusterQueues.brokerSenderMap = brokerSenderMap;
            clusterQueues.brokerMetaDataList = brokerMetaDataList;
            this.clusterQueues = clusterQueues;
            closeSender(brokerSenderRemoveList);
        }
    }
    
    private void closeSender(List<MQBrokerSender> brokerSenderRemoveList) {
        for (MQBrokerSender brokerSender: brokerSenderRemoveList) {
            brokerSender.sender.close();
        }
    }

    void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception {
        switch (commandCode) {
            case PRODUCE: {
                produceDispatch(commandId, commandData); 
                break;
            }
            default: throw new IllegalArgumentException("command[code:" + Integer.toHexString(commandCode) + "] not support.");
        }
    }
    
    private void produceDispatch(long commandId, ByteBuf commandData) {
        Status status = Status.valueOf(commandData.readInt());
        if (status == Status.OK) {
            commandData.release();
            complete(commandId, Status.OK);
            return;
        }
        
        Exception exception = dispatch.decodeException(commandData);
        commandData.release();
        complete(commandId, Status.FAIL, exception);
    }
    
    private void complete(long id, Status status) {
        complete(id, status, null);
    }
    
    private void complete(long id, Status status, Exception exception) {
        MQFutureMetaData future = futures.remove(id);
        future.complete0(status, exception);
    }

    public void start() throws Exception {
        logger.info("{} started.", this.getClass().getName());
    }
    
    public void shutdown() throws Exception {
        client.shutdown();
    }
    
    public MQFuture<MQRecord> sendTxMsg(String topic, byte[] body) {
        return dispatch(clusterQueues, newFutureMetaData(createGlobalID(), NON_TX, topic, body));
    }
    
    public MQFuture<MQRecord> commit(MQRecord record) {
        MQFutureMetaData future = newFutureMetaData(record.getId(), COMMIT_TX, record.getTopic(), record.getBody());
        MQClusterQueues clusterQueues = this.clusterQueues;
        future.recordMetaData.addHistory(clusterQueues.metaData(record.getBroker()));
        return dispatch(clusterQueues, future);
    }
    
    public MQFuture<MQRecord> rollback(MQRecord record) {
        MQFutureMetaData future = newFutureMetaData(record.getId(), ROLLBACK_TX, record.getTopic(), record.getBody());
        MQClusterQueues clusterQueues = this.clusterQueues;
        future.recordMetaData.addHistory(clusterQueues.metaData(record.getBroker()));
        return dispatch(clusterQueues, future);
    }
    
    public MQFuture<MQRecord> sendMsg(String topic, byte[] body) {
        return sendMsg(createGlobalID(), topic, body);
    }
    
    public MQFuture<MQRecord> sendMsg(byte[] id, String topic, byte[] body) { 
        checkArgument(id.length == GLOBAL_ID_LENGTH, "id length must be 16 bytes"); // 128 bit
        return dispatch(clusterQueues, newFutureMetaData(id, NON_TX, topic, body));
    }
    
    MQFutureMetaData newFutureMetaData(byte[] id, byte sign, String topic, byte[] body) {
        MQFutureMetaData future = new MQFutureMetaData();
        MQRecordMetaData recordMetaData = new MQRecordMetaData();
        recordMetaData.futureId = nextId();
        recordMetaData.id = id;
        recordMetaData.sign = sign;
        recordMetaData.topic = topic;
        recordMetaData.body = body;
        recordMetaData.producer = name0();
        future.recordMetaData = recordMetaData;
        return future;
    }
    
    private byte[] name0() {
        return name().getBytes();
    }
    
    public String name() {
        return producerCfg.name;
    }
    
    private MQFutureMetaData dispatch(MQClusterQueues clusterQueues, MQFutureMetaData futureMetaData) {
        MQRecordMetaData recordMetaData = futureMetaData.recordMetaData;
        // must be put to the future collection first, because the result is returned asynchronously after offer queue
        futures.put(recordMetaData.futureId, futureMetaData);
        try {
            dispatch(clusterQueues, recordMetaData);
        } catch (Exception e) {
            logger.debug("dispatch error", e);
            complete(recordMetaData.futureId, Status.FAIL, e);
        }
        return futureMetaData;
    }
    
    void dispatch(MQClusterQueues routeDeques, MQRecordMetaData recordMetaData) {
        if (recordMetaData.retry >= RETRY) {
            complete(recordMetaData.futureId, Status.FAIL, new RuntimeException("retry " + recordMetaData.retry + " times"));
            return;
        }
        MQBrokerMetaData brokerMetaData = recordMetaData.historyLast();// default route
        if (recordMetaData.sign == NON_TX || (recordMetaData.retry == 0 && recordMetaData.sign == PRE_TX)) {
            brokerMetaData = route(routeDeques.brokerMetaDataList(), recordMetaData);// extend route
        }
        recordMetaData.addHistory(brokerMetaData);
        routeDeques.dispatch(brokerMetaData, recordMetaData);
    }
    
    
    /**
     * Extendable Method
     * 
     * @throws Exception 
     * 
     * */
    protected byte[] createGlobalID() {
        return nextUUID();
    }
    
    /**
     * Extendable Method
     * 
     * @throws Exception 
     * 
     * */
    protected MQBrokerMetaData route(List<MQBrokerMetaData> brokerMetaDataList, MQRecordMetaData recordMetaData) {
        return router.route(brokerMetaDataList, recordMetaData);
    }
    
    class MQRoundRobinRouter {

        private int sequence;
        
        private Map<MQBrokerMetaData, Long> failureBroker = new ConcurrentHashMap<MQBrokerMetaData, Long>();
        
        public MQBrokerMetaData route(List<MQBrokerMetaData> brokerMetaDataList, MQRecordMetaData recordMetaData) {
            List<MQBrokerMetaData> availableBrokerList = brokerMetaDataList;
            if (!failureBroker.isEmpty()) {
                availableBrokerList = new ArrayList<MQBrokerMetaData>(availableBrokerList);
                
                Set<Entry<MQBrokerMetaData, Long>> entrys = failureBroker.entrySet();
                for (Entry<MQBrokerMetaData, Long> entry: entrys) {
                    MQBrokerMetaData brokerMetaData = entry.getKey();
                    long timestamp = entry.getValue();
                    if (System.currentTimeMillis() - timestamp < TimeUnit.SECONDS.toMillis(60)) {
                        availableBrokerList.remove(brokerMetaData);
                        continue;
                    }
                    failureBroker.remove(brokerMetaData);
                }
            }
            
            // failover
            if (recordMetaData.retry > 0) {
                MQBrokerMetaData brokerMetaData = recordMetaData.historyLast();
                availableBrokerList = new ArrayList<MQBrokerMetaData>(availableBrokerList);
                availableBrokerList.remove(brokerMetaData);
                failureBroker.put(brokerMetaData, System.currentTimeMillis());
            }
            if (availableBrokerList.isEmpty()) {
                throw new IllegalStateException("no broker available");
            }
            return availableBrokerList.get(Math.abs(sequence++) % availableBrokerList.size());
        }
        
    }
    
    ByteBuffer encode(MQRecordMetaData messageMetaData) throws Exception {
        if (messageMetaData.sign == PRE_TX || messageMetaData.sign == ROLLBACK_TX) {
            byte[] producer = messageMetaData.producer;
            
            // magic + sign + producer length + producer
            int length = 1 + 1 + 16 + 2 + producer.length;
            
            ByteBuffer buffer = ByteBuffer.allocate(length);
            /**
             * |----------------------------- producer command data -----------------------------|
             * | magic | sign |              global id              | producer length | producer |
             * |-------|------|-------------------------------------|-----------------|----------|
             * |  XX   |  XX  | XXXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX |      XXXX       |     N    |
              * */
            buffer.put(MAGIC);
            buffer.put(messageMetaData.sign);
            buffer.put(messageMetaData.id);
            buffer.putShort((short) producer.length);
            buffer.put(producer);
            buffer.flip();
            return buffer;
        }
        
        byte[] body = messageMetaData.body;
        byte[] producer = messageMetaData.producer;
        byte[] topic = messageMetaData.topic.getBytes();
        
        // magic + type + transaction id + topic length + topic + body length + body + body crc + producer length + producer
        int length = 1 + 1 + 16 + 2 + topic.length + 2 + body.length + 4 + 2 + producer.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);
        /**
         * |------------------------------------------------------------ producer command data -----------------------------------------------------------|
         * | magic | sign |              global id              | topic length | topic | body length | body |   body  crc    | producer length | producer |
         * |-------|------|-------------------------------------|--------------|-------|-------------|------|----------------|-----------------|----------|
         * |  XX   |  XX  | XXXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX |     XXXX     |   N   |     XXXX    |   N  |    XXXX XXXX   |      XXXX       |     N    |
          * */
        buffer.put(MAGIC);
        buffer.put(messageMetaData.sign);
        buffer.put(messageMetaData.id);
        buffer.putShort((short) topic.length);
        buffer.put(topic);
        buffer.putShort((short) messageMetaData.body.length);
        buffer.put(messageMetaData.body);
        buffer.putInt(crc(messageMetaData.body));
        buffer.putShort((short) producer.length);
        buffer.put(producer);
        buffer.flip();
        return buffer;
    }
    
    void batchRepeatDispatch(List<MQRecordMetaData> batchRecordMetaData) {
        for (MQRecordMetaData recordMetaData: batchRecordMetaData) {
            recordMetaData.retry++;
            dispatch(clusterQueues, recordMetaData);
        }
    }
    
    class MQBrokerSender {
        
        MQClient client;
        
        volatile MQSender sender;
        
        final MQBrokerMetaData brokerMetaData;
        
        final int bufferCapacity;
        
        List<MQRecordMetaData> batchMessageMetaData = new ArrayList<MQRecordMetaData>();
        
        AtomicInteger concurrent = new AtomicInteger();
        
        ByteBuffer buffer;
        
        public MQBrokerSender(final MQClient client, final MQBrokerMetaData brokerMetaData, final int bufferCapacity) {
            this.client = client;
            this.brokerMetaData = brokerMetaData;
            this.bufferCapacity = bufferCapacity;
            this.buffer = ByteBuffer.allocate(bufferCapacity);
        }
        
        public void close() {
            sender.close();
        }
        
        public void put(MQRecordMetaData messageMetaData) {
            concurrent.incrementAndGet();
            
            ByteBuffer commandData = null;
            try {
                commandData = encode(messageMetaData);
            } catch (Exception e) {
                exceptionCaught(e);
            }
            int commandDataLength = commandData.remaining();
            int commandLength = COMMAND_DATA_OFFSET + commandDataLength;
            if (commandLength > bufferCapacity) {
                exceptionCaught(new IllegalArgumentException("command length > max capacity"));
            }
            
            synchronized (this) {
                int remainingCapacity = buffer.remaining();
                if (remainingCapacity < commandLength) {
                    flush(buffer);
                }
                buffer.putInt(PRODUCE);
                buffer.putLong(messageMetaData.futureId);
                buffer.putInt(commandDataLength);
                buffer.put(commandData);
                batchMessageMetaData.add(messageMetaData);
                long value = concurrent.decrementAndGet();
                if (value == 0) {
                    flush(buffer);
                }
            }
        }
        
        private void exceptionCaught(Exception e) {
            synchronized (this) {
                concurrent.decrementAndGet();
                flush(buffer);
            }
            throw new RuntimeException(e);
        }

        private void flush(ByteBuffer buffer) {
            buffer.flip();
            try {
                if (buffer.hasRemaining()) {
                    flush0(buffer);
                }
            } catch (Exception e) {
                logger.warn(sender.toString() + " flush error", e);
                
                List<MQRecordMetaData> batchRecordMetaData = new ArrayList<MQRecordMetaData>(batchMessageMetaData);
                clearCache();
                batchRepeatDispatch(batchRecordMetaData);
            } finally {
                clearCache();
            }
        }
        
        private void clearCache() {
            batchMessageMetaData.clear();
            buffer.clear();
        }
        
        public MQSender sender(MQClient client) {
            if (sender == null) {                
                synchronized (this) {
                    if (sender == null) {
                        sender = client.createMQSender(brokerMetaData.socketAddress());
                    }
                }
            }
            return sender;
        }
        
        protected void flush0(ByteBuffer buffer) throws Exception {
            sender(client).write(buffer); // asynchronous socket write
        }
        
    }
    
    //atomic updater
    private class MQClusterQueues {
        
        Map<String, MQBrokerSender> brokerSenderMap = new LinkedHashMap<String, MQBrokerSender>();
        
        List<MQBrokerMetaData> brokerMetaDataList;

        public MQBrokerMetaData metaData(String broker) {
            return brokerSenderMap.get(broker).brokerMetaData;
        }
        
        void dispatch(MQBrokerMetaData brokerMetaData, MQRecordMetaData messageMetaData) {
            brokerSenderMap.get(brokerMetaData.name()).put(messageMetaData);
        }
        
        List<MQBrokerMetaData> brokerMetaDataList() {
            return brokerMetaDataList;
        }
        
    }
    
    public static class MQRecordMetaData {
        
        Long futureId;
        
        Byte sign;
        
        String topic;
        
        byte[] body;
        
        byte[] id;
         
        volatile int retry;
         
        byte[] producer;
         
        ArrayDeque<MQBrokerMetaData> history = new ArrayDeque<MQBrokerMetaData>(RETRY);
         
        public List<MQBrokerMetaData> history() {
            return new ArrayList<MQBrokerMetaData>(history);
        }
        
        public void addHistory(MQBrokerMetaData brokerMetaData) {
            history.add(brokerMetaData);
        }
        
        public MQBrokerMetaData historyLast() {
            return history.peekLast();
        }
        
        public byte[] getId() {
            return id;
        }
        
        public Byte getSign() {
            return sign;
        }
        
        public byte[] getBody() {
            return body;
        }
        
        public String getTopic() {
            return topic;
        }
        
    }
    
    class MQFutureMetaData extends MQFuture<MQRecord> {
        
        MQRecordMetaData recordMetaData;
        
        @Override
        public MQRecord get(long timeout, TimeUnit timeUnit) throws InterruptedException {
            MQRecord record = super.get();
            MQFutureMetaData future = futures.remove(recordMetaData.futureId);
            if (record == null && future != null) {
                record = complete0(Status.FAIL, new TimeoutException("get timeout."));
            }
            return record;
        }
        
        MQRecord complete0(Status status, Exception exception) {
            MQBrokerMetaData brokerMetaData = recordMetaData.historyLast();
            MQRecord record = new MQRecord();
            record.setId(recordMetaData.id);
            record.setSign(recordMetaData.sign);
            record.setTopic(recordMetaData.topic);
            record.setBody(recordMetaData.body);
            record.setBroker(brokerMetaData.name());
            record.setStatus(status);
            record.setException(exception);
            complete(record);
            return record;
        }
        
    }
    
    public static class MQProducerCfg {
        
        private final String name;
        
        private final String clusterName;
        
        public MQProducerCfg(String name, String clusterName) {
            this.name = name;
            this.clusterName = clusterName;
        }
        
        public String clusterName() {
            return clusterName;
        }
        
        public String name() {
            return name;
        }
        
    }

}


