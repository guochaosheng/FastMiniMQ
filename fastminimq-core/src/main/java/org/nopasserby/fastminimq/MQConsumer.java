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

import static org.nopasserby.fastminimq.MQConstants.GLOBAL_ID_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.IMMUTABLE;
import static org.nopasserby.fastminimq.MQConstants.MAGIC;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_LENGTH_SELF_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.CONSUME;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.KV_PUT;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.KV_GET;
import static org.nopasserby.fastminimq.MQUtil.checkArgument;
import static org.nopasserby.fastminimq.MQUtil.crc;
import static org.nopasserby.fastminimq.MQUtil.nextId;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.nopasserby.fastminimq.MQConstants.Transaction;
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
import io.netty.buffer.Unpooled;

public class MQConsumer {
    
    private static Logger logger = LoggerFactory.getLogger(MQConsumer.class);
    
    private Map<Long, RecordListResult> futureRecordMap = new ConcurrentHashMap<Long, RecordListResult>();
    
    private Map<Long, AckResult> futureAckMap = new ConcurrentHashMap<Long, AckResult>();
    
    private Map<Long, UpdateResult> futureUpdateMap = new ConcurrentHashMap<Long, UpdateResult>();
    
    private boolean started;
    
    private MQConsumerCfg consumerCfg;
    
    private CountDownLatch startedLatch = new CountDownLatch(1);
    
    private MQDispatch dispatch = new MQDispatch() {
        @Override
        protected void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData)
                throws Exception {
            MQConsumer.this.dispatch(channel, commandCode, commandId, commandData);
        }
    };
    
    private MQClient client = new MQClient(dispatch);
    
    private MQSender sender;
    
    public MQConsumer(MQConsumerCfg consumerCfg, MQClusterMetaData clusterMetaData) throws Exception {
        this.consumerCfg = consumerCfg;
        this.metaDataLoad(clusterMetaData.metaData());
    }
    
    void metaDataLoad(List<MQBrokerMetaData> brokerMetaDataList) {
        metaDataLoad(client, brokerMetaDataList);
    }
    
    void metaDataLoad(MQClient client, List<MQBrokerMetaData> brokerMetaDataList) {
        if (brokerMetaDataList.isEmpty()) {
            return;
        }
        for (MQBrokerMetaData brokerMetaData: brokerMetaDataList) {
            if (brokerMetaData.name().equals(consumerCfg.brokerName)) {
                metaDataLoad(client, brokerMetaData);
                break;
            }
        }
    }
    
    void metaDataLoad(MQBrokerMetaData brokerMetaData) {
        metaDataLoad(client, brokerMetaData);
    }
    
    void metaDataLoad(MQClient client, MQBrokerMetaData brokerMetaData) {
        SocketAddress newSocketAddress = brokerMetaData.socketAddress();
        if (sender == null) {
            sender = client.createMQSender(newSocketAddress);
            return;
        }
        if (!sender.socketAddress().equals(newSocketAddress)) {    
            sender.close();
            sender = client.createMQSender(newSocketAddress);
        }
    }
    
    public MQConsumer(MQConsumerCfg consumerCfg) throws Exception {
        this.consumerCfg = consumerCfg;
    }
    
    public void start() throws Exception {
        if (!started) {
            started = true;
            startedLatch.countDown();
        }
        logger.info("{} started.", this.getClass().getName());
    }
    
    public void shutdown() {
        client.shutdown();
    }
    
    private void checkStarted() throws InterruptedException {
        if (!started) {
            logger.warn("warn:{} not started, can't operation before started.", MQConsumer.class.getName());
            startedLatch.await();
        }
    }
    
    void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception {
        switch (commandCode) {
            case CONSUME: {
                consumeDispatch(commandId, commandData); 
                break;
            }
            case KV_PUT: {
                consumeOffsetDispatch(commandId, commandData); 
                break;
            }
            case KV_GET: {
                consumeFetchOffsetDispatch(commandId, commandData); 
                break;
            }
            default: throw new IllegalArgumentException("command[code:" + Integer.toHexString(commandCode) + "] not support.");
        }
    }

    private void consumeDispatch(long commandId, ByteBuf commandData) throws Exception {
        RecordListResult recordListResult = futureRecordMap.remove(commandId);
        if (Objects.isNull(recordListResult)) {
            logger.warn("command[id:{}] non has future.", commandId);
            return;
        }
        
        Status status = Status.valueOf(commandData.readInt());
        
        if (status == Status.OK) {
            long nextIndex = commandData.readLong();
            ByteBuf buffer = Unpooled.buffer(commandData.readableBytes());
            commandData.readBytes(buffer);
            commandData.release();
            
            MQResultLazy result = new MQResultLazy();
            result.buffer = buffer.nioBuffer();
            result.setStatus(Status.OK);
            synchronized (recordListResult.future) { // auto completed if timeout 
                recordListResult.queue.nextIndex(nextIndex);
                recordListResult.future.complete(result);
            }
            return;
        }
        
        failComplete(recordListResult.future, commandData);
    }
    
    private void consumeOffsetDispatch(long commandId, ByteBuf commandData) {
        AckResult ackResult = futureAckMap.remove(commandId);
        Status status = Status.valueOf(commandData.readInt());
        if (status == Status.OK) {
            commandData.release();
            
            MQResult<Object> result = new MQResult<Object>();
            result.setStatus(Status.OK);
            ackResult.future.complete(result);
            return;
        }
        if (!commandData.isReadable()) {
            commandData = Unpooled.wrappedBuffer(new String("failed to put offset, maybe kv-db does not have enough space.").getBytes());
        }
        failComplete(ackResult.future, commandData);
    }
    
    private void consumeFetchOffsetDispatch(long commandId, ByteBuf commandData) {
        UpdateResult updateResult = futureUpdateMap.remove(commandId);
        Status status = Status.valueOf(commandData.readInt());
        if (status == Status.OK) {
            long nextIndex = 0;
            int valueLength = commandData.readShort();
            if (valueLength != 0) {
                nextIndex = commandData.readLong();
            }
            commandData.release();
            
            updateResult.queue.nextIndex(nextIndex);
            
            MQResult<Object> result = new MQResult<Object>();
            result.setStatus(Status.OK);
            updateResult.future.complete(result);
            return;
        }
        
        failComplete(updateResult.future, commandData);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void failComplete(MQFuture<?> future, ByteBuf commandData) {
        Exception exception = dispatch.decodeException(commandData);
        commandData.release();
        
        MQResult<Object> result = new MQResult<Object>();
        result.setStatus(Status.FAIL);
        result.setException(exception);
        ((MQFuture) future).complete(result);
    }
    
    class MQResultLazy extends MQResult<List<MQRecord>> {
        ByteBuffer buffer;
        @Override
        public List<MQRecord> getResult() {
            List<MQRecord> recordList = super.getResult();
            if (recordList != null) {
                return recordList;
            }
            synchronized (this) {
                recordList = decodeRecordList(buffer);
                setResult(recordList);
            }
            return recordList;
        }
        
    }
    
    List<MQRecord> decodeRecordList(ByteBuffer buffer) {
        List<MQRecord> recordList = new ArrayList<MQRecord>();
        if (buffer == null) {
            return recordList;
        }
        int length = buffer.remaining();
        while (length > 0) {
            /**
             * | record length | record type | record time stamp  |    record body     |
             * |---------------|-------------|--------------------|--------------------|
             * |   XXXXXXXX    |    XXXX     | XXXXXXXX XXXXXXXX  |          N         |
             * 
             * */
            int recordLength = buffer.getInt();
            int recordType = buffer.getShort();
            checkArgument(recordType == IMMUTABLE, "type error");
            long recordTimestamp = buffer.getLong();
            
            /**
             * |----------------------------------------------------------------------------------------------------------------------------------------------|
             * | magic | sign |              global id              | topic length | topic | body length | body |   body  crc    | producer length | producer |
             * |-------|------|-------------------------------------|--------------|-------|-------------|------|----------------|-----------------|----------|
             * |   XX  |  XX  | XXXXXXXX XXXXXXXX XXXXXXXX XXXXXXXX |     XXXX     |   N   |     XXXX    |   N  |    XXXX XXXX   |      XXXX       |     N    |
             * 
             * */
            byte magic = buffer.get();
            checkArgument(magic == MAGIC, "magic error");
            byte sign = buffer.get();
            checkArgument(sign != Transaction.ROLLBACK.ordinal(), "sign error");
            byte[] id = new byte[GLOBAL_ID_LENGTH];
            buffer.get(id);
            int topicLength = buffer.getShort();
            byte[] topic = new byte[topicLength];
            buffer.get(topic);
            int bodyLength = buffer.getShort();
            byte[] body = new byte[bodyLength];
            buffer.get(body);
            int crc = buffer.getInt();
            checkArgument(crc == crc(body), "crc error");
            int producerLength = buffer.getShort();
            byte[] producer = new byte[producerLength];
            buffer.get(producer);
            
            length -= REOCRD_LENGTH_SELF_LENGTH + recordLength;
            
            MQRecord record = new MQRecord();
            record.setId(id);
            record.setTopic(new String(topic));
            record.setBody(body);
            record.setTimestamp(recordTimestamp);
            record.setStatus(Status.OK);
            
            recordList.add(record);
        }
        return recordList;
    }

    class RecordListResult {
        long delayMillis;
        MQQueue queue;
        MQFuture<MQResult<List<MQRecord>>> future;
    }
    
    class AckResult {
        MQQueue queue;
        MQFuture<MQResult<Object>> future;
    }
    
    class UpdateResult {
        MQQueue queue;
        MQFuture<MQResult<Object>> future;
    }
    
    private MQFuture<MQResult<List<MQRecord>>> fetchMsg0(MQQueue queue, long beginMillis, long delayMillis) throws Exception {
        this.checkStarted();
        
        long id = nextId();
        ByteBuffer out = buildRecordOut(id, queue, beginMillis, delayMillis);
        
        RecordListResult fv = new RecordListResult();
        fv.future = new MQFutureResult(id);
        fv.queue = queue;
        fv.delayMillis = delayMillis;
        
        futureRecordMap.put(id, fv);
        
        sender.write(out);
        
        return fv.future;
    }
    
    class MQFutureResult extends MQFuture<MQResult<List<MQRecord>>> {
        
        final long id;
        
        MQFutureResult(long id) {
            this.id = id;
        }
        
        @Override
        public MQResult<List<MQRecord>> get(long timeout, TimeUnit timeUnit) throws InterruptedException {
            MQResult<List<MQRecord>> result = super.get(timeout, timeUnit);
            RecordListResult recordListResult = futureRecordMap.remove(id);
            if (result == null && recordListResult != null) {
                result = new MQResultLazy();
                result.setStatus(Status.FAIL);
                result.setException(new TimeoutException("get timeout."));
                recordListResult.future.complete(result);
            }
            return result;
        }
        
    }
    
    public MQFuture<MQResult<List<MQRecord>>> fetchMsg(MQQueue queue, Date beginDateTime, long delay, TimeUnit timeunit) throws Exception {
        return fetchMsg0(queue, beginDateTime.getTime(), timeunit.toMillis(delay));
    }
    
    public MQFuture<MQResult<List<MQRecord>>> fetchMsg(MQQueue queue, Date beginDateTime) throws Exception {
        return fetchMsg0(queue, beginDateTime.getTime(), 0);
    }
    
    public MQFuture<MQResult<List<MQRecord>>> fetchMsg(MQQueue queue, long delay, TimeUnit timeunit) throws Exception {
        return fetchMsg0(queue, 0, timeunit.toMillis(delay));
    }
    
    public MQFuture<MQResult<List<MQRecord>>> fetchMsg(MQQueue queue) throws Exception {
        return fetchMsg0(queue, 0, 0);
    }

    public void fetchUpdate(MQQueue queue) throws Exception {
        this.checkStarted();
        
        long id = nextId();
        ByteBuffer out = buildUpdateOut(id, queue);
        
        UpdateResult updateResult = new UpdateResult();
        updateResult.future = new MQFuture<MQResult<Object>>();
        updateResult.queue = queue;
        
        futureUpdateMap.put(id, updateResult);
        
        sender.write(out);
        
        MQResult<Object> result = updateResult.future.get();
        if (result.getStatus() == Status.FAIL) {
            throw result.getException();
        }
    }
    
    public void waitAck(MQQueue queue) throws Exception {
        waitAck(queue, Integer.MAX_VALUE, TimeUnit.HOURS); // keep waiting
    }

    public void waitAck(MQQueue queue, long timeout, TimeUnit timeUnit) throws Exception {
        this.checkStarted();
        
        AckResult ackResult = ack0(queue);
        MQResult<Object> result = ackResult.future.get(timeout, timeUnit);
        if (result.getStatus() == Status.FAIL) {
            throw result.getException();
        }
        queue.ack(); // local queue skip to next index
    }
    
    private AckResult ack0(MQQueue queue) throws Exception {
        this.checkStarted();
        
        long id = nextId();
        ByteBuffer out = buildAckOut(id, queue);
        
        AckResult ackResult = new AckResult();
        ackResult.future = new MQFuture<MQResult<Object>>();
        ackResult.queue = queue;
        
        futureAckMap.put(id, ackResult);
        
        sender.write(out);
        
        return ackResult;
    }
    
    ByteBuffer buildUpdateOut(long id, MQQueue queue) {
        
        byte[] topic = queue.getTopic().getBytes();
        byte[] group = queue.getGroup().getBytes();
        int groups = queue.getSubgroups();
        int groupNo = queue.getSubgroupNo();
        int step = queue.getStep();
        
        // topic length + topic + group length + group + subgroups + subgroupNo + step
        int length = 2 + topic.length + 2 + group.length + 4 + 4 + 4;
        
        ByteBuffer buffer = ByteBuffer.allocate(COMMAND_DATA_OFFSET + length);
        
        buffer.putInt(KV_GET);                            // put command code
        buffer.putLong(id);                               // put command id
        buffer.putInt(length);                            // put command data length
        
        buffer.putShort((short) topic.length);            // put key - topic length
        buffer.put(topic);                                // put key - topic
        buffer.putShort((short) group.length);            // put key - consumer group length
        buffer.put(group);                                // put key - consumer group
        buffer.putInt(groups);                            // put key - groups
        buffer.putInt(groupNo);                           // put key - groupNo
        buffer.putInt(step);                              // put key - step
        buffer.flip();
        
        return buffer;
    }
    
    ByteBuffer buildAckOut(long id, MQQueue queue) throws InterruptedException {
        
        byte[] topic = queue.getTopic().getBytes();
        byte[] group = queue.getGroup().getBytes();
        int subgroups = queue.getSubgroups();
        int subgroupNo = queue.getSubgroupNo();
        int step = queue.getStep();
        
        long index = queue.ack0();
        
        // topic length + topic + group length + group + subgroups + subgroupNo + step
        int keyLength = 2 + topic.length + 2 + group.length + 4 + 4 + 4;
        int valueLength = 8;
        
        // key length + key + value length + value
        int length = 2 + keyLength + 2 + valueLength;
        ByteBuffer buffer = ByteBuffer.allocate(COMMAND_DATA_OFFSET + length);
        
        buffer.putInt(KV_PUT);                            // put command code
        buffer.putLong(id);                               // put command id
        buffer.putInt(length);                            // put command data length
        
        buffer.putShort((short) keyLength);               // put key length
        buffer.putShort((short) topic.length);            // put key - topic length
        buffer.put(topic);                                // put key - topic
        buffer.putShort((short) group.length);            // put key - group length
        buffer.put(group);                                // put key - group
        buffer.putInt(subgroups);                         // put key - subgroups
        buffer.putInt(subgroupNo);                        // put key - subgroupNo
        buffer.putInt(step);                              // put key - step
        buffer.putShort((short) Long.BYTES);              // put value length
        buffer.putLong(index);                            // put value
        
        buffer.flip();
        
        return buffer;
    }
    
    ByteBuffer buildRecordOut(long id, MQQueue queue, long beginMillis, long delayMillis) throws InterruptedException {
        queue.nextIndex(queue.getIndex());
        long nextIndex = queue.nextIndex();
        int rows = queue.rows(nextIndex);
        byte[] topic = queue.getTopic().getBytes();

        int length = 2 + topic.length + 8 + 4 + 8 + 8;
        ByteBuffer buffer = ByteBuffer.allocate(COMMAND_DATA_OFFSET + length);
        
        buffer.putInt(CONSUME);                           // put command code
        buffer.putLong(id);                               // put command id
        buffer.putInt(length);                            // put command data length
        buffer.putShort((short) topic.length);            // put topic length
        buffer.put(topic);                                // put topic
        buffer.putLong(nextIndex);                        // put next index
        buffer.putInt(rows);                              // put rows
        buffer.putLong(beginMillis);                      // put begin time stamp (unit milliseconds)
        buffer.putLong(delayMillis);                      // put delay (unit milliseconds)
        buffer.flip();
        
        return buffer;
    }

    public static class MQConsumerCfg {
        
        String name;
        
        String clusterName;
        
        String brokerName;
        
        public MQConsumerCfg(String name, String clusterName, String brokerName) {
            this.name = name;
            this.clusterName = clusterName;
            this.brokerName = brokerName;
        }
        
        public String clusterName() {
            return clusterName;
        }
        
        public String brokerName() {
            return brokerName;
        }
        
        public String name() {
            return name;
        }
        
    }
    
}
