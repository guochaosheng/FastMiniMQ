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

import static java.io.File.separator;
import static org.nopasserby.fastminimq.MQConstants.LogType.commit;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BYTES_RINGBUFFER_BLOCKUNIT;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.CLIENT_DECODE_MAX_FRAME_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_DIR;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BYTES_BATCH_SIZE;
import static org.nopasserby.fastminimq.ReocrdHelper.createNonTransactionRecord;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.MQConsumer.MQConsumerCfg;
import org.nopasserby.fastminimq.MQExecutor.ChannelDelegate;
import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;
import org.nopasserby.fastminimq.MQResult.MQRecord;
import org.nopasserby.fastminimq.log.RandomBufferSegmentLogFactory;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ConsumeProcessorTest {

    @Test
    public void nextTest() throws Exception {
        MQKVdb kvdb = new MQKVdb();
        MQNodeLog commitLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + commit));
        MQRingBuffer ringBuffer = new MQRingBuffer(1, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, false);
        MQStorage storage = new MQStorage(kvdb, 1, ringBuffer, commitLog);
        ConsumeQueue consumeQueue = new ConsumeQueue(storage);
        
        String topic = "testTopic";
        int batch = 1024 * 128;
        for (int index = 0; index < batch; index++) {
            ByteBuffer reocrdData = createNonTransactionRecord(topic, index);
            ringBuffer.writeBytes(reocrdData);
        }
        ringBuffer.flush();
        
        consumeQueue.buildIndexDoWork(ringBuffer);
        
        ConsumeProcessor consumeProcessor = new ConsumeProcessor(consumeQueue);
        ByteBuffer buffer = ByteBuffer.allocate(CLIENT_DECODE_MAX_FRAME_LENGTH);
        long nextIndex = 0;
        MQConsumerCfg consumerCfg = new MQConsumerCfg("testConsumer", "testCluster", "testBroker");
        MQConsumer consumer = new MQConsumer(consumerCfg, new MQClusterMetaData(null));
        int index = 0;
        while (index < batch) {
            nextIndex = consumeProcessor.next(topic, nextIndex, 200, 0, 0, buffer);
            buffer.flip();
            List<MQRecord> recordList = consumer.decodeRecordList(buffer);
            for (MQRecord record:recordList) {
                String body = new String(record.getBody());
                
                Assert.assertEquals(index, Integer.parseInt(body.substring(body.length() - 10)));
                
                index++;
            }
            buffer.clear();
        }
        
        consumeQueue.getBuffer().release();
        consumeQueue.getCommitLog().delete();
        consumeQueue.getConsumeQueueIndex().delete();
    }
    
    @Test
    public void dispatchTest() throws Exception {
        MQKVdb kvdb = new MQKVdb();
        MQNodeLog commitLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + commit));
        MQRingBuffer ringBuffer = new MQRingBuffer(1, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, false);
        MQStorage storage = new MQStorage(kvdb, 1, ringBuffer, commitLog);
        ConsumeQueue consumeQueue = new ConsumeQueue(storage);
        String topic = "testTopic";
        int marker = 88889999;
        ByteBuffer reocrdData = createNonTransactionRecord(topic, marker);
        ringBuffer.writeBytes(reocrdData);
        ringBuffer.flush();
        consumeQueue.buildIndexDoWork(ringBuffer);
        
        ConsumeProcessor consumeProcessor = new ConsumeProcessor(consumeQueue);
        
        ByteBuffer commandData = ByteBuffer.allocate(256);
        commandData.putShort((short) topic.getBytes().length); // put topic length
        commandData.put(topic.getBytes());                     // put topic
        commandData.putLong(0);                                // put next index
        commandData.putInt(200);                               // put rows
        commandData.putLong(0);                                // put begin time stamp (unit milliseconds)
        commandData.putLong(0);                                // put delay (unit milliseconds)
        commandData.flip();
        
        MQConsumerCfg consumerCfg = new MQConsumerCfg("testConsumer", "testCluster", "testBroker");
        MQConsumer consumer = new MQConsumer(consumerCfg, new MQClusterMetaData(null));
        
        consumeProcessor.dispatch(new ChannelDelegate(null) {
            @Override
            public void writeAndFlush(ByteBuf command) {
                command.readInt(); // commandCode
                command.readLong(); // command id
                command.readInt(); // command length
                command.readInt(); // status
                
                long nextIndex = command.readLong();
                Assert.assertEquals(1, nextIndex);
                
                MQRecord record = consumer.decodeRecordList(command.nioBuffer()).get(0);
                String body = new String(record.getBody());
                
                Assert.assertEquals(marker, Integer.parseInt(body.substring(body.length() - 10)));
            }
        }, 0, 0L, Unpooled.wrappedBuffer(commandData));
        
        consumeQueue.getBuffer().release();
        consumeQueue.getCommitLog().delete();
        consumeQueue.getConsumeQueueIndex().delete();
    }
    
}
