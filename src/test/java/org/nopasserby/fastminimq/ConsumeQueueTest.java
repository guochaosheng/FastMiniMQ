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
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_DIR;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BYTES_BATCH_SIZE;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.MAX_RECORD_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_LENGTH_SELF_LENGTH;
import static org.nopasserby.fastminimq.ReocrdHelper.createNonTransactionRecord;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.ConsumeQueue.MappedBuffer;
import org.nopasserby.fastminimq.ConsumeQueueIndex.QueryIndex;
import org.nopasserby.fastminimq.log.RandomBufferSegmentLogFactory;

public class ConsumeQueueTest {

    @Test
    public void buildBatchIndexTest() throws Exception {
        MQKVdb kvdb = new MQKVdb();
        MQNodeLog commitLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + commit));
        MQRingBuffer ringBuffer = new MQRingBuffer(1, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, false);
        MQStorage storage = new MQStorage(kvdb, 1024, ringBuffer, commitLog);
        ConsumeQueue consumeQueue = new ConsumeQueue(storage);
        
        String topic = "testTopic";
        int batch = 1024 * 128;
        for (int index = 0; index < batch; index++) {
            ByteBuffer reocrdData = createNonTransactionRecord(topic, index);
            ringBuffer.writeBytes(reocrdData);
        }
        ringBuffer.flush();
        
        consumeQueue.buildIndexDoWork(ringBuffer);
        
        ConsumeQueueIndex consumeQueueIndex = consumeQueue.getConsumeQueueIndex();
        QueryIndex queryIndex = consumeQueueIndex.queryIndex(topic, 0);
        
        MappedBuffer mapped = ringBuffer.mapped(0);
        int index = 0;
        while (queryIndex.next()) {
            long recordOffset = queryIndex.currentRecordOffset();
            int recordLength = queryIndex.currentRecordLength();
            
            Integer length = mapped.getInteger(recordOffset);
            
            Assert.assertEquals(recordLength, REOCRD_LENGTH_SELF_LENGTH + length);
            
            ByteBuffer recordData = ByteBuffer.allocate(MAX_RECORD_LENGTH);
            
            if (mapped.read(recordData, recordOffset, recordLength) == 0) {
                break;
            }
            recordData.flip();
            
            Assert.assertEquals(index, ReocrdHelper.decodeReocrdIndex(recordData));
           
            index++;
        }
        
        Assert.assertEquals(batch, index);
        
        consumeQueueIndex.delete();
        consumeQueue.getCommitLog().delete();
        consumeQueue.getBuffer().release();
    }
    
    @Test
    public void buildIndexMakeupTest() throws Exception {
        MQKVdb kvdb = new MQKVdb();
        MQNodeLog commitLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + separator + commit));
        MQRingBuffer ringBuffer = new MQRingBuffer(1, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, false);
        MQStorage storage = new MQStorage(kvdb, 1024, ringBuffer, commitLog);
        
        String topic = "testTopic";
        MQNodeLog nodeLog = storage.getCommitLog();
        int batch = 1024 * 128;
        for (int index = 0; index < batch; index++) {
            ByteBuffer reocrdData = createNonTransactionRecord(topic, index);
            nodeLog.write(reocrdData);
        }
        nodeLog.flush();
        
        ConsumeQueue consumeQueue = new ConsumeQueue(storage);
        
        ConsumeQueueIndex consumeQueueIndex = consumeQueue.getConsumeQueueIndex();
 
        for (int index = 0; index < batch;) {
            QueryIndex queryIndex = consumeQueueIndex.queryIndex(topic, index);
            long startOffset = queryIndex.startRecordOffset();
            
            MappedBuffer buffer = nodeLog.mapped(startOffset);
            long startPos = buffer.position();
            
            while (queryIndex.next()) {
                long recordOffset = queryIndex.currentRecordOffset();
                int recordLength = queryIndex.currentRecordLength();
                
                long relativePos = startPos + recordOffset - startOffset;
                Integer length = buffer.getInteger(relativePos);
                if (length == null) {
                    break;
                }
                
                Assert.assertEquals(recordLength, REOCRD_LENGTH_SELF_LENGTH + length);
                
                ByteBuffer recordData = ByteBuffer.allocate(MAX_RECORD_LENGTH);
                
                if (buffer.read(recordData, relativePos, recordLength) == 0) {
                    break;
                }
                recordData.flip();
                
                Assert.assertEquals(index, ReocrdHelper.decodeReocrdIndex(recordData));
                
                index++;
            }
            
            buffer.release();
        }
        
        consumeQueueIndex.delete();
        consumeQueue.getCommitLog().delete();
        consumeQueue.getBuffer().release();
    }
    
}
