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

import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_RETENTION_CHECK_INTERVAL_MILLIS;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_RETENTION_MILLIS;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.MAX_RECORD_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_LENGTH_SELF_LENGTH;
import static org.nopasserby.fastminimq.MQUtil.currentTimeMillis;
import static org.nopasserby.fastminimq.MQUtil.startThread;

import java.nio.ByteBuffer;

import org.nopasserby.fastminimq.log.SegmentLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumeQueue implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ConsumeQueue.class);
    
    private MQNodeLog commitLog;
    
    private MQStorage storage;
    
    private MQRingBuffer buffer;
    
    private ConsumeQueueIndex consumeQueueIndex;
    
    private volatile long offset;
    
    private volatile boolean shutdown;
    
    public ConsumeQueue(MQStorage storage) throws Exception {
        this.storage = storage;
        this.commitLog = storage.getCommitLog();
        this.buffer = storage.getBuffer();
        this.consumeQueueIndex = new ConsumeQueueIndex();
        this.init();
    }
    
    private void init() throws Exception {
        offset = consumeQueueIndex.resumeCheckPoint(commitLog.resumeOffset());
        long lastOffset = commitLog.writeOffset();
        while (lastOffset != offset) {
            buildIndexMakeup(commitLog);
        }
        storage.getKVdb().recover(consumeQueueIndex.getKVdb());
    }
    
    protected void buildIndexMakeup(Mappable mappable) {
        try {
            long lastOffset = offset;
            buildIndexDoWork0(mappable);
            if (lastOffset == offset) {
                throw new IllegalStateException(
                        "commit log unable to build index, last index offset is " + offset + ", commit offset is " + commitLog.writeOffset());
            }
        } catch (Exception e) {
            logger.warn("build index error, "
                    + "which may be caused by restarting after abnormal exit, "
                    + "build index work will start from the next available offset", e);
            // skip to next available segment
            long offset = this.offset;
            this.offset = this.commitLog.nextAvailableOffset(offset);
            logger.warn("build index skip offset {}->{}", offset, this.offset);
        }
    }
    
    @Override
    public void run() {
        startThread(buildIndexWork, "MQ-BROKER-CONSUMEQUEUE-BUILDINDEX");
        startThread(deleteExpiredWork, "MQ-BROKER-DELETE-EXPIRED-LOG");
        startThread(consumeQueueIndex, "MQ-BROKER-CONSUMEQUEUE-INDEX");
    }
    
    boolean isShutdown() {
        return shutdown;
    }
    
    public void shutdown() throws Exception {
        shutdown = true;
        consumeQueueIndex.shutdown();
        
        synchronized (deleteExpiredWork) {
            deleteExpiredWork.notify();
        }
    }
    
    private Runnable buildIndexWork = new Runnable() {
        
        @Override
        public void run() {
            while (!isShutdown()) {
                buildIndexDoWork(buffer);
            }
            logger.info("consume queue index build has stopped.");
        }

    };
    
    private Runnable deleteExpiredWork = new Runnable() {
        
        @Override
        public void run() {
            while (!isShutdown()) {
                deleteExpiredDoWork();
            }
            logger.info("delete expired log has stopped.");
        }
        
    };
    
    protected void deleteExpiredDoWork() {
        try {
            deleteExpiredDoWork0();
            
            synchronized (deleteExpiredWork) {
                deleteExpiredWork.wait(DATA_RETENTION_CHECK_INTERVAL_MILLIS);
            }
        } catch (Exception e) {
            logger.error("delete expired error", e);
        }
    }

    private void deleteExpiredDoWork0() throws Exception {
        long startTimestamp = currentTimeMillis() - DATA_RETENTION_MILLIS;
        SegmentLog[] segmentLogArray = commitLog.segmentLog();
        long startAvailableOffset = 0;
        for (SegmentLog segmentLog: segmentLogArray) {
            long createTimestamp = segmentLog.createTimestamp();
            if (createTimestamp < startTimestamp) {
                long baseOffset = segmentLog.baseOffset();
                if (startAvailableOffset < baseOffset) {
                    startAvailableOffset = baseOffset;
                }
            }
        }
        commitLog.deleteSegmentLog(startAvailableOffset);
        consumeQueueIndex.deleteExpiredIndex(startAvailableOffset);
    }

    protected void buildIndexDoWork(Mappable mappable) {
        try {
            buildIndexDoWork0(mappable);
        } catch (Exception e) {
            logger.error("build index error at " + offset + " offset ", e);
        }
    }
    
    private void buildIndexDoWork0(Mappable mappable) throws Exception {
        MappedBuffer buffer = mappable.mapped(offset);
        if (buffer == null) {
            waitFor();
            return;
        }
        
        buildBatchIndex(buffer);
        
        buffer.release();
    }
    
    protected void waitFor() throws Exception {
        Thread.sleep(1);
    }
    
    private ByteBuffer recordData = ByteBuffer.allocate(MAX_RECORD_LENGTH);
    
    protected void buildBatchIndex(MappedBuffer buffer) throws Exception {
        while (!isShutdown()) {
            long position = buffer.position();
            Integer length = buffer.getInteger(position);
            if (length == null || length == 0) {
                break;
            }
            
            int recordLength = REOCRD_LENGTH_SELF_LENGTH + length;
            
            long nextOffset = offset + (REOCRD_LENGTH_SELF_LENGTH + length);
            if (buffer.read(recordData, position, recordLength) == 0) {
                break;
            }
            try {
                recordData.flip();
                consumeQueueIndex.createIndex(offset, recordData);
            } catch (Throwable e) {
                logger.error("offset:" + offset + " build index exception, skip to next index offset:" + nextOffset, e);
            } finally {
                recordData.clear();            
            }
            
            buffer.position(position + recordLength);
            this.offset = nextOffset;
            this.buffer.setReadIndex(offset);
            
            consumeQueueIndex.clearInvalidCache(this.buffer.startAvailableReadIndex());
        }
    }
    
    public ConsumeQueueIndex getConsumeQueueIndex() {
        return this.consumeQueueIndex;
    }

    public MQNodeLog getCommitLog() {
        return this.commitLog;
    }
    
    public MQRingBuffer getBuffer() {
        return this.buffer;
    }
    
    public interface Mappable {
        MappedBuffer mapped(long offset) throws Exception;
    }
    
    public interface MappedBuffer {
        
        public Integer getInteger(long position);
        
        public int read(ByteBuffer buffer, long position, int length);
        
        public void release();
        
        long position();
        
        public void position(long newPosition);
        
    }

}
