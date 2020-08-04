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
import static org.nopasserby.fastminimq.MQConstants.DYNAMIC;
import static org.nopasserby.fastminimq.MQConstants.IMMUTABLE;
import static org.nopasserby.fastminimq.MQConstants.LogType.commit;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.EVENT_QUEUE_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_DIR;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BYTES_RINGBUFFER_BLOCKSIZE;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BYTES_RINGBUFFER_BLOCKUNIT;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.EVENT_BUFFER_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BYTES_BATCH_SIZE;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.SYNC_AFTER_FLUSH;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.MAX_BATCH_COUNT;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.PRODUCE;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.KV_PUT;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_BODY_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_HEAD_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_TIMESTAMP_OFFSET;
import static org.nopasserby.fastminimq.MQUtil.currentTimeMillis;
import static org.nopasserby.fastminimq.MQUtil.startThread;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQExecutor.ChannelDelegate;
import org.nopasserby.fastminimq.log.FileChannelSegmentLogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MQStorage implements Runnable {
    
    private static Logger logger = LoggerFactory.getLogger(MQStorage.class);
    
    private MQKVdb kvdb;
    
    private BlockingQueue<Event> eventQueue;
    
    private BlockingQueue<Event> freeQueue;
    
    private MQNodeLog commitLog;
    
    private MQRingBuffer buffer;
    
    private volatile boolean shutdown; 
    
    public MQStorage(MQKVdb kvdb) throws Exception {
        this.kvdb = kvdb;
        this.commitLog = new MQNodeLog(new FileChannelSegmentLogFactory(DATA_DIR + separator + commit));
        this.eventQueue = new ArrayBlockingQueue<Event>(EVENT_QUEUE_LENGTH);
        this.freeQueue = new ArrayBlockingQueue<Event>(EVENT_QUEUE_LENGTH);
        while (freeQueue.offer(new Event())) { }
        this.buffer = new MQRingBuffer(BYTES_RINGBUFFER_BLOCKSIZE, BYTES_RINGBUFFER_BLOCKUNIT, commitLog, BYTES_BATCH_SIZE, SYNC_AFTER_FLUSH);
    }
    
    public MQStorage(MQKVdb kvdb, int eventQueueLength, MQRingBuffer buffer, MQNodeLog commitLog) throws Exception {
        this.kvdb = kvdb;
        this.commitLog = commitLog;
        this.eventQueue = new ArrayBlockingQueue<Event>(eventQueueLength);
        this.freeQueue = new ArrayBlockingQueue<Event>(eventQueueLength);
        while (freeQueue.offer(new Event())) { }
        this.buffer = buffer;
    }
    
    public void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception {
        Event event = freeQueue.poll();
        if (event == null) {
            commandData.release();
            channel.writeAndFlush(buildOut(commandCode, commandId, Status.FAIL.ordinal()));
            return;
        }
        
        event.fetchData(channel, commandCode, commandId, commandData);
        eventQueue.put(event);
    }
    
    static short recordType(int commandCode) {
        switch (commandCode) {
            case PRODUCE: return IMMUTABLE;
            case KV_PUT: return DYNAMIC;
            default: throw new IllegalArgumentException("command[code:" + Integer.toHexString(commandCode) + "] not support.");
        }
    }
    
    static final class Event {
        ByteBuffer recordData = ByteBuffer.allocate(EVENT_BUFFER_LENGTH);
        ByteBuffer bak = recordData;
        long commandId;
        int commandCode;
        ChannelDelegate channel;
        Status status = Status.OK;
        
        private void fetchData(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) {
            int recordBodyLength = commandData.readableBytes();
            int recordDataLength = REOCRD_BODY_OFFSET + recordBodyLength;
            if (recordDataLength > recordData.capacity()) {
                recordData = ByteBuffer.allocate(recordDataLength);
            }
            recordData.clear();
            recordData.putInt(REOCRD_HEAD_LENGTH + recordBodyLength);      // record length
            recordData.putShort(recordType(commandCode));                  // record type
            recordData.limit(recordDataLength);
            commandData.readBytes(recordData.array(), REOCRD_BODY_OFFSET, recordBodyLength);
            recordData.position(recordDataLength);
            commandData.release();
            recordData.flip(); // ready to read
            this.commandCode = commandCode;
            this.commandId = commandId;
            this.channel = channel;
        }
        
        private void release() {
            recordData = bak;
            status = Status.OK;
        }
    }

    @Override
    public void run() {
        startThread(storageProcessor, "MQ-BROKER-STORAGE-EVENTPROCESSOR");
    }
    
    public void shutdown() {
        shutdown = true;
    }
    
    public boolean isShutdown() {
        return shutdown;
    }

    protected void processBatch(List<Event> batchEvent) throws Exception {
        for (Event event: batchEvent) {
            ByteBuffer recordData = event.recordData;
            
            createTimestamp(event);
            createDynamicIndex(event);
            
            buffer.writeBytes(recordData);
        }
        buffer.flush();
        
        completedBatch(batchEvent);
    }
    
    private void createTimestamp(Event event) {
        ByteBuffer recordData = event.recordData;
        recordData.putLong(REOCRD_TIMESTAMP_OFFSET, currentTimeMillis());
    }
    
    private void createDynamicIndex(Event event) {
        ByteBuffer recordData = event.recordData;
        if (kvdb.isDynamic(recordData)) {
            event.status = kvdb.add(recordData) ? Status.OK : Status.FAIL;
        }
    }

    public void completedBatch(List<Event> batchEvent) {
        Set<ChannelDelegate> channels = new HashSet<ChannelDelegate>();
        for (Event event: batchEvent) {
            completed(event);
            if (!channels.contains(event.channel)) {
                channels.add(event.channel);
            }
        }
        for (ChannelDelegate channel: channels) {
            channel.flush();
        }
    }
    
    public void completed(Event event) {
        ChannelDelegate channel = event.channel;
        long commandId = event.commandId;
        int commandCode = event.commandCode;
        
        channel.write(buildOut(commandCode, commandId, event.status.ordinal()));
        
        event.release();
    }
    
    private static final int COMMAND_LENGTH = 20;
    private static final int COMMAND_DATA_LENGTH = 4;
    
    public ByteBuf buildOut(int commandCode, long commandId, int status) {
        ByteBuffer command = ByteBuffer.allocate(COMMAND_LENGTH);
        command.putInt(commandCode);
        command.putLong(commandId);
        command.putInt(COMMAND_DATA_LENGTH);
        command.putInt(status);
        command.flip();
        return Unpooled.wrappedBuffer(command);
    }
    
    private Runnable storageProcessor = new Runnable() {

        public void storageProcessorWorking() throws Exception  {
            while (!isShutdown()) {
                eventHandling();
            }
        }
        
        @Override
        public void run() {
            try {
                storageProcessorWorking();
            } catch (Exception e) {
                logger.error("event processor error", e);
            }
        }
        
    };
    
    private List<Event> batchEvent = new ArrayList<Event>(MAX_BATCH_COUNT);
    
    private void eventHandling() throws Exception {
        Event event = eventQueue.take();
        batchEvent.add(event);
        eventQueue.drainTo(batchEvent, MAX_BATCH_COUNT - 1);
        processBatch(batchEvent);
        freeQueue.addAll(batchEvent);
        batchEvent.clear();
    }

    public MQKVdb getKVdb() {
        return kvdb;
    }
    
    public MQNodeLog getCommitLog() {
        return commitLog;
    }

    public MQRingBuffer getBuffer() {
        return this.buffer;
    }
    
}
