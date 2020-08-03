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

import static org.nopasserby.fastminimq.MQConstants.MQBroker.MAX_BATCH_READ_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.MIN_RESUME_LENGTH;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentSkipListMap;

import org.nopasserby.fastminimq.ConsumeQueue.Mappable;
import org.nopasserby.fastminimq.ConsumeQueue.MappedBuffer;
import org.nopasserby.fastminimq.log.SegmentLog;
import org.nopasserby.fastminimq.log.SegmentLogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;

public class MQNodeLog implements Mappable {

    static Logger logger = LoggerFactory.getLogger(MQNodeLog.class);
    
    static final int SECONDS_IN_DAY = 60 * 60 * 24;
    static final long MILLIS_IN_DAY = 1000L * SECONDS_IN_DAY;
    static final int RAW_OFFSET = TimeZone.getDefault().getRawOffset();
    
    private NavigableMap<Long, SegmentLog> segmentLogMap = new ConcurrentSkipListMap<Long, SegmentLog>();
    
    private SegmentLog segmentLog;
    
    private long millisOfLastDay; 
    
    private volatile long writeOffset;
    
    private long readOffset;
    
    private SegmentLogFactory segmentLogFactory;
    
    public MQNodeLog(SegmentLogFactory segmentLogFactory) throws Exception {
        this.segmentLogFactory = segmentLogFactory;
        this.init();
        this.expandCapacity();
    }

    private void init() throws Exception {
        SegmentLog[] segmentLogs = segmentLogFactory.reloadSegmentLog();
        
        for (SegmentLog segmentLog: segmentLogs) {
            Long baseOffset = segmentLog.baseOffset();
            if (this.writeOffset <= baseOffset) {
                this.writeOffset = baseOffset;
                this.segmentLog = segmentLog;
            }
            segmentLogMap.put(baseOffset, segmentLog);
        }
        if (segmentLog != null) {            
            this.writeOffset += segmentLog.writeOffset();
        }
    }

    public synchronized void write(ByteBuffer buffer) throws Exception {
        int remaining = buffer.remaining();
        ensureCapacity(remaining);
        segmentLog.write(buffer);
        writeOffset += remaining;
    }
    
    public synchronized void write(byte[] src, int srcPos, int len) throws Exception {
        ensureCapacity(len);
        segmentLog.write(src, srcPos, len);
        writeOffset += len;
    }
    
    public void truncate(long writeOffset) throws Exception {
        if (this.writeOffset == writeOffset) {
            return;
        }
        
        Entry<Long, SegmentLog> segmentLogEntry = segmentLogMap.floorEntry(writeOffset);
        if (segmentLogEntry == null) {
            return;
        }
        Long baseOffset = segmentLogEntry.getKey();
        SegmentLog segmentLog = segmentLogEntry.getValue();
        long relativeOffset = writeOffset - baseOffset;
        segmentLog.truncate(relativeOffset);
        
        Set<Entry<Long, SegmentLog>> segmentLogEntrySet = segmentLogMap.entrySet();
        for (Entry<Long, SegmentLog> segment: segmentLogEntrySet) {
            Long key = segment.getKey();
            if (key > baseOffset) {
                segmentLog = segment.getValue();
                segmentLogMap.remove(key);
                segmentLog.delete();
            }
        }
        this.writeOffset = writeOffset;
        
        this.segmentLog = segmentLogMap.lastEntry().getValue();
    }
    
    public int read(ByteBuffer dst) throws Exception {
        Entry<Long, SegmentLog> segmentLogEntry = segmentLogMap.floorEntry(readOffset);
        if (segmentLogEntry == null) {
            return 0;
        }
        SegmentLog segmentLog = segmentLogEntry.getValue();
        
        int length = segmentLog.read(dst);
        readOffset += length;
        
        if (dst.hasRemaining() && length > 0 && readOffset < writeOffset()) {
            length += read(dst);
        }
        return length;
    }
    
    int readFlip(ByteBuffer dst, long readOffset) throws Exception {
        int length = read(dst, readOffset);
        dst.flip();
        return length;
    }
    
    int read(ByteBuffer dst, long readOffset) throws Exception {
        Entry<Long, SegmentLog> segmentLogEntry = segmentLogMap.floorEntry(readOffset);
        if (segmentLogEntry == null) {
            return 0;
        }
        Long baseOffset = segmentLogEntry.getKey();
        SegmentLog segmentLog = segmentLogEntry.getValue();
        
        long relativeOffset = readOffset - baseOffset;
        int length = segmentLog.read(dst, relativeOffset);
        readOffset += length;
        
        if (dst.hasRemaining() && length > 0 && readOffset < writeOffset()) {
            length += read(dst, readOffset);
        }
        return length;
    }
    
    public SegmentLog[] segmentLog() {
        List<SegmentLog> list = new ArrayList<SegmentLog>(segmentLogMap.values());
        return list.toArray(new SegmentLog[list.size()]);
    }
    
    public long startOffset() {
        return segmentLogMap.isEmpty() ? 0 : segmentLogMap.firstKey();
    }
    
    public long readOffset() {
        return readOffset;
    }
    
    public long writeOffset() {
        return writeOffset;
    }

    public void flush() throws Exception {
        if (segmentLog != null) {            
            segmentLog.flush();
        }
    }
    
    public void sync() throws Exception {
        if (segmentLog != null) {            
            segmentLog.sync();
        }
    }
    
    public void close() throws Exception {
        for (SegmentLog segmentLog: segmentLogMap.values()) {
            segmentLog.close();
        }
    }

    public long nextAvailableOffset(long offset) {
        // skip to next available segment 
        Long nextAvailableOffset = segmentLogMap.ceilingKey(offset);
        if (nextAvailableOffset == null) {            
            throw new RuntimeException("no next available offset exists");
        }
        return nextAvailableOffset;
    }
    
    public long nearestAvailableOffset(long offset) {
        Long nearestAvailableOffset = segmentLogMap.floorKey(offset);
        return nearestAvailableOffset == null ? 0 : nearestAvailableOffset;
    }
    
    public long resumeOffset() {
        long startOffset = writeOffset() - MIN_RESUME_LENGTH;
        long resumeOffset = nearestAvailableOffset(startOffset);
        return resumeOffset;
    }
    
    public void delete() throws Exception {
        if (segmentLog != null) {
            segmentLog.delete();
        }
        for (SegmentLog segmentLog: segmentLogMap.values()) {
            segmentLog.delete();
        }
        segmentLogMap.clear();
    }
    
    public void deleteSegmentLog(long startAvailableOffset) throws Exception {
        if (segmentLogMap.isEmpty()) {
            return;
        }
        for (SegmentLog segmentLog: segmentLogMap.values()) {
            if (startAvailableOffset >= segmentLog.writeOffset() && this.segmentLog != segmentLog) {
                segmentLogMap.remove(segmentLog.baseOffset());
                segmentLog.delete();
            }
        }
    }
    
    public void ensureCapacity(int minRemainingCapacity) throws Exception {
        if (segmentLog == null || segmentLog.remainingCapacity() < minRemainingCapacity || isDateExpired()) {
            this.flush();
            this.expandCapacity();
        }
    }
    
    // archive logs by day
    // Does delay caused by new file cause TPS jitter ?
    private boolean isDateExpired() {
        return System.currentTimeMillis() - millisOfLastDay > MILLIS_IN_DAY; 
    }
    
    private void expandCapacity() throws Exception {
        long baseOffset = writeOffset();
        if (!segmentLogMap.containsKey(baseOffset)) {
            segmentLog = segmentLogFactory.createSegmentLog(baseOffset);
            segmentLogMap.put(baseOffset, segmentLog);
            millisOfLastDay = System.currentTimeMillis() / MILLIS_IN_DAY * MILLIS_IN_DAY - RAW_OFFSET + MILLIS_IN_DAY;
        }
        logger.debug("expand capacity {} bytes", segmentLog.capacity());
    }
    
    FastThreadLocal<ImmutableMappedBuffer> immutableMappedBufferLocal = new FastThreadLocal<ImmutableMappedBuffer>() {
        @Override
        protected ImmutableMappedBuffer initialValue() throws Exception {
            return new ImmutableMappedBuffer();
        }
    };
    
    public MappedBuffer mapped(long startOffset, long endOffset) throws Exception {
        ImmutableMappedBuffer mappedBuffer = immutableMappedBufferLocal.get();
        ByteBuffer buffer = mappedBuffer.buffer;
        long length = endOffset - startOffset;
        int limit = (int) (length < MAX_BATCH_READ_LENGTH ? length : MAX_BATCH_READ_LENGTH);
        buffer.limit(limit);
        read(buffer, startOffset);
        buffer.flip();
        return mappedBuffer;
    }
    
    @Override
    public MappedBuffer mapped(long offset) throws Exception {
        return mapped(offset, offset + MAX_BATCH_READ_LENGTH);
    }
    
    public class ImmutableMappedBuffer implements MappedBuffer {
        ByteBuffer buffer = ByteBuffer.allocate(MAX_BATCH_READ_LENGTH);
        long position;
        
        @Override
        public Integer getInteger(long position) {
            if (checkAvailable(position + 4)) {
                return buffer.getInt((int) position);
            }
            return null;
        }
        
        @Override
        public void release() {
            buffer.clear();
            position = 0;
        }
        
        boolean checkAvailable(long position) {
            return buffer.limit() >= position;
        }

        @Override
        public int read(ByteBuffer buffer, long position, int length) {
            if (!checkAvailable(position + length)) {
                return 0;
            }
            this.buffer.position((int) position);
            length = Math.min(buffer.remaining(), length);
            int pos = buffer.position();
            int newPos = pos + length;
            this.buffer.get(buffer.array(), pos, length);
            buffer.position(newPos);
            return length;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public void position(long position) {
            this.position = position;
        }

    }

}
