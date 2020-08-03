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

import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;
import org.nopasserby.fastminimq.ConsumeQueue.Mappable;
import org.nopasserby.fastminimq.ConsumeQueue.MappedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;

public class MQRingBuffer implements Mappable {
    
    private static Logger logger = LoggerFactory.getLogger(MQRingBuffer.class);
    
    private long baseOffset;
    
    private volatile long lastBatchOffset;
    
    private volatile long syncOffset;
    
    private boolean flushSync;
    
    private int batchCache;
    
    private int blockUnitSize;
    
    public int blockSize;
    
    private long bufferSize;
    
    private byte[][] buffers;
    
    protected volatile long writeIndex = -1L;
    
    protected volatile long readIndex = -1L;
    
    protected volatile long cachedValue = -1L;
    
    private final MQNodeLog nodeLog;
    
    public MQRingBuffer(final int blockSize, final int blockUnitSize, final MQNodeLog nodeLog, final int batchCache, boolean flushSync) {
        ensureAvailable("blockSize", blockSize);
        ensureAvailable("blockUnitSize", blockUnitSize);
        this.blockSize = blockSize;
        this.blockUnitSize = blockUnitSize;
        this.bufferSize = (long) blockSize * (long) blockUnitSize;
        this.flushSync = flushSync;
        
        this.nodeLog = nodeLog;
        this.baseOffset = nodeLog.writeOffset();
        this.batchCache = batchCache;
        
        buffers = new byte[blockSize][];
        for (int i = 0; i < blockSize; i++) {
            buffers[i] = new byte[blockUnitSize];
        }
    }

    private void ensureAvailable(String argName, long argValue) {
        if (argValue < 1)  {
            throw new IllegalArgumentException(argName + " must not be less than 1");
        }
        if (Long.bitCount(argValue) != 1) {
            throw new IllegalArgumentException(argName + " must be a power of 2");
        }
    }
    
    public void flush() throws Exception {
        if (nodeLog != null) {
            long writeOffset = nodeLog.writeOffset();
            long writeIndex = writeIndex();
            int cache = (int) (writeIndex - (writeOffset - baseOffset - 1));
            if (cache > 0) {
                writeBytes(nodeLog, writeOffset - baseOffset, cache);
            }
            nodeLog.flush();
            if (flushSync) {
                nodeLog.sync();
                syncOffset = nodeLog.writeOffset();
            }
        }
    }

    public void writeBytes(ByteBuffer src) throws Exception {
        int length = src.remaining();
        writeBytes(src.array(), src.position(), length);
        if (nodeLog != null) {
            long writeOffset = nodeLog.writeOffset();
            nodeLog.ensureCapacity(length);
            int cache = (int) (writeIndex() - (writeOffset - baseOffset - 1));
            if (cache > batchCache) {
                writeBytes(nodeLog, writeOffset - baseOffset, cache);
            }
        }
    }
    
    public void writeBytes(MQNodeLog nodeLog, long writeIndex, int length) throws Exception {
        int bufferIndex = bufferIndex(writeIndex);
        int bufferPos = bufferPos(writeIndex);
        while (length > 0) {
            byte[] dest = buffers[bufferIndex];
            int localLength = Math.min(length, dest.length - bufferPos);
            nodeLog.write(dest, bufferPos, localLength);
            lastBatchOffset = nodeLog.writeOffset();
            bufferPos += localLength;
            length -= localLength;
            if (bufferPos == dest.length) {
                bufferIndex = (bufferIndex + 1) & (buffers.length - 1);// next buffer index
                bufferPos = 0;
            }
        }
    }
    
    public void writeBytes(byte[] src, int srcIndex, int length) {
        long current = writeIndex;
        long nextIndex = current + length;
        
        long wrapPoint = nextIndex - bufferSize;
        long cachedValue = this.cachedValue;

        if (wrapPoint > cachedValue || cachedValue > current) {
            long minValue;
            while (wrapPoint > (minValue = Math.min(readIndex, current))) {
                LockSupport.parkNanos(1L);
            }

            this.cachedValue = minValue;
        }
        
        long startIndex = current + 1;
        int bufferIndex = bufferIndex(startIndex);
        int bufferPos = bufferPos(startIndex);
        while (length > 0) {
            byte[] dest = buffers[bufferIndex];
            int localLength = Math.min(length, dest.length - bufferPos);
            System.arraycopy(src, srcIndex, dest, bufferPos, localLength);
            bufferPos += localLength;
            srcIndex += localLength;
            length -= localLength;
            if (bufferPos == dest.length) {
                bufferIndex = (bufferIndex + 1) & (buffers.length - 1);// next buffer index
                bufferPos = 0;
            }
        }
       
        writeIndex = nextIndex;
    }
    
    public long readIndex() {
        return readIndex;
    }
    
    public void setReadIndex(long readIndex) {
        this.readIndex = readIndex - baseOffset;
    }
    
    public long startAvailableReadIndex() {
        return writeIndex - bufferSize + baseOffset;
    }
    
    public long writeIndex() {
        return writeIndex;
    }
    
    public void setWriteIndex(long writeIndex) {
        this.writeIndex = writeIndex - baseOffset;
    }
    
    public boolean checkAvailable(long index) {
        if (hasNodeLog() && flushSync && index >= syncOffset) {
            return false;
        }
        if (hasNodeLog() && index >= lastBatchOffset) {
            return false;
        }
        long relativeIndex = index - baseOffset;
        if (relativeIndex < 0) {
            return false;
        }
        long writeIndex = writeIndex();
        if (relativeIndex > writeIndex) {
            return false;
        }
        boolean available = writeIndex - bufferSize < relativeIndex;
        if (!available) {
            logger.info("index {} is invalid, current write index value is {} and base offset is {}", index, writeIndex, baseOffset);
        }
        return available;
    }
    
    FastThreadLocal<DynamicMappedBuffer> dynamicMappedBufferLocal = new FastThreadLocal<DynamicMappedBuffer>() {
        @Override
        protected DynamicMappedBuffer initialValue() throws Exception {
            return new DynamicMappedBuffer(baseOffset, buffers);
        }
    };
    
    public boolean hasNodeLog() {
        return nodeLog != null;
    }
    
    public MQNodeLog getNodeLog() {
        return nodeLog;
    }
    
    @Override
    public MappedBuffer mapped(long offset) {
        if (!checkAvailable(offset)) {
            return null;
        }
        DynamicMappedBuffer mappedRingBuffer = dynamicMappedBufferLocal.get();
        mappedRingBuffer.position(offset - baseOffset);
        return mappedRingBuffer;
    }

    public void release() {
        buffers = null;
        writeIndex = -1L;
        readIndex = -1L;
        cachedValue = -1L;
    }
    
    int bufferIndex(long index) {
        return (int) ((index / blockUnitSize) & (blockSize - 1));
    }
    
    int bufferPos(long index) {
        return (int) (index & (blockUnitSize - 1));
    }
    
    public class DynamicMappedBuffer implements MappedBuffer {
        long baseOffset;
        byte[][] buffers;
        long position;
        
        DynamicMappedBuffer(long baseOffset, byte[][] mappedBuffers) {
            this.baseOffset = baseOffset;
            this.buffers = mappedBuffers;
        }

        @Override
        public Integer getInteger(long position) {
            if (!checkAvailable(position + 4)) {
                return null;
            }
            
            int startBufferIndex = bufferIndex(position);
            int endBufferIndex = bufferIndex(position + 4);// buffer.length > 4
            if (startBufferIndex == endBufferIndex) {
                byte[] buffer = buffers[startBufferIndex];
                int bufferPos = bufferPos(position);
                int value = makeInt(buffer[bufferPos],
                                    buffer[bufferPos + 1],
                                    buffer[bufferPos + 2],
                                    buffer[bufferPos + 3]);
                if (checkAvailable(position)) {
                    return value;
                }
                return null;
            }
            
            byte[] startBuffer = buffers[startBufferIndex];
            byte[] endBuffer = buffers[endBufferIndex];
            int bufferPos = bufferPos(position);
            int remaining = (int) (startBuffer.length - bufferPos);
            int value;
            if (remaining == 1) {                
                value = makeInt(startBuffer[bufferPos],
                                endBuffer[0],
                                endBuffer[1],
                                endBuffer[2]);
            } else if (remaining == 2) {
                value = makeInt(startBuffer[bufferPos],
                                startBuffer[bufferPos + 1],
                                endBuffer[0],
                                endBuffer[1]);
            } else if (remaining == 3) {        
                value = makeInt(startBuffer[bufferPos],
                                startBuffer[bufferPos + 1],
                                startBuffer[bufferPos + 2],
                                endBuffer[0]);
            } else if (remaining == 4) {    
                value = makeInt(startBuffer[bufferPos],
                                startBuffer[bufferPos + 1],
                                startBuffer[bufferPos + 2],
                                startBuffer[bufferPos + 3]);
            } else {
                throw new RuntimeException();
            }
                
            if (checkAvailable(position)) {
                return value;
            }
            
            return null;
        }

        int makeInt(byte b3, byte b2, byte b1, byte b0) {
            return  (b3 << 24) |
                    ((b2 & 0xff) << 16) |
                    ((b1 & 0xff) <<  8) |
                    ((b0 & 0xff));
        }
        
        @Override
        public void release() {
            position = 0;
        }
        
        @Override
        public int read(ByteBuffer buffer, long position, int length) {
            int bufferIndex = bufferIndex(position);
            int bufferPos = bufferPos(position);
            
            int remaining = length;
            while (remaining > 0) {
                byte[] src = buffers[bufferIndex];
                int localLength = Math.min(remaining, src.length - bufferPos);
                buffer.put(src, bufferPos, localLength);
                bufferPos += localLength;
                remaining -= localLength;
                if (bufferPos == src.length) {
                    bufferIndex = (bufferIndex + 1) & (buffers.length - 1);// next buffer index
                    bufferPos = 0;
                }
            }
            
            if (checkAvailable(position)) {
                return length;
            }
            return 0;
        }
        
        private boolean checkAvailable(long position) {
            return MQRingBuffer.this.checkAvailable(baseOffset + position);
        }

        @Override
        public void position(long newPosition) {
            this.position = newPosition;
        }
        
        @Override
        public long position() {
            return position;
        }

    }
    
}
