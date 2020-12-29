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

import static org.nopasserby.fastminimq.MQConstants.MQBroker.CLIENT_DECODE_MAX_FRAME_LENGTH;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_CODE_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_RES_CONTENT_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.CONSUME;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_LENGTH_SELF_LENGTH;
import static org.nopasserby.fastminimq.MQUtil.currentTimeMillis;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.nopasserby.fastminimq.ConsumeQueue.MappedBuffer;
import org.nopasserby.fastminimq.ConsumeQueueIndex.QueryIndex;
import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQExecutor.ChannelDelegate;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;

public class ConsumeProcessor {
    
    private ConsumeQueueIndex consumeQueueIndex;
    
    private MQNodeLog nodeLog;
    
    private MQRingBuffer buffer;
    
    public ConsumeProcessor(ConsumeQueue consumeQueue) throws IOException {
        this.consumeQueueIndex = consumeQueue.getConsumeQueueIndex();
        this.nodeLog = consumeQueue.getCommitLog();
        this.buffer = consumeQueue.getBuffer();
    }
    
    private static final int COMMAND_DATA_NEXT_INDEX_LENGTH = 8;
    
    private static final int COMMAND_DATA_RECORDS_OFFSET = COMMAND_DATA_RES_CONTENT_OFFSET + COMMAND_DATA_NEXT_INDEX_LENGTH;
    
    private FastThreadLocal<ByteBuffer> outLocal = new FastThreadLocal<ByteBuffer>() {
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(CLIENT_DECODE_MAX_FRAME_LENGTH);
        }
    };
    
    public ByteBuffer takeOut() {
        ByteBuffer out = outLocal.get();
        out.position(COMMAND_DATA_RECORDS_OFFSET);
        return out;
    }
    
    private String takeTopic(ByteBuf commandData) {
        short topicLength = commandData.readShort();
        byte[] topic = new byte[topicLength];
        commandData.readBytes(topic);
        return new String(topic);
    }
    
    public void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception {
        String topic = takeTopic(commandData);
        long startIndex = commandData.readLong(); // start index
        int rows = commandData.readInt();         // rows
        long beginMillis = commandData.readLong();// begin time stamp (unit milliseconds)
        long delayMillis = commandData.readLong();// delay (unit milliseconds)
        commandData.release();
        
        ByteBuffer out = takeOut();
        long nextIndex = next(topic, startIndex, rows, beginMillis, delayMillis, out);
        
        channel.writeAndFlush(buildAck(out, commandId, nextIndex));
    }
    
    private ByteBuf buildAck(ByteBuffer out, long id, long nextIndex) {
        int endPos = out.position();
        int length = endPos - COMMAND_DATA_OFFSET;
        out.position(COMMAND_CODE_OFFSET);
        out.putInt(CONSUME);
        out.putLong(id);
        out.putInt(length);
        out.putInt(Status.OK.ordinal());
        out.putLong(nextIndex);
        out.position(endPos);
        out.flip();
        ByteBuf bytebuf = Unpooled.copiedBuffer(out);
        out.clear();
        return bytebuf;
    }
    
    public long next(String topic, long index, int rows, long beginMillis, long delayMillis, ByteBuffer out) throws Exception {
        QueryIndex queryIndex = consumeQueueIndex.queryIndex(topic, index);
        
        if (!queryIndex.hasAvailableIndex()) {
            long nextIndex = queryIndex.nextAvailableIndex(index);
            queryIndex.release();
            return nextIndex;
        }
                
        long startOffset = queryIndex.startRecordOffset();
        
        MappedBuffer buffer = this.buffer.mapped(startOffset);
        if (buffer == null) {
            long endOffset = queryIndex.endRecordOffset() + queryIndex.endRecordLength();
            buffer = this.nodeLog.mapped(startOffset, endOffset);
        }
        
        if (buffer == null) {
            long nextIndex = queryIndex.nextAvailableIndex(index);
            queryIndex.release();
            return nextIndex;
        }
        
        long startPos = buffer.position();
        int rowsRead = 0;
        while (rowsRead < rows && queryIndex.next()) {
            long recordOffset = queryIndex.currentRecordOffset();
            int recordLength = queryIndex.currentRecordLength();
            long recordTimestamp = queryIndex.currentRecordTimestamp();
            
            if (beginMillis > 0 && beginMillis > recordTimestamp) {
                rowsRead++;
                continue;
            }
            
            if (delayMillis > 0 && recordTimestamp + delayMillis > currentTimeMillis()) {
                break;
            }
            
            long relativePos = startPos + recordOffset - startOffset;
            Integer length = buffer.getInteger(relativePos);
            if (length == null) {
                break;
            }
            
            if (REOCRD_LENGTH_SELF_LENGTH + length != recordLength) {
                throw new IllegalAccessException("offset:" + recordOffset + " record index error," 
                        + " expected record length is " + recordLength 
                        + " but actual record length is " + (REOCRD_LENGTH_SELF_LENGTH + length));
            }
            
            if (out.remaining() < recordLength) {
                break;
            }
            
            if (buffer.read(out, relativePos, recordLength) == 0) {
                break;
            }
            
            rowsRead++;
        }
        
        long nextIndex = queryIndex.nextAvailableIndex(index + rowsRead);
        
        queryIndex.release();
        buffer.release();
        
        return nextIndex;
    }
  
}