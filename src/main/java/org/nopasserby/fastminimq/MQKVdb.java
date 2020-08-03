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

import static org.nopasserby.fastminimq.MQConstants.DYNAMIC;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.KV_GET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.KV_DEL;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.KV_DB_SIZE;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_BODY_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_OFFSET;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.nopasserby.fastminimq.MQConstants.Status;
import org.nopasserby.fastminimq.MQExecutor.ChannelDelegate;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

public class MQKVdb {

    private static byte[] valueEmpty = new byte[0];
    
    private int spaceLimit = KV_DB_SIZE;
    
    private AtomicInteger space = new AtomicInteger();
    
    private Map<String, byte[]> table = new ConcurrentHashMap<String, byte[]>();
    
    public void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception {
        switch (commandCode) {
            case KV_GET: {
                dispatchGet(channel, commandCode, commandId, commandData); 
                break;
            }
            case KV_DEL: {
                dispatchDelete(channel, commandCode, commandId, commandData); 
                break;
            }
            default: throw new IllegalArgumentException("command[code:" + Integer.toHexString(commandCode) + "] not support.");
        }
    };
    
    public void dispatchGet(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) {
        String key = ByteBufUtil.hexDump(commandData);
        commandData.release();
        byte[] value = table.get(key);
        value = value == null ? valueEmpty : value;
        channel.writeAndFlush(buildAck(commandCode, commandId, Status.OK.ordinal(), value));
    }
    
    public Map<String, byte[]> getTable() {
        return table;
    }
    
    public int space() {
        return space.get();
    }
    
    public void dispatchDelete(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) {
        String key = ByteBufUtil.hexDump(commandData);
        commandData.release();
        byte[] value = table.remove(key);
        int usedspace = Short.BYTES + value.length; // value length self length + value length
        space.addAndGet(-usedspace); 
        channel.writeAndFlush(buildAck(commandCode, commandId, Status.OK.ordinal(), value));
    }
    
    public ByteBuf buildAck(int commandCode, long commandId, int status, byte[] value) {
        // status + value length + value
        int length = Integer.BYTES + Short.BYTES + value.length;
        
        ByteBuffer command = ByteBuffer.allocate(COMMAND_DATA_OFFSET + length);
        command.putInt(commandCode);             // put command code
        command.putLong(commandId);              // put command id
        command.putInt(length);                  // put command data length
        command.putInt(status);                  // put status
        command.putShort((short) value.length);  // put value length
        command.put(value);                      // put value
        command.flip(); 
        return Unpooled.wrappedBuffer(command);
    }
    
    public boolean isDynamic(ByteBuffer recordData) {
        return recordData.getShort(4) == DYNAMIC;
    }

    public boolean add(ByteBuffer recordData) {
        recordData = recordData.duplicate();
        recordData.position(REOCRD_BODY_OFFSET);
        
        int recordBodyLength = recordData.remaining();
        
        int keyLength = recordData.getShort();
        String key = ByteBufUtil.hexDump(recordData.array(), recordData.position(), keyLength);
        recordData.position(recordData.position() + keyLength);
        int valueLength = recordData.getShort();
        
        boolean addable = false;
        int space = this.space.get();
        int spaceUpdate = space + recordBodyLength;
        while (spaceUpdate <= spaceLimit && !addable) {
            addable = this.space.compareAndSet(space, spaceUpdate);
            if (addable) {
                byte[] value = new byte[valueLength];
                recordData.get(value);
                value = table.put(key, value);
                if (value != null) {
                    int usedspace = Short.BYTES + keyLength + Short.BYTES + value.length;
                    this.space.addAndGet(-usedspace); // available space may be slightly smaller than the limit
                }
                break;
            }
            space = this.space.get();
            spaceUpdate = space + recordBodyLength;
        }
        return addable;
    }
    
    public static MQKVdb unwrap(ByteBuffer checkPointWrapper) {
        MQKVdb kvdb = new MQKVdb();
        while (checkPointWrapper.hasRemaining()) {
            short keyLength = checkPointWrapper.getShort();
            byte[] key = new byte[keyLength];
            checkPointWrapper.get(key);
            
            short valueLength = checkPointWrapper.getShort();
            byte[] value = new byte[valueLength];
            checkPointWrapper.get(value);
            kvdb.table.put(ByteBufUtil.hexDump(key), value);
        }
        return kvdb;
    }
    
    public ByteBuffer wrapCheckPoint() {
        ByteBuf checkPointWrapper = Unpooled.buffer();
        table.forEach(new BiConsumer<String, byte[]>() {
            @Override
            public void accept(String key, byte[] value) {
                byte[] keybf = ByteBufUtil.decodeHexDump(key);
                checkPointWrapper.writeShort(keybf.length);
                checkPointWrapper.writeBytes(keybf);
                checkPointWrapper.writeShort(value.length);
                checkPointWrapper.writeBytes(value);
            }
        });
        return checkPointWrapper.nioBuffer();
    }
    
    public void recover(MQKVdb kvdb) {
        Map<String, byte[]> table = new ConcurrentHashMap<String, byte[]>();
        AtomicInteger space = new AtomicInteger();
        kvdb.table.forEach(new BiConsumer<String, byte[]>() {
            @Override
            public void accept(String key, byte[] value) {
                table.put(key, value);
                byte[] keybf = ByteBufUtil.decodeHexDump(key);
                int usedspace = Short.BYTES + keybf.length + Short.BYTES + value.length;
                space.addAndGet(usedspace);
            }
        });
        this.table = table;
        this.space = space;
    }
    
    public boolean hasIndex() {
        return !table.isEmpty();
    }

    public void clear() {
        table.clear();
    }

}
