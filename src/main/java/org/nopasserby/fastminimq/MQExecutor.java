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

import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_RES_CONTENT_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_RES_STATUS_SELF_LENGTH;
import static org.nopasserby.fastminimq.MQUtil.throwableToString;

import org.nopasserby.fastminimq.MQConstants.Status;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

public abstract class MQExecutor {
    
    public static abstract class MQDispatch extends MQExecutor {
        
        @Override
        protected void execute(ChannelDelegate channel, ByteBuf commandWrapper) throws Exception {
            int commandCode = commandWrapper.readInt();
            long commandId = commandWrapper.readLong();
            int commandDataLength = commandWrapper.readInt();
            int remaining = commandWrapper.readableBytes();
            if (commandDataLength != remaining) {
                throw new IllegalArgumentException();
            }
            ByteBuf commandData = commandWrapper;
            
            dispatch(channel, commandCode, commandId, commandData);
        }
        
        @Override
        protected void exceptionCaught(ChannelDelegate channel, Throwable cause) throws Exception {
        }
        
        protected abstract void dispatch(ChannelDelegate channel, int commandCode, long commandId, ByteBuf commandData) throws Exception;
        
    }
    
    protected abstract void execute(ChannelDelegate channel, ByteBuf commandWrapper) throws Exception;
    
    protected abstract void exceptionCaught(ChannelDelegate channel, Throwable cause) throws Exception;

    ChannelHandler channelHandler(int maxFrameLength) {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(maxFrameLength, 12, 4, 0, 0))
                             .addLast(new ChannelInboundHandlerAdapter() {
                                 @Override
                                 public void channelRead(ChannelHandlerContext ctx, Object wrapper) throws Exception {
                                     execute(delegate(ctx.channel()), (ByteBuf) wrapper);
                                 }
                                 @Override
                                 public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                                     MQExecutor.this.exceptionCaught(delegate(ctx.channel()), cause);
                                 }
                             });
            }
        };
    }
    
    private static final AttributeKey<ChannelDelegate> CHANNEL_DELEGATE_KEY = AttributeKey.valueOf("channel_delegate_key");
    
    ChannelDelegate delegate(Channel channel) {
        Attribute<ChannelDelegate> attr = channel.attr(CHANNEL_DELEGATE_KEY);
        ChannelDelegate channelDelegate = attr.get();
        if (channelDelegate == null) {
            channelDelegate = new ChannelDelegate(channel);
            attr.set(channelDelegate);
        }
        return channelDelegate;
    }
    
    ByteBuf buildErr(int commandCode, long commandId, Exception e) {
        ByteBuf exception = encodeException(e);
        ByteBuf buffer = Unpooled.buffer(COMMAND_DATA_RES_CONTENT_OFFSET + exception.readableBytes());
        int commandLength = COMMAND_DATA_RES_STATUS_SELF_LENGTH + exception.readableBytes();
        buffer.writeInt(commandCode);
        buffer.writeLong(commandId);
        buffer.writeInt(commandLength);
        buffer.writeInt(Status.FAIL.ordinal());
        buffer.writeBytes(exception);
        return buffer;
    }
    
    ByteBuf encodeException(Exception e) {
        return Unpooled.copiedBuffer(throwableToString(e).getBytes());
    }
    
    Exception decodeException(ByteBuf ebuf) {
        int describeLength = ebuf.readableBytes();
        byte[] trace = new byte[describeLength];
        ebuf.readBytes(trace);
        return new RuntimeException(new String(trace));
    }
    
    public static class ChannelDelegate {
        final Channel channel;
        
        public ChannelDelegate(Channel channel) {
            this.channel = channel;
        }

        public void writeAndFlush(ByteBuf bytebuf) {
            channel.writeAndFlush(bytebuf);
        }

        public void flush() {
            channel.flush();
        }

        public void write(ByteBuf bytebuf) {
            channel.write(bytebuf);
        }
        
        @Override
        public String toString() {
            return channel.localAddress() + "->" + channel.remoteAddress();
        }
        
    }

}
