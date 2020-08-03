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

import static org.nopasserby.fastminimq.MQConstants.COMMIT_TX;
import static org.nopasserby.fastminimq.MQConstants.DYNAMIC;
import static org.nopasserby.fastminimq.MQConstants.IMMUTABLE;
import static org.nopasserby.fastminimq.MQConstants.NON_TX;
import static org.nopasserby.fastminimq.MQConstants.PRE_TX;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.COMMAND_DATA_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_BODY_OFFSET;
import static org.nopasserby.fastminimq.MQConstants.MQCommand.REOCRD_HEAD_LENGTH;
import static org.nopasserby.fastminimq.MQUtil.nextUUID;

import org.nopasserby.fastminimq.MQRegistry.MQClusterMetaData;

import java.nio.ByteBuffer;

public class ReocrdHelper {
	
    public static ByteBuffer createKVRecord(String topic, long index) throws Exception {
        MQConsumer consumer = new MQConsumer(null, new MQClusterMetaData(null));
        MQQueue queue = createMQQueue("testTopic", MQGroup.GROUP_1, index);
        ByteBuffer command = consumer.buildAckOut(0, queue);
        command.position(COMMAND_DATA_OFFSET);
        ByteBuffer recordData = ByteBuffer.allocate(REOCRD_BODY_OFFSET + command.remaining());
        recordData.putInt(REOCRD_HEAD_LENGTH + command.remaining()); // record length
        recordData.putShort(DYNAMIC);                                // record type
        recordData.putLong(MQUtil.currentTimeMillis());              // record time stamp
        recordData.put(command);
        recordData.flip();
        return recordData;
    }
    
    public static MQQueue createMQQueue(String topic, MQGroup group, long index) {
        MQQueue queue = new MQQueue();
        MQGroup[] groups = MQGroup.values();
        queue.setTopic(topic);
        queue.setGroup(MQGroup.class.getSimpleName());
        queue.setSubgroups(groups.length);
        queue.setSubgroupNo(group.ordinal());
        queue.setStep(400);
        queue.nextIndex(index);
        return queue;
    }
    
    enum MQGroup {
        GROUP_1
    }
    
	public static ByteBuffer createNonTransactionRecord(String topic) throws Exception {
		return encodeReocrdIndex(nextUUID(), topic, NON_TX, 0);
	}
	
	public static ByteBuffer createNonTransactionRecord(String topic, int index) throws Exception {
        return encodeReocrdIndex(nextUUID(), topic, NON_TX, index);
    }
	
	public static ByteBuffer createPrepareTransactionReocrd(byte[] txid, String topic) throws Exception {
		return encodeReocrdIndex(txid, topic, PRE_TX, 0);
	}
	
	public static ByteBuffer createCommitTransactionReocrd(byte[] txid, String topic) throws Exception {
		return encodeReocrdIndex(txid, topic, COMMIT_TX, 0);
	}
	
	public static ByteBuffer createReocrd(byte[] globalId, String topic, byte sign, byte[] body) throws Exception {
	    MQProducer producer = new MQProducer(null, new MQClusterMetaData(null));
	    MQProducer.MQRecordMetaData recordMetaData = new MQProducer.MQRecordMetaData();
	    recordMetaData.body = body;
	    recordMetaData.sign = sign;
	    recordMetaData.id = globalId;
	    recordMetaData.topic = topic;
	    recordMetaData.producer = "testProducer".getBytes();
	    ByteBuffer recordBody = producer.encode(recordMetaData);
	    
	    ByteBuffer recordData = ByteBuffer.allocate(REOCRD_BODY_OFFSET + recordBody.remaining());
        recordData.putInt(REOCRD_HEAD_LENGTH + recordBody.remaining()); // record length
        recordData.putShort(IMMUTABLE);                                 // record type
        recordData.putLong(MQUtil.currentTimeMillis());                 // record time stamp
        recordData.put(recordBody);                                     // record body
		recordData.flip();
		
		return recordData;
	}
	
	static ByteBuffer encodeReocrdIndex(byte[] txid, String topic, byte sign, int index) throws Exception {
        String body = "Hello world! Hello world! Hello world! Hello world! Hello world! "
                    + "Hello world! Hello world! Hello world! Hello world! "
                    + "-" + String.format("%010d", index);
        return createReocrd(txid, topic, sign, body.getBytes());
    }
	
	public static int decodeReocrdIndex(ByteBuffer recordData) throws Exception {
	    MQConsumer consumer = new MQConsumer(null, new MQClusterMetaData(null));
	    String body = new String(consumer.decodeRecordList(recordData).get(0).getBody());
		return Integer.parseInt(body.substring(body.length() - 10));
	}

}
