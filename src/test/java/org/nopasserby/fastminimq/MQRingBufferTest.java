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

import static org.nopasserby.fastminimq.MQConstants.LogType.commit;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_DIR;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BYTES_BATCH_SIZE;
import static org.nopasserby.fastminimq.ReocrdHelper.createNonTransactionRecord;
import static org.nopasserby.fastminimq.ReocrdHelper.decodeReocrdIndex;
import java.io.File;
import java.nio.ByteBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.ConsumeQueue.MappedBuffer;
import org.nopasserby.fastminimq.log.RandomBufferSegmentLogFactory;

public class MQRingBufferTest {

    @Test(expected = IllegalArgumentException.class)
    public void ensureCapacityAvailableTest() {
        new MQRingBuffer(3, 1024, null, 0, false);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void ensureCapacityAvailableTest2() {
        new MQRingBuffer(4, 639, null, 0, false);
    }
    
	@Test
	public void writeBytesTest() throws Exception {
		MQNodeLog nodeLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + File.separator + commit));
		MQRingBuffer ringBuffer = new MQRingBuffer(1, 512 * 1024 * 1024 , nodeLog, BYTES_BATCH_SIZE, false);
		
		int batch = 1024 * 128;
		for (int index = 0; index < batch; index++) {
			ByteBuffer reocrdData = createNonTransactionRecord("testTopic", index);
			ringBuffer.writeBytes(reocrdData);
		}
		ringBuffer.flush();
		
		long offset = 0;
		ByteBuffer buffer = ByteBuffer.allocate(256);
		for (int index = 0; index < batch;) {
			MappedBuffer mapped = ringBuffer.mapped(offset);
			
			while (true) {
				long position = mapped.position();
				Integer length = mapped.getInteger(position);
				if (length == null) {
					break;
				}
				int recordLength = Integer.BYTES + length;
				if (mapped.read(buffer, position, recordLength) == 0) {
					break;
				}
				buffer.flip();
				
				Assert.assertEquals(index, decodeReocrdIndex(buffer));
				
				long newPosition = position + recordLength;
				mapped.position(newPosition);
				offset += recordLength;
				index++;
				buffer.clear();
			}
			mapped.release();
		}
		
		offset = 0;
		for (int index = 0; index < batch;) {
			MappedBuffer mapped = nodeLog.mapped(offset);
			
			while (true) {
				long position = mapped.position();
				Integer length = mapped.getInteger(position);
				if (length == null) {
					break;
				}
				int recordLength = Integer.BYTES + length;
				if (mapped.read(buffer, position, recordLength) == 0) {
					break;
				}
				buffer.flip();
				
				Assert.assertEquals(index, decodeReocrdIndex(buffer));
				
				long newPosition = position + recordLength;
				mapped.position(newPosition);
				offset += recordLength;
				index++;
				buffer.clear();
			}
			mapped.release();
		}
		
		nodeLog.delete();
	}
	
}
