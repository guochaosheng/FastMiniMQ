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
import static org.nopasserby.fastminimq.MQConstants.MAGIC_V1;
import static org.nopasserby.fastminimq.MQConstants.LogType.commit;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.DATA_DIR;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.MAX_DATA_SEGMENT_LENGTH;
import static org.nopasserby.fastminimq.ReocrdHelper.createNonTransactionRecord;
import static org.nopasserby.fastminimq.ReocrdHelper.decodeReocrdIndex;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import org.nopasserby.fastminimq.ConsumeQueue.MappedBuffer;
import org.nopasserby.fastminimq.log.RandomBufferSegmentLogFactory;
import org.nopasserby.fastminimq.log.SegmentLog;
import org.nopasserby.fastminimq.log.SegmentLogFactory;
import org.nopasserby.fastminimq.log.FileChannelSegmentLogFactory;

public class MQNodeLogTest {

	@Test
	public void writeTest() throws Exception {
		MQNodeLog nodeLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + File.separator + commit));
		
		int batch = 1024;
		for (int index = 0; index < batch; index++) {
			ByteBuffer reocrdData = createNonTransactionRecord("testTopic", index);
			nodeLog.write(reocrdData);
		}
		nodeLog.flush();
		
		long offset = 0;
		ByteBuffer buffer = ByteBuffer.allocate(256);
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
	
	@Test
	public void reloadTest() throws Exception {
		MQNodeLog nodeLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + File.separator + commit));
		int marker = 8888888;
		ByteBuffer reocrdData = createNonTransactionRecord("testTopic", marker);
		int reocrdLength = reocrdData.remaining();
		nodeLog.write(reocrdData);
		nodeLog.flush();
		
		nodeLog.close();
		nodeLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + File.separator + commit));
		
		ByteBuffer buffer = ByteBuffer.allocate(reocrdLength);
		nodeLog.read(buffer);
		buffer.flip();
		Assert.assertEquals(marker, decodeReocrdIndex(buffer));
		
		nodeLog.delete();
	}
	
	@Test
	public void nextAvailableOffsetTest() throws Exception {
		MQNodeLog nodeLog = new MQNodeLog(new SegmentLogFactory() {
			String rootdirPath = DATA_DIR + separator + commit;
			@Override
			public SegmentLog[] reloadSegmentLog() throws Exception {
				MQUtil.createDir(rootdirPath);
				return new SegmentLog[0];
			}
			@Override
			public SegmentLog createSegmentLog(long baseOffset) throws Exception {
				
				String filename = SegmentLog.baseOffset2Filename(baseOffset);
				String pathname = rootdirPath + separator + filename;
				FileChannelSegmentLogFactory fileChannelSegmentLogFactory = new FileChannelSegmentLogFactory(rootdirPath);
				return fileChannelSegmentLogFactory.new FileChannelFileSegment(new File(pathname), MAX_DATA_SEGMENT_LENGTH) {
					int count;
					@Override
					public int write(ByteBuffer src) throws Exception {
						if (count++ > 0) {
							int truncateLength = 1 + new Random().nextInt(src.remaining() - 1);
							src = ByteBuffer.allocate(truncateLength).put(src.array(), 0, truncateLength);//write interrupt
							src.flip();
							super.write(src);
							throw new Exception("Abort");
						}
						return super.write(src);
					}
				};
			}
		});
		
		int marker = 88888888;
		ByteBuffer reocrdData_1 = createNonTransactionRecord("testTopic", marker);
		nodeLog.write(reocrdData_1);
		
		long startOffset = nodeLog.writeOffset();
		ByteBuffer reocrdData_2 = createNonTransactionRecord("testTopic", marker);
		try {
			nodeLog.write(reocrdData_2);
		} catch (Exception e) {
			//Abort Exception
			nodeLog.close();
		}
		
		nodeLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + File.separator + commit));
		long endOffset = nodeLog.writeOffset();
		
		long nextAvailableOffset = 0;
		long breakoutOffset = 0;
		long offset = 0;
		try {
			while (true) {				
				long lastOffset = offset;
				
				MappedBuffer mapped = nodeLog.mapped(offset);
				while (true) {
					ByteBuffer buffer = ByteBuffer.allocate(256);
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
					
					Assert.assertEquals(marker, decodeReocrdIndex(buffer));
					
					long newPosition = position + recordLength;
					mapped.position(newPosition);
					offset += recordLength;
					buffer.clear();
				}
				mapped.release();
				
				if (lastOffset == offset) {
					throw new IllegalStateException("commit log unable to build index.");
				}
			}
		} catch (Exception e) {
			breakoutOffset = offset;
			nextAvailableOffset = nodeLog.nextAvailableOffset(offset);
		} finally {
			nodeLog.delete();
		}
		
		Assert.assertTrue(startOffset < endOffset);
		Assert.assertEquals(startOffset, breakoutOffset);
		Assert.assertEquals(endOffset, nextAvailableOffset);
	}
	
	@Test
	public void truncateTest() throws Exception {
		MQNodeLog nodeLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + File.separator + commit));
		int checkPointSize = 56;
		ByteBuffer checkPointWrapper_1 = ByteBuffer.allocate(checkPointSize);
		long checkPointOffset_1 = 100;
		long checkPointIndex_1 = 100;
		long globalIDPointOffset_1 = 200;
		int globalIDPointLength_1 = 200;
		long consumeOffsetPointOffset_1 = 300;
		int consumeOffsetPointLength_1 = 300;
		long pointOffset_1 = 400;
		int pointLength_1 = 400;
		checkPointWrapper_1.putLong(checkPointOffset_1);
		checkPointWrapper_1.putLong(checkPointIndex_1);
		checkPointWrapper_1.putLong(globalIDPointOffset_1);
		checkPointWrapper_1.putInt(globalIDPointLength_1);
		checkPointWrapper_1.putLong(consumeOffsetPointOffset_1);
		checkPointWrapper_1.putInt(consumeOffsetPointLength_1);
		checkPointWrapper_1.putLong(pointOffset_1);
		checkPointWrapper_1.putInt(pointLength_1);
		checkPointWrapper_1.putInt(MAGIC_V1);
		checkPointWrapper_1.flip();
		nodeLog.write(checkPointWrapper_1.array(), checkPointWrapper_1.position(), checkPointWrapper_1.remaining());
		nodeLog.flush();
		
		ByteBuffer checkPointWrapper_2 = ByteBuffer.allocate(checkPointSize);
		long checkPointOffset_2 = 100;
		long checkPointIndex_2 = 100;
		long globalIDPointOffset_2 = 200;
		int globalIDPointLength_2 = 200;
		long consumeOffsetPointOffset_2 = 300;
		int consumeOffsetPointLength_2 = 300;
		long pointOffset_2 = 400;
		int pointLength_2 = 400;
		checkPointWrapper_2.putLong(checkPointOffset_2);
		checkPointWrapper_2.putLong(checkPointIndex_2);
		checkPointWrapper_2.putLong(globalIDPointOffset_2);
		checkPointWrapper_2.putInt(globalIDPointLength_2);
		checkPointWrapper_2.putLong(consumeOffsetPointOffset_2);
		checkPointWrapper_2.putInt(consumeOffsetPointLength_2);
		checkPointWrapper_2.putLong(pointOffset_2);
		checkPointWrapper_2.putInt(pointLength_2);
		checkPointWrapper_2.putInt(MAGIC_V1);
		checkPointWrapper_2.flip();
		nodeLog.write(checkPointWrapper_2.array(), checkPointWrapper_2.position(), checkPointWrapper_2.remaining());
		nodeLog.flush();
		
		nodeLog.close();
		nodeLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + File.separator + commit));
		nodeLog.truncate(checkPointSize);
		ByteBuffer buffer_2 = ByteBuffer.allocate(checkPointSize << 1);
		nodeLog.readFlip(buffer_2, 0);
		
		Assert.assertEquals(checkPointSize, buffer_2.remaining());
		
		ByteBuffer checkPointWrapper_3 = ByteBuffer.allocate(checkPointSize);
		long checkPointOffset_3 = 100;
		long checkPointIndex_3 = 100;
		long globalIDPointOffset_3 = 200;
		int globalIDPointLength_3 = 200;
		long consumeOffsetPointOffset_3 = 300;
		int consumeOffsetPointLength_3 = 300;
		long pointOffset_3 = 400;
		int pointLength_3 = 400;
		checkPointWrapper_3.putLong(checkPointOffset_3);
		checkPointWrapper_3.putLong(checkPointIndex_3);
		checkPointWrapper_3.putLong(globalIDPointOffset_3);
		checkPointWrapper_3.putInt(globalIDPointLength_3);
		checkPointWrapper_3.putLong(consumeOffsetPointOffset_3);
		checkPointWrapper_3.putInt(consumeOffsetPointLength_3);
		checkPointWrapper_3.putLong(pointOffset_3);
		checkPointWrapper_3.putInt(pointLength_3);
		checkPointWrapper_3.putInt(MAGIC_V1);
		checkPointWrapper_3.flip();
		nodeLog.write(checkPointWrapper_3.array(), checkPointWrapper_3.position(), checkPointWrapper_3.remaining());
		nodeLog.flush();
		
		ByteBuffer buffer_3_1 = ByteBuffer.allocate(checkPointSize << 1);
		nodeLog.readFlip(buffer_3_1, 0);
		
		Assert.assertEquals(checkPointSize << 1, buffer_3_1.remaining());
		
		nodeLog.close();
		nodeLog = new MQNodeLog(new RandomBufferSegmentLogFactory(DATA_DIR + File.separator + commit));
		
		ByteBuffer buffer_3_2 = ByteBuffer.allocate(checkPointSize << 1);
		nodeLog.readFlip(buffer_3_2, 0);
		
		Assert.assertEquals(checkPointSize << 1, buffer_3_2.remaining());
		
		nodeLog.delete();
	}
	
}
