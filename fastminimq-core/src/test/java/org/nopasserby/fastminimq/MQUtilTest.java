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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MQUtilTest {

	@Test
	public void createLRUCacheTest() {
		Map<Object, Object> lru = MQUtil.createLRUCache(3);
		lru.put(2, 2);
		lru.put(1, 1);
		lru.put(3, 3);
		lru.put(4, 4);
		
		Assert.assertNull(lru.get(2));
		Assert.assertEquals(1, lru.get(1));
		Assert.assertEquals(3, lru.get(3));
		Assert.assertEquals(4, lru.get(4));
	}
	
	@Test
	public void envLoadTest() throws FileNotFoundException, IOException {
	    MQUtil.envLoad(MQUtilTest.class.getResource("/").getPath() + "broker-test.conf");
	    
	    Assert.assertEquals("broker-1", System.getProperty("broker.id"));
	    Assert.assertEquals("0.0.0.0:6001", System.getProperty("hostname"));
	}
	
}
