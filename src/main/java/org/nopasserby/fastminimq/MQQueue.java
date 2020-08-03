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
                                                             
public class MQQueue {
    
    private static final int DEFAULT_SUBGROUPS = 1;
    
    private static final int DEFAULT_STEP = 400;
    
    private String topic;
    
    private String group;
    
    private int subgroups = DEFAULT_SUBGROUPS;
    
    private int subgroupNo;
    
    private int step = DEFAULT_STEP;
    
    private long index;
    
    private long nextIndex;
    
    public MQQueue() {
    }

    public MQQueue(String topic, String group) {
        this.topic = topic;
        this.group = group;
    }
    
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public int getSubgroups() {
        return subgroups;
    }

    public void setSubgroups(int subgroups) {
        this.subgroups = subgroups;
    }

    public int getSubgroupNo() {
        return subgroupNo;
    }

    public void setSubgroupNo(int subgroupNo) {
        this.subgroupNo = subgroupNo;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }
    
    public void ack() {
        index = ack0();
    }
    
    protected long ack0() {
        return nextAvailableIndex(nextIndex);
    }
    
    public void nextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
    }
    
    public long nextIndex() {
        index = nextAvailableIndex(nextIndex);
        return index;
    }
    
    private long nextAvailableIndex(long index) {
        long period = subgroups * step;
        long startIndex = index / period * period + subgroupNo * step;
        long endIndex = startIndex + step - 1;
        if (index < startIndex) {
            index = startIndex;
        }
        if (index > endIndex) {
            index = startIndex + period; // skip to next period start offset
        }
        return index;
    }
    
    public int rows(long index) {
        long period = subgroups * step;
        long startIndex = index / period * period + subgroupNo * step;
        long endIndex = startIndex + step - 1;
        
        int rows = (int) (endIndex - index + 1);
        if (rows < 0 || rows > step) {
            rows = -1; // offset is not in the current period, it may be an offset error
        }
        return rows;
    }

}
