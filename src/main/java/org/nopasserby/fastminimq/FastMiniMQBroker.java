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

import static org.nopasserby.fastminimq.MQConstants.MQBroker.BROKER_ID;
import static org.nopasserby.fastminimq.MQConstants.MQBroker.BROKER_HOSTNAME;
import org.nopasserby.fastminimq.MQBroker.MQBrokerCfg;

public class FastMiniMQBroker {
    
    /**
     * usage: java -jar FastMiniMQBroker.jar 
     *        or 
     *        java -jar FastMiniMQBroker.jar -c broker.conf
     * 
     * */
    public static void main(String[] args) throws Exception {
        if (args.length > 1 && "-c".equals(args[0])) {
            MQUtil.envLoad(args[1]);
        }
        
        bannerOut();
        
        MQBroker broker = new MQBroker(new MQBrokerCfg(BROKER_ID, BROKER_HOSTNAME));
        broker.run();
    }
    
    public static void bannerOut() {
        out("  _______  _______  _______  _______  __   __  ___   __    _  ___   __   __  _______  ");
        out(" |       ||   _   ||       ||       ||  |_|  ||   | |  |  | ||   | |  |_|  ||       | ");
        out(" |    ___||  |_|  ||  _____||_     _||       ||   | |   |_| ||   | |       ||   _   | ");
        out(" |   |___ |       || |_____   |   |  |       ||   | |       ||   | |       ||  | |  | ");
        out(" |    ___||       ||_____  |  |   |  |       ||   | |  _    ||   | |       ||  |_|  | ");
        out(" |   |    |   _   | _____| |  |   |  | ||_|| ||   | | | |   ||   | | ||_|| ||      |  ");
        out(" |___|    |__| |__||_______|  |___|  |_|   |_||___| |_|  |__||___| |_|   |_||____||_| ");
        out("                                                                                      ");
    }
    
    public static void out(String s) {
        System.out.printf("%s%n", s);
    }
    
}
