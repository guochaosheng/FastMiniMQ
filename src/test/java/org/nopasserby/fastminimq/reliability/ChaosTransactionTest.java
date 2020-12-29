package org.nopasserby.fastminimq.reliability;

import static java.lang.Thread.sleep;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;

import org.nopasserby.fastminimq.reliability.ChaosMonkey.Node;
import org.nopasserby.fastminimq.reliability.ChaosMonkey.NodeType;

public class ChaosTransactionTest {
    
    public void run(String[] args) throws Exception {
        // 1. terminate application when it's running
        // 2. initialize broker data 
        ChaosMonkey chaosMonkey = new ChaosMonkey();
        chaosMonkey.load(ChaosMonkey.configurationFile);
        for (Node node: chaosMonkey.nodes) {
            chaosMonkey.ssh.exec(node.sshHost, node.sshUsername, node.sshPassword, "pkill -9 java");
            System.out.println(LocalDateTime.now() + "," + node.name + ",exec: pkill -9 java");
            sleep(100);
            if (node.type == NodeType.BROKER) {
                chaosMonkey.ssh.exec(node.sshHost, node.sshUsername, node.sshPassword, "rm -rf /data/fastminimq"); // remove broker old data 
                System.out.println(LocalDateTime.now() + "," + node.name + ",exec: rm -rf /data/fastminimq");
            }
        }
        
        // 3. initialize producer database/consumer database tables
        Checker checker = new Checker();
        checker.load(Checker.configurationFile);
        for (LocalDB localDB: checker.multiDB.producerGroup) {
            String initSQL = loadInitSQL("/producer-transaction-init.sql");
            System.out.println(LocalDateTime.now() + "," + localDB.label + ",exec sql:\n" + initSQL);
            localDB.execute(initSQL, false);
        }
        for (LocalDB localDB: checker.multiDB.consumerGroup) {
            String initSQL = loadInitSQL("/consumer-transaction-init.sql");
            System.out.println(LocalDateTime.now() + "," + localDB.label + ",exec sql:\n" + initSQL);
            localDB.execute(initSQL, false);
        }
        
        // 4. startup broker-group/producer-group/consumer-group
        for (Node node: chaosMonkey.nodes) {
            chaosMonkey.ssh.exec(node.sshHost, node.sshUsername, node.sshPassword, node.startupApplicationCmd);
            System.out.println(LocalDateTime.now() + "," + node.name + ",exec: " + node.startupApplicationCmd);
        }
        
        // 5. startup chaos-monkey
        chaosMonkey.start();
        
        sleep(10 * 60  * 1000);
        
        // 6. startup checker
        checker.start();
    }
    
    public String loadInitSQL(String file) throws IOException {
        File config = new File(file);
        InputStream is = config.isFile() ? new FileInputStream(config) : this.getClass().getResourceAsStream(file);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] b = new byte[4 * 1024];
        int len = 0;
        while ((len = is.read(b)) > 0) {
            bos.write(b, 0, len);
        }
        is.close();
        return new String(bos.toByteArray());
    }
    
    public static void main(String[] args) throws Exception {
        ChaosTransactionTest chaosTransactionTest = new ChaosTransactionTest();
        chaosTransactionTest.run(args);
    }
    
}
