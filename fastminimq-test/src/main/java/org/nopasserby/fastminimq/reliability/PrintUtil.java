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

package org.nopasserby.fastminimq.reliability;

public class PrintUtil {
    
    private final static String sep = System.lineSeparator();
    
    public final static String[] LINE_SEPARATOR_REFERENCE = new String[0];
    
    public static void print(String[][] table) {
        System.out.print(formatTable(table));
    }
    
    public static String formatTable(String[][] table) {
        int cnt = table[0].length;
        int[] columnLength = new int[table[0].length];
        for (int row = 0; row < table.length; row++) {
            if (table[row] != LINE_SEPARATOR_REFERENCE && table[row].length != cnt) 
                throw new IllegalArgumentException("table rows are different");
            
            for (int col = 0; col < table[row].length; col++) {
                if (table[row][col] == null) table[row][col] = "";
                
                columnLength[col] = Math.max(columnLength[col], table[row][col].length() + 2); // add 2 spaces
            }
        }
        return formatTable(table, columnLength);
    }
    
    /**
     * Example:
     * 
     * table = new String[4][3]
     * table[0] = new String[]{"node name", "node host", "down count"}
     * table[1] = new String[]{"broker1", "47.92.76.189", "10"}
     * table[2] = new String[]{"consumer1", "39.100.17.56", "14"}
     * table[3] = new String[]{"producer1", "47.92.106.53", "8"}
     * 
     * @param: table
     * @return:
     * +-----------+---------------+------------+
     * |node name  |node host      |down count  |
     * +-----------+---------------+------------+
     * |broker1    |47.92.76.189   |10          |
     * |consumer1  |39.100.17.56   |14          |
     * |producer1  |47.92.106.53   |8           |
     * +-----------+---------------+------------+
     * 
     */
    private static String formatTable(String[][] table, int[] columnLength) {
        StringBuilder sb = new StringBuilder();
        sb.append(tolineSeparator(columnLength));
        for (int row = 0; row < table.length; row++) {
            if (table[row] == LINE_SEPARATOR_REFERENCE) {// print line separator
                sb.append(tolineSeparator(columnLength));
                continue;
            }
            for (int col = 0; col < table[row].length; col++) {
                sb.append("|");
                sb.append(padding(table[row][col], " ", columnLength[col]));
            }
            sb.append("|");
            sb.append(sep);
            if (row == 0) sb.append(tolineSeparator(columnLength));
        }
        sb.append(tolineSeparator(columnLength)); //add foot
        return sb.toString();
    }
    
    private static String tolineSeparator(int[] colLen) {
        StringBuilder sb = new StringBuilder();
        for (int col = 0; col < colLen.length; col++) {
            sb.append("+");
            sb.append(padding("", "-", colLen[col]));
        }
        sb.append("+");
        sb.append(sep);
        return sb.toString();
    }
    
    private static String padding(String str, String pad, int length) {
        while (str.length() < length) {
            str += pad;
        }
        return str;
    }
    
}
