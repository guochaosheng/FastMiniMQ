-- ----------------------------
-- Table structure for `queue_record`
-- ----------------------------
DROP TABLE IF EXISTS `queue_record`;
CREATE TABLE `queue_record` (
  `topic` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '主题',
  `group` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '组群名称',
  `subgroups` int NOT NULL COMMENT '当前群组子组数量',
  `subgroup_no` int NOT NULL COMMENT '当前子组编号',
  `step` int NOT NULL COMMENT '步长',
  `index` bigint DEFAULT NULL COMMENT '索引',
  PRIMARY KEY (`topic`,`group`,`subgroups`,`subgroup_no`,`step`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of queue_record
-- ----------------------------

-- ----------------------------
-- Table structure for `transaction_record`
-- ----------------------------
DROP TABLE IF EXISTS `transaction_record`;
CREATE TABLE `transaction_record` (
  `id` varchar(32) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `status` int DEFAULT NULL COMMENT '消息状态',
  `sign` int DEFAULT NULL COMMENT '消息类型',
  `broker` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '目标 Broker ',
  `topic` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '消息主题',
  `body` varchar(2000) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '消息内容',
  `timestamp` bigint DEFAULT NULL COMMENT '消息（存储）时间戳',
  `exception` varchar(2000) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL COMMENT '消息异常提示',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='消息 ID';

-- ----------------------------
-- Records of transaction_record
-- ----------------------------

-- ----------------------------
-- Table structure for `transaction_record_cnt`
-- ----------------------------
DROP TABLE IF EXISTS `transaction_record_cnt`;
CREATE TABLE `transaction_record_cnt` (
  `id` int NOT NULL COMMENT 'ID （段锁 ID）',
  `record_cnt` bigint DEFAULT NULL COMMENT '记录数量',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

-- ----------------------------
-- Records of transaction_record_cnt
-- ----------------------------
INSERT INTO `transaction_record_cnt` VALUES ('0', '0');
INSERT INTO `transaction_record_cnt` VALUES ('1', '0');
INSERT INTO `transaction_record_cnt` VALUES ('2', '0');
INSERT INTO `transaction_record_cnt` VALUES ('3', '0');
INSERT INTO `transaction_record_cnt` VALUES ('4', '0');
INSERT INTO `transaction_record_cnt` VALUES ('5', '0');
INSERT INTO `transaction_record_cnt` VALUES ('6', '0');
INSERT INTO `transaction_record_cnt` VALUES ('7', '0');
