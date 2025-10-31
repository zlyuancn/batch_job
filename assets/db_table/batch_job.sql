CREATE TABLE `batch_job_biz`
(
    `biz_id`           int unsigned     NOT NULL AUTO_INCREMENT COMMENT '业务id',
    `biz_name`         varchar(32)      NOT NULL COMMENT '业务名',
    `remark`           varchar(1024)    NOT NULL DEFAULT '' COMMENT '备注',
    `exec_type`        tinyint unsigned NOT NULL DEFAULT 0 COMMENT '执行类型',
    `exec_extend_data` varchar(4096)    NOT NULL DEFAULT '' COMMENT '执行器扩展数据',
    `create_time`      datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`      datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `op_source`        varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作来源',
    `op_user_id`       varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户id',
    `op_user_name`     varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户名',
    `op_remark`        varchar(1024)    NOT NULL DEFAULT '' COMMENT '最后操作备注',
    `status`           tinyint unsigned NOT NULL DEFAULT 0 COMMENT '状态 0=正常 1=隐藏',
    PRIMARY KEY (`biz_id`),
    KEY `batch_job_biz_create_time` (`create_time` DESC),
    KEY `batch_job_biz_status_create_time` (`status`, `create_time` DESC)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='批量任务类型';

CREATE TABLE `batch_job_biz_history`
(
    `id`               bigint unsigned  NOT NULL AUTO_INCREMENT,
    `biz_id`           int unsigned     NOT NULL COMMENT '业务id',
    `biz_name`         varchar(32)      NOT NULL COMMENT '业务名',
    `remark`           varchar(1024)    NOT NULL DEFAULT '' COMMENT '备注',
    `exec_type`        tinyint unsigned NOT NULL DEFAULT 0 COMMENT '执行类型',
    `exec_extend_data` varchar(4096)    NOT NULL DEFAULT '' COMMENT '执行器扩展数据',
    `update_time`      datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `op_source`        varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作来源',
    `op_user_id`       varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户id',
    `op_user_name`     varchar(32)      NOT NULL DEFAULT '' COMMENT '最后操作用户名',
    `op_remark`        varchar(1024)    NOT NULL DEFAULT '' COMMENT '最后操作备注',
    `status`           tinyint unsigned NOT NULL DEFAULT 0 COMMENT '状态 0=正常 1=隐藏',
    PRIMARY KEY (`id`),
    KEY `batch_job_biz_create_time` (`biz_id`, `update_time` DESC)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='批量任务类型历史记录';

CREATE TABLE `batch_job_list`
(
    `id`                 bigint unsigned    NOT NULL AUTO_INCREMENT,
    `job_id`             int unsigned       NOT NULL COMMENT '任务号',
    `job_name`           varchar(1024)      NOT NULL DEFAULT '' COMMENT '任务名称',
    `biz_id`             mediumint unsigned NOT NULL COMMENT '业务id',
    `job_data`           varchar(8192)      NOT NULL DEFAULT '' COMMENT '任务数据, 让业务知道应该做什么',
    `process_data_total` bigint unsigned    NOT NULL DEFAULT 0 COMMENT '需要处理数据总数',
    `processed_count`    bigint unsigned    NOT NULL DEFAULT 0 COMMENT '已处理过的数据量, 无论成功还是失败. 如果任务在运行中, 则真实进度存在于redis',
    `err_log_count`      bigint unsigned    NOT NULL DEFAULT 0 COMMENT '错误日志数',
    `status`             tinyint unsigned   NOT NULL DEFAULT 0 COMMENT '任务状态 0=已创建 1=等待业务主动启动 2=运行中 3=已完成 4=正在停止 5=已停止',
    `create_time`        datetime           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`        datetime           NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `op_source`          varchar(32)        NOT NULL DEFAULT '' COMMENT '最后操作来源',
    `op_user_id`         varchar(32)        NOT NULL DEFAULT '' COMMENT '最后操作用户id',
    `op_user_name`       varchar(32)        NOT NULL DEFAULT '' COMMENT '最后操作用户名',
    `op_remark`          varchar(1024)      NOT NULL DEFAULT '' COMMENT '最后操作备注',
    `status_info`        varchar(1024)      NOT NULL DEFAULT '' COMMENT '状态信息',
    `rate_type`          tinyint unsigned   NOT NULL DEFAULT 0 COMMENT '速率类型. 0=通过rate_sec限速, 1=串行化',
    `rate_sec`           int unsigned       NOT NULL DEFAULT 0 COMMENT '每秒处理速率. 0表示不限制',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`job_id`),
    KEY `batch_job_list_biz_id` (`biz_id`, `status`, `update_time` DESC),
    KEY `batch_job_list_job_id_op_user_id_create_time_index` (`biz_id`, `op_user_id`, `status`, `create_time` DESC)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='批量任务列表';

CREATE TABLE `batch_job_list_history`
(
    `id`                 bigint unsigned    NOT NULL AUTO_INCREMENT,
    `job_id`             int unsigned       NOT NULL COMMENT '任务号',
    `job_name`           varchar(1024)      NOT NULL DEFAULT '' COMMENT '任务名称',
    `biz_id`             mediumint unsigned NOT NULL COMMENT '业务id',
    `job_data`           varchar(8192)      NOT NULL DEFAULT '' COMMENT '任务数据, 让业务知道应该做什么',
    `process_data_total` bigint unsigned    NOT NULL DEFAULT 0 COMMENT '需要处理数据总数',
    `processed_count`    bigint unsigned    NOT NULL DEFAULT 0 COMMENT '已处理过的数据量, 无论成功还是失败. 如果任务在运行中, 则真实进度存在于redis',
    `status`             tinyint unsigned   NOT NULL DEFAULT 0 COMMENT '任务状态 0=已创建 1=等待业务主动启动 2=运行中 3=已完成 4=正在停止 5=已停止',
    `update_time`        datetime           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `op_source`          varchar(32)        NOT NULL DEFAULT '' COMMENT '最后操作来源',
    `op_user_id`         varchar(32)        NOT NULL DEFAULT '' COMMENT '最后操作用户id',
    `op_user_name`       varchar(32)        NOT NULL DEFAULT '' COMMENT '最后操作用户名',
    `op_remark`          varchar(1024)      NOT NULL DEFAULT '' COMMENT '最后操作备注',
    `status_info`        varchar(1024)      NOT NULL DEFAULT '' COMMENT '状态信息',
    `rate_type`          tinyint unsigned   NOT NULL DEFAULT 0 COMMENT '速率类型. 0=通过rate_sec限速, 1=串行化',
    `rate_sec`           int unsigned       NOT NULL DEFAULT 0 COMMENT '每秒处理速率. 0表示不限制',
    PRIMARY KEY (`id`),
    KEY `batch_job_list_biz_id` (`job_id`, `update_time` DESC)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='批量任务列表历史记录';

CREATE TABLE `batch_job_log`
(
    `id`          bigint unsigned  NOT NULL AUTO_INCREMENT,
    `job_id`      int unsigned     NOT NULL COMMENT '任务号',
    `data_id`     varchar(256)     NOT NULL COMMENT '数据id',
    `remark`      varchar(4096)    NOT NULL DEFAULT '' COMMENT '备注',
    `extend`      varchar(4096)    NOT NULL DEFAULT '' COMMENT '扩展数据',
    `log_type`    tinyint unsigned NOT NULL DEFAULT 0 COMMENT '日志类型 0=调试Debug 1=信息Info 2=警告Warn 3=错误Err',
    `create_time` datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `batch_job_log_job_id` (`job_id`, `create_time` DESC),
    KEY `batch_job_log_job_id_log_type` (`job_id`, `log_type`, `create_time` DESC)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='批量任务日志';
