CREATE TABLE `batch_job_type`
(
    `id`                       int unsigned       NOT NULL AUTO_INCREMENT,
    `biz_type`                 mediumint unsigned NOT NULL COMMENT '业务类型',
    `biz_name`                 varchar(32)        NOT NULL COMMENT '业务名',
    `remark`                   varchar(4096)      NOT NULL DEFAULT '' COMMENT '备注',
    `exec_type`                tinyint unsigned   NOT NULL DEFAULT 0 COMMENT '执行类型',
    `cb_before_create`         varchar(256)       NOT NULL DEFAULT '' COMMENT '创建任务回调url',
    `cb_before_run`            varchar(256)       NOT NULL DEFAULT '' COMMENT '启动前回调. 一旦配置, 则任务必须由业务主动调用 BizStartJob 执行任务. 否则任务将一直处于 JobStatus.WaitBizRun 状态',
    `cb_process`               varchar(256)       NOT NULL DEFAULT '' COMMENT '处理任务回调. 必填',
    `cb_process_stop`          varchar(256)       NOT NULL DEFAULT '' COMMENT '任务停止回调. 用于业务方做一些清理. 选填',
    `cb_before_create_timeout` int unsigned       NOT NULL DEFAULT 30 COMMENT '启动前回调超时秒数',
    `cb_before_run_timeout`    int unsigned       NOT NULL DEFAULT 30 COMMENT '启动前回调超时秒数',
    `cb_process_timeout`       int unsigned       NOT NULL DEFAULT 30 COMMENT '处理任务回调超时秒数',
    `cb_process_stop_timeout`  int unsigned       NOT NULL DEFAULT 30 COMMENT '任务停止回调超时秒数',
    `create_time`              datetime           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `rate_type`                tinyint unsigned   NOT NULL DEFAULT 0 COMMENT '速率类型. 0=通过rate_sec限速, 1=串行化',
    `rate_sec`                 int unsigned       NOT NULL DEFAULT 0 COMMENT '每秒处理速率. 0表示不限制',
    PRIMARY KEY (`id`),
    UNIQUE KEY `batch_job_type_biz_type` (`biz_type`),
    KEY `batch_job_type_create_time` (`create_time` DESC)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='批量任务类型';

CREATE TABLE `batch_job_list`
(
    `id`                 bigint unsigned    NOT NULL AUTO_INCREMENT,
    `job_id`             bigint unsigned    NOT NULL COMMENT '任务号',
    `biz_type`           mediumint unsigned NOT NULL COMMENT '业务类型',
    `biz_data`           varchar(10240)     NOT NULL DEFAULT '' COMMENT '业务任务数据, 让业务知道应该做什么',
    `process_data_total` bigint unsigned    NOT NULL DEFAULT 0 COMMENT '需要处理数据总数',
    `processed_count`    bigint unsigned    NOT NULL DEFAULT 0 COMMENT '已处理过的数据量, 无论成功还是失败. 如果任务在运行中, 则真实进度存在于redis',
    `err_log_count`      bigint unsigned    NOT NULL DEFAULT 0 COMMENT '错误日志数',
    `status`             tinyint unsigned   NOT NULL DEFAULT 0 COMMENT '任务状态 0=已创建 1=等待业务主动启动 2=运行中 3=已完成 4=正在停止 5=已停止',
    `create_time`        datetime           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`        datetime           NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `last_op_source`     varchar(32)        NOT NULL DEFAULT '' COMMENT '最后操作来源',
    `last_op_user_id`    varchar(32)        NOT NULL DEFAULT '' COMMENT '最后操作用户id',
    `last_op_user_name`  varchar(32)        NOT NULL DEFAULT '' COMMENT '最后操作用户名',
    `status_info`        varchar(4096)      NOT NULL DEFAULT '' COMMENT '状态信息',
    `op_history`         json               NOT NULL COMMENT '操作历史信息',
    `biz_process_data`   mediumtext         NOT NULL COMMENT '业务中需要处理的批量数据',
    PRIMARY KEY (`id`),
    UNIQUE KEY `batch_job_job_id` (`job_id`),
    KEY `batch_job_biz_type` (`biz_type`),
    KEY `batch_job_job_id_create_time_biz_type_index` (`job_id`, `create_time` DESC, `biz_type`),
    KEY `batch_job_job_id_update_time_biz_type_index` (`job_id`, `update_time` DESC, `biz_type`),
    KEY `batch_job_job_id_last_op_user_id_create_time_index` (`job_id`, `last_op_user_id`, `create_time` DESC)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_general_ci COMMENT ='批量任务列表';

CREATE TABLE `batch_job_log`
(
    `id`          bigint unsigned  NOT NULL AUTO_INCREMENT,
    `job_id`      bigint unsigned  NOT NULL COMMENT '任务号',
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
