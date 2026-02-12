package io.debezium.connector.kingbasees.console.model;

import lombok.Data;

@Data
public class TaskRuntimeInfo {

    private String taskId;
    private boolean running;
    private String lastRunMode;
    private String lastTrigger;
    private Long lastStartAt;
    private Long lastStopAt;
    private Integer lastExitCode;
    private String message;
}
