package io.debezium.connector.kingbasees.console.web;

import lombok.Data;

/**
 * 统一接口响应包装。
 * @param <T> 业务数据类型
 */
@Data
public class ApiResponse<T> {

    private boolean ok;
    private String message;
    private T data;

    public static <T> ApiResponse<T> ok(T data) {
        ApiResponse<T> response = new ApiResponse<T>();
        response.setOk(true);
        response.setData(data);
        return response;
    }

    public static <T> ApiResponse<T> error(String message) {
        ApiResponse<T> response = new ApiResponse<T>();
        response.setOk(false);
        response.setMessage(message);
        return response;
    }
}
