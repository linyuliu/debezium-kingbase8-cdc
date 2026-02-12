package io.debezium.connector.kingbasees.console.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
@Slf4j
public class ApiExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    @ResponseBody
    public ApiResponse<Void> handleBadRequest(IllegalArgumentException e) {
        log.warn("[接口] 参数错误: {}", e.getMessage());
        return ApiResponse.error(e.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public ApiResponse<Void> handleStateError(IllegalStateException e) {
        log.error("[接口] 状态异常", e);
        return ApiResponse.error(e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    @ResponseBody
    public ApiResponse<Void> handleException(Exception e) {
        log.error("[接口] 未处理异常", e);
        return ApiResponse.error("系统内部异常: " + e.getMessage());
    }
}
