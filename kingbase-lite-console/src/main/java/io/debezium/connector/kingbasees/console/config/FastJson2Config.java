package io.debezium.connector.kingbasees.console.config;

import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.support.config.FastJsonConfig;
import com.alibaba.fastjson2.support.spring.http.converter.FastJsonHttpMessageConverter;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * FastJson2 全局配置：
 * 1) 统一 Spring MVC 的 JSON 序列化/反序列化实现
 * 2) 移除默认 Jackson Converter，避免多套 JSON 行为不一致
 */
@Configuration
public class FastJson2Config implements WebMvcConfigurer {

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        converters.removeIf(converter -> converter.getClass().getName().contains("MappingJackson2HttpMessageConverter"));

        FastJsonHttpMessageConverter fastJsonConverter = new FastJsonHttpMessageConverter();
        FastJsonConfig fastJsonConfig = new FastJsonConfig();
        fastJsonConfig.setDateFormat("yyyy-MM-dd HH:mm:ss");
        fastJsonConfig.setReaderFeatures(JSONReader.Feature.TrimString);
        fastJsonConfig.setWriterFeatures(
                JSONWriter.Feature.WriteNulls,
                JSONWriter.Feature.MapSortField,
                JSONWriter.Feature.BrowserCompatible
        );
        fastJsonConverter.setFastJsonConfig(fastJsonConfig);
        fastJsonConverter.setDefaultCharset(StandardCharsets.UTF_8);

        List<MediaType> mediaTypes = new ArrayList<MediaType>();
        mediaTypes.add(MediaType.APPLICATION_JSON);
        mediaTypes.add(MediaType.APPLICATION_JSON_UTF8);
        mediaTypes.add(MediaType.TEXT_PLAIN);
        mediaTypes.add(MediaType.ALL);
        fastJsonConverter.setSupportedMediaTypes(mediaTypes);

        converters.add(0, fastJsonConverter);
    }
}
