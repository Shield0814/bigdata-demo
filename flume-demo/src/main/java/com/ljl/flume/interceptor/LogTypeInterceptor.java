package com.ljl.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 自定义flume拦截器：
 * 配合 channel selector 使用,对event进行过滤或简单加工
 * 要点：
 * 1. 实现Interceptor接口
 * 2. 实现Interceptor.Builder接口
 */
public class LogTypeInterceptor implements Interceptor {

    private int startCounter = 0;
    private int eventCounter = 0;

    private Logger logger = Logger.getLogger(LogTypeInterceptor.class);

    /**
     * 初始化操作
     */
    @Override
    public void initialize() {
    }

    /**
     * 对输入的单个event的进行过滤或加工
     *
     * @param event
     * @return 如果返回null则该event不会进入任何channel
     */
    @Override
    public Event intercept(Event event) {
        String log = new String(event.getBody(), Charset.forName("UTF-8"));
        Map<String, String> headers = new HashMap<>();
        if (log.contains("start")) {
            headers.put("topic", "topic_start");
            startCounter++;
        } else {
            headers.put("topic", "topic_event");
            eventCounter++;
        }
        event.setHeaders(headers);
        return event;
    }

    /**
     * 对输入的多个个event的进行过滤或加工
     *
     * @param events
     * @return
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        return events.stream()
                .map(event -> intercept(event))
                .collect(Collectors.toList());
    }

    /**
     * 拦截器使用结束后进行的操作
     */
    @Override
    public void close() {
        logger.info("topic_start counter output: " + startCounter);
        logger.info("topic_event counter output: " + eventCounter);
    }

    /**
     * 用于在配置文件中创建拦截器，eg:
     * a1.sources.r1.interceptors = i1 i2
     * a1.sources.r1.interceptors.i1.type = com.ljl.flume.interceptor.LogETLInterceptor$Builder
     * a1.sources.r1.interceptors.i2.type = com.ljl.flume.interceptor.LogTypeInterceptor$Builder
     * <p>
     * a1.sources.r1.selector.type = multiplexing
     * a1.sources.r1.selector.header = topic
     * a1.sources.r1.selector.mapping.topic_start = c1
     * a1.sources.r1.selector.mapping.topic_event = c2
     */
    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
