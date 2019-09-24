package com.ljl.flume.interceptor;


import com.ljl.flume.util.LogUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;
import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

public class LogETLInterceptor implements Interceptor {

    private static final Logger logger = Logger.getLogger(LogETLInterceptor.class);

    private int failCounter = 0;

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String log = new String(event.getBody(), Charset.forName("UTF-8"));
        if (LogUtils.validateEvent(log)) {
            return event;
        } else {
            failCounter++;
        }
        return null;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return list.stream()
                .map(event -> new String(event.getBody(), Charset.forName("UTF-8")))
                .filter(LogUtils::validateEvent)
                .map(log -> {
                    SimpleEvent event = new SimpleEvent();
                    event.setBody(log.getBytes());
                    return event;
                }).collect(Collectors.toList());

    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new LogETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
