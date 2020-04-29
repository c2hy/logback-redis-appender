package com.github.suzor.logback.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    private final BlockingQueue<ILoggingEvent> logQueue = new ArrayBlockingQueue<>(1000 * 10);
    private final AtomicBoolean start = new AtomicBoolean(true);
    private final StatefulRedisConnection<String, String> redisConnection = this.createRedisConnection();

    private Encoder<ILoggingEvent> encoder;
    private String key = "logback:logs";
    private String hostname = "localhost";
    private String password;
    private int port = 6379;
    private int database = 0;

    @Override
    public void start() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> start.set(false)));
        this.consumeLog();
        super.start();
    }

    @Override
    protected void append(ILoggingEvent iLoggingEvent) {
        if (!logQueue.offer(iLoggingEvent)) {
            System.err.println("Queue Fulled");
            System.out.println(Arrays.toString(this.getEncoder().encode(iLoggingEvent)));
        }
    }

    public void consumeLog() {
        CompletableFuture.runAsync(() -> {
            while (start.get()) {
                this.pushLog(this.redisConnection);
            }
        });
    }

    private void pushLog(StatefulRedisConnection<String, String> redisConnection) {
        try {
            ILoggingEvent event = this.logQueue.take();
            redisConnection.async().rpush(this.getKey(), new String(this.getEncoder().encode(event), StandardCharsets.UTF_8));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private StatefulRedisConnection<String, String> createRedisConnection() {
        RedisURI.Builder redisUriBuilder = RedisURI.Builder.redis(this.getHostname());
        if (this.getPassword() != null && !this.getPassword().isEmpty()) {
            redisUriBuilder.withPassword(this.getPassword());
        }
        redisUriBuilder.withPort(this.getPort());
        redisUriBuilder.withDatabase(this.getDatabase());
        return RedisClient.create(redisUriBuilder.build()).connect();
    }

    public Encoder<ILoggingEvent> getEncoder() {
        return encoder;
    }

    public void setEncoder(Encoder<ILoggingEvent> encoder) {
        this.encoder = encoder;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }
}
