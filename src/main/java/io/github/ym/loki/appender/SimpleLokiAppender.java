package io.github.ym.loki.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import cn.hutool.core.net.NetUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import org.jctools.queues.MpscArrayQueue;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 异步 Loki Appender（纯净版）
 * 特性：无锁队列 + 批量聚合 + HTTP推送 + 线程封闭缓冲
 */
@SuppressWarnings("unused")
public class SimpleLokiAppender extends AppenderBase<ILoggingEvent> {

    private static final String HOST;

    static {
        String ip = null;
        try {
            ip = NetUtil.getLocalhost().getHostAddress();
        } catch (Exception ignored) {}
        HOST = StrUtil.isNotBlank(ip) ? ip : "unknown";
    }

    // ==================== XML 配置属性 ====================
    private String lokiUrl;
    private int queueCapacity = 8192;
    private int batchSize = 100;
    private long flushIntervalMs = 1000L;

    @SuppressWarnings("unused")
    public void setLokiUrl(String lokiUrl) {
        if (StrUtil.isBlank(lokiUrl)) {
            throw new IllegalArgumentException("lokiUrl 不能为空");
        }
        this.lokiUrl = lokiUrl.trim();
    }

    @SuppressWarnings("unused")
    public void setQueueCapacity(int cap) {
        if (cap <= 0) throw new IllegalArgumentException("queueCapacity 必须为正整数，当前值: " + cap);
        this.queueCapacity = cap;
    }

    @SuppressWarnings("unused")
    public void setBatchSize(int size) {
        if (size <= 0) throw new IllegalArgumentException("batchSize 必须为正整数，当前值: " + size);
        this.batchSize = size;
    }

    @SuppressWarnings("unused")
    public void setFlushIntervalMs(long ms) {
        if (ms <= 0) throw new IllegalArgumentException("flushIntervalMs 必须为正整数，当前值: " + ms);
        this.flushIntervalMs = ms;
    }

    // ==================== 内部运行时状态 ====================
    private MpscArrayQueue<ILoggingEvent> queue;
    private volatile boolean running = false;
    private Thread consumerThread;
    private volatile long lastFlushTime = System.currentTimeMillis();

    private final AtomicLong lastWarnTime = new AtomicLong(0);
    private static final long WARN_THROTTLE_MS = 30_000L;

    // ==================== 生命周期 ====================
    @Override
    public void start() {
        if (isStarted()) return;
        if (StrUtil.isBlank(lokiUrl)) {
            addError("[Loki] 启动失败: 缺少必填配置项 lokiUrl");
            return;
        }
        this.queueCapacity = Integer.highestOneBit(Math.max(queueCapacity, 2) - 1) << 1;
        this.queue = new MpscArrayQueue<>(queueCapacity);

        this.running = true;
        this.lastFlushTime = System.currentTimeMillis();
        this.consumerThread = new Thread(this::consumeLoop, "loki-appender-consumer");
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();

        super.start();
        addInfo("[Loki] 异步Appender启动成功 | 队列容量:" + queueCapacity + " | 批次大小:" + batchSize);
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (!running) return;
        event.prepareForDeferredProcessing();
        if (!queue.offer(event)) {
            long now = System.currentTimeMillis();
            if (now - lastWarnTime.get() > WARN_THROTTLE_MS) {
                lastWarnTime.set(now);
                addWarn("[Loki] 内存队列已满，当前日志被丢弃！请检查Loki服务端状态或调大队列容量");
            }
        }
    }

    @Override
    public void stop() {
        if (!isStarted()) return;
        running = false;

        if (consumerThread != null) {
            consumerThread.interrupt();
            try { consumerThread.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        List<ILoggingEvent> remaining = new ArrayList<>();
        ILoggingEvent event;
        while ((event = queue.poll()) != null) remaining.add(event);
        if (!remaining.isEmpty()) flushBuffer(remaining);

        super.stop();
        addInfo("[Loki] 异步Appender已安全停止，队列残余日志已尝试刷出");
    }

    // ==================== 消费循环 ====================
    private void consumeLoop() {
        List<ILoggingEvent> buffer = new ArrayList<>(batchSize);

        while (true) {
            if (!buffer.isEmpty() && (System.currentTimeMillis() - lastFlushTime) >= flushIntervalMs) {
                flushBuffer(buffer);
                buffer.clear();
                lastFlushTime = System.currentTimeMillis();
            }

            ILoggingEvent event = queue.poll();
            if (event == null) {
                if (!running && queue.isEmpty()) break;
                LockSupport.parkNanos(10_000_000L);
                if (Thread.currentThread().isInterrupted()) break;
                continue;
            }

            buffer.add(event);
            if (buffer.size() >= batchSize) {
                flushBuffer(buffer);
                buffer.clear();
                lastFlushTime = System.currentTimeMillis();
            }
        }

        if (!buffer.isEmpty()) flushBuffer(buffer);
    }

    // ==================== HTTP 推送 ====================
    private void flushBuffer(List<ILoggingEvent> batch) {
        if (batch == null || batch.isEmpty()) return;

        JSONObject payload = buildPayload(batch);
        try (HttpResponse response = HttpRequest.post(lokiUrl)
                .header("Content-Type", "application/json")
                .body(JSONUtil.toJsonStr(payload))
                .timeout(2000)
                .execute()) {
            int status = response.getStatus();
            if (status != 200 && status != 204) {
                throttleWarn("[Loki] HTTP推送失败，状态码: " + status);
            }
        } catch (Exception e) {
            throttleWarn("[Loki] HTTP推送异常: " + e.getMessage());
        }
    }

    private void throttleWarn(String msg) {
        long now = System.currentTimeMillis();
        if (now - lastWarnTime.get() > WARN_THROTTLE_MS) {
            lastWarnTime.set(now);
            addWarn(msg);
        }
    }

    // ==================== Payload 构建 ====================
    private JSONObject buildPayload(List<ILoggingEvent> events) {
        Map<Map<String, String>, List<ILoggingEvent>> grouped = new LinkedHashMap<>();
        for (ILoggingEvent e : events) {
            grouped.computeIfAbsent(resolveLabels(e), k -> new ArrayList<>()).add(e);
        }

        JSONArray streams = new JSONArray();
        for (Map.Entry<Map<String, String>, List<ILoggingEvent>> entry : grouped.entrySet()) {
            JSONObject streamObj = new JSONObject();
            JSONObject labelObj = new JSONObject();
            entry.getKey().forEach(labelObj::set);
            streamObj.set("stream", labelObj);

            JSONArray values = new JSONArray();
            for (ILoggingEvent e : entry.getValue()) {
                JSONArray value = new JSONArray();
                value.add(String.valueOf(e.getTimeStamp() * 1_000_000L));
                value.add(e.getFormattedMessage());
                values.add(value);
            }
            streamObj.set("values", values);
            streams.add(streamObj);
        }

        JSONObject root = new JSONObject();
        root.set("streams", streams);
        return root;
    }

    private Map<String, String> resolveLabels(ILoggingEvent event) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("level", event.getLevel().toString());
        labels.put("logger_name", event.getLoggerName());
        labels.put("host", HOST);

        Map<String, String> mdc = event.getMDCPropertyMap();
        if (mdc != null) {
            mdc.forEach((k, v) -> {
                if (k.startsWith("loki_") && StrUtil.isNotBlank(v)) {
                    labels.put(k.substring(5), v);
                }
            });
        }
        return Collections.unmodifiableMap(labels);
    }
}