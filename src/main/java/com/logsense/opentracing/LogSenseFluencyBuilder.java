package com.logsense.opentracing;

import org.komamitsu.fluency.Fluency;
import org.komamitsu.fluency.fluentd.FluencyBuilderForFluentd;
import org.komamitsu.fluency.fluentd.ingester.FluentdIngester;
import org.komamitsu.fluency.fluentd.ingester.sender.FluentdSender;
import org.komamitsu.fluency.fluentd.ingester.sender.MultiSender;
import org.komamitsu.fluency.fluentd.ingester.sender.RetryableSender;
import org.komamitsu.fluency.fluentd.ingester.sender.SSLSender;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.FailureDetector;
import org.komamitsu.fluency.fluentd.ingester.sender.failuredetect.PhiAccrualFailureDetectStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.heartbeat.SSLHeartbeater;
import org.komamitsu.fluency.fluentd.ingester.sender.retry.ExponentialBackOffRetryStrategy;
import org.komamitsu.fluency.fluentd.ingester.sender.retry.RetryStrategy;
import org.komamitsu.fluency.fluentd.recordformat.FluentdRecordFormatter;
import org.komamitsu.fluency.ingester.Ingester;
import org.komamitsu.fluency.recordformat.RecordFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The purpose of this class is to have a dedicated, quiet data emitter
 */
public class LogSenseFluencyBuilder extends FluencyBuilderForFluentd {
    private boolean heartbeatEnabled = false;

    public static class LogSenseMultiSender extends MultiSender {
        public LogSenseMultiSender(List<FluentdSender> senders) { super(senders); }

        @Override
        protected synchronized void sendInternal(List<ByteBuffer> buffers, byte[] ackToken)
                throws AllNodesUnavailableException
        {
            for (FluentdSender sender : getSenders()) {
                boolean isAvailable = sender.isAvailable();
                if (isAvailable) {
                    try {
                        if (ackToken == null) {
                            sender.send(buffers);
                        }
                        else {
                            sender.sendWithAck(buffers, ackToken);
                        }
                        return;
                    }
                    catch (IOException e) {
                        // Skip silently
                    }
                }
            }
            throw new AllNodesUnavailableException("All nodes are unavailable");
        }
    }

    public static class LogSenseRetryableSender extends RetryableSender {
        private static final Logger LOG = LoggerFactory.getLogger(RetryableSender.class);

        public LogSenseRetryableSender(Config config, FluentdSender baseSender, RetryStrategy retryStrategy)
        {
            super(config, baseSender, retryStrategy);
        }

        @Override
        protected synchronized void sendInternal(List<ByteBuffer> buffers, byte[] ackToken)
                throws IOException
        {
            IOException firstException = null;

            int retry = 0;
            while (!getRetryStrategy().isRetriedOver(retry)) {
                if (isClosed()) {
                    throw new RetryOverException("This sender is already closed", firstException);
                }

                try {
                    if (ackToken == null) {
                        getBaseSender().send(buffers);
                    }
                    else {
                        getBaseSender().sendWithAck(buffers, ackToken);
                    }
                    return;
                }
                catch (IOException e) {
                    firstException = e;
                }

                try {
                    TimeUnit.MILLISECONDS.sleep(getRetryStrategy().getNextIntervalMillis(retry));
                }
                catch (InterruptedException e) {
                    LOG.debug("Interrupted while waiting", e);
                    Thread.currentThread().interrupt();
                }
                retry++;
            }

            throw new RetryOverException("Sending data was retried over", firstException);
        }
    }

    private RecordFormatter buildRecordFormatter()
    {
        return new FluentdRecordFormatter();
    }

    private Ingester buildIngester(FluentdSender baseSender)
    {
        ExponentialBackOffRetryStrategy.Config retryStrategyConfig =
                new ExponentialBackOffRetryStrategy.Config();

        if (getSenderMaxRetryCount() != null) {
            retryStrategyConfig.setMaxRetryCount(getSenderMaxRetryCount());
        }

        if (getSenderMaxRetryCount() != null) {
            retryStrategyConfig.setMaxRetryCount(getSenderMaxRetryCount());
        }

        RetryableSender.Config senderConfig = new RetryableSender.Config();

        if (getErrorHandler() != null) {
            senderConfig.setErrorHandler(getErrorHandler());
        }

        LogSenseRetryableSender retryableSender = new LogSenseRetryableSender(senderConfig, baseSender,
                new ExponentialBackOffRetryStrategy(retryStrategyConfig));

        FluentdIngester.Config ingesterConfig = new FluentdIngester.Config();
        ingesterConfig.setAckResponseMode(isAckResponseMode());

        return new FluentdIngester(ingesterConfig, retryableSender);
    }

    private FluentdSender createBaseSender(String host, Integer port)
    {
        SSLSender.Config senderConfig = new SSLSender.Config();
        FailureDetector failureDetector = null;

        if (host != null) {
            senderConfig.setHost(host);
        }
        if (port != null) {
            senderConfig.setPort(port);
        }
        if (heartbeatEnabled) {
            SSLHeartbeater.Config hbConfig = new SSLHeartbeater.Config();
            hbConfig.setHost(host);
            hbConfig.setPort(port);
            SSLHeartbeater heartbeater = new SSLHeartbeater(hbConfig);
            failureDetector = new FailureDetector(new PhiAccrualFailureDetectStrategy(), heartbeater);
        }
        if (getConnectionTimeoutMilli() != null) {
            senderConfig.setConnectionTimeoutMilli(getConnectionTimeoutMilli());
        }
        if (getReadTimeoutMilli() != null) {
            senderConfig.setReadTimeoutMilli(getReadTimeoutMilli());
        }
        return new SSLSender(senderConfig, failureDetector);
    }

    /**
     * @deprecated - SSL is always enabled for this implementation
     */
    public boolean isSslEnabled()
    {
        return true;
    }

    /**
     * @deprecated - SSL is always enabled for this implementation
     */
    public void setSslEnabled(boolean sslEnabled) { }

    public void setHeartbeatEnabled(boolean heartbeatEnabled) {
        this.heartbeatEnabled = heartbeatEnabled;
    }

    public boolean isHeartbeatEnabled() {
        return heartbeatEnabled;
    }

    public Fluency build(String host, int port)
    {
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(createBaseSender(host, port)));
    }

    public Fluency build(int port)
    {
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(createBaseSender(null, port)));
    }

    public Fluency build()
    {
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(createBaseSender(null, null)));
    }

    public Fluency build(List<InetSocketAddress> servers)
    {
        List<FluentdSender> senders = new ArrayList<>();
        for (InetSocketAddress server : servers) {
            senders.add(createBaseSender(server.getHostName(), server.getPort()));
        }
        return buildFromIngester(
                buildRecordFormatter(),
                buildIngester(new LogSenseMultiSender(senders)));
    }
}
