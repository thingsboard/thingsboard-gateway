package org.thingsboard.gateway.service;

import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.Promise;
import lombok.extern.slf4j.Slf4j;
import nl.jk5.mqtt.ChannelClosedException;
import nl.jk5.mqtt.MqttClient;
import nl.jk5.mqtt.MqttClientConfig;
import nl.jk5.mqtt.MqttConnectResult;
import org.apache.commons.lang3.StringUtils;
import org.thingsboard.gateway.dao.PersistentMqttMessage;
import org.thingsboard.gateway.dao.PersistentMqttMessageRepository;
import org.thingsboard.gateway.service.conf.TbConnectionConfiguration;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 12/12/2017.
 */
@Slf4j
public class MqttMessageSender implements Runnable {

    private MqttClient tbClient;

    private MqttMessageService mqttSenderService;

    private TbPersistenceConfiguration persistence;

    private PersistentMqttMessageRepository messageRepository;

    private Consumer<PersistentMqttMessage> defaultSuccessCallback = message -> log.debug("Successfully sent message: [{}]", message);
    private Consumer<Throwable> defaultFailureCallback = e -> log.warn("Failed to send message: [{}]", e);

    public MqttMessageSender(TbPersistenceConfiguration persistence,
                             MqttClient tbClient,
                             MqttMessageService mqttSenderService,
                             PersistentMqttMessageRepository messageRepository) {
        this.persistence = persistence;
        this.tbClient = tbClient;
        this.mqttSenderService = mqttSenderService;
        this.messageRepository = messageRepository;
    }

    @Override
    public void run() {
        while (true) {
            List<PersistentMqttMessage> storedMessages = null;
            storedMessages = mqttSenderService.getMessages(persistence.getMaxMessagesPerPoll());
            if (!storedMessages.isEmpty()) {
                for (PersistentMqttMessage message : storedMessages) {
                    messageRepository.delete(message);
                    tbClient.publish(message.getTopic(), Unpooled.wrappedBuffer(message.getPayload())).addListener(
                            future -> {
                                if (future.isSuccess()) {
                                    Consumer<PersistentMqttMessage> successCallback = mqttSenderService.getSuccessCallback(message.getId()).orElse(defaultSuccessCallback);
                                    successCallback.accept(message);
                                    mqttSenderService.resolveFutureSuccess(message.getId());
                                } else {
                                    messageRepository.save(new PersistentMqttMessage(message));
                                    /*
                                    if (future.cause() instanceof ChannelClosedException) {
                                        tbClient.c
                                    }
                                    */
                                    mqttSenderService.getFailureCallback(message.getId()).orElse(defaultFailureCallback).accept(future.cause());
                                    mqttSenderService.resolveFutureFailed(message.getId());
                                    log.warn("Failed to send message [{}] due to [{}]", message, future.cause());
                                }
                            }
                    );
                }
            } else {
                try {
                    Thread.sleep(persistence.getPollingInterval());
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
