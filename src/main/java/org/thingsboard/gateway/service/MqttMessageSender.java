/**
 * Copyright © 2017 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.gateway.service;

import com.google.common.collect.Lists;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;
import nl.jk5.mqtt.MqttClient;
import nl.jk5.mqtt.MqttConnectResult;
import org.thingsboard.gateway.service.conf.TbConnectionConfiguration;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 12/12/2017.
 */
@Slf4j
public class MqttMessageSender implements Runnable {

    private MqttClient tbClient;

    private PersistentFileService persistentFileService;

    private TbPersistenceConfiguration persistence;

    private final TbConnectionConfiguration connection;

    private BlockingQueue<MessageFuturePair> incomingQueue;
    private Queue<Future<Void>> outgoingQueue;

    public MqttMessageSender(TbPersistenceConfiguration persistence,
                             TbConnectionConfiguration connection,
                             MqttClient tbClient,
                             PersistentFileService persistentFileService,
                             BlockingQueue<MessageFuturePair> incomingQueue) {
        this.persistence = persistence;
        this.connection = connection;
        this.tbClient = tbClient;
        this.persistentFileService = persistentFileService;
        this.incomingQueue = incomingQueue;
        outgoingQueue = new ConcurrentLinkedQueue();
    }

    @Override
    public void run() {
        while (true) {
            try {
                checkClientConnected();
                if (!checkOutgoingQueueIsEmpty()) {
                    log.debug("Waiting until all messages are sent before going to the next bucket");
                    Thread.sleep(persistence.getPollingInterval());
                    continue;
                }
                List<MqttPersistentMessage> storedMessages = getMessages();
                if (!storedMessages.isEmpty()) {
                    Iterator<MqttPersistentMessage> iter = storedMessages.iterator();
                    while (iter.hasNext()) {
                        if (!checkClientConnected()) {
                            persistentFileService.saveForResend(Lists.newArrayList(iter));
                            break;
                        }
                        MqttPersistentMessage message = iter.next();
                        log.debug("Sending message [{}]", message);
                        Future<Void> publishFuture = publishMqttMessage(message);
                        outgoingQueue.add(publishFuture);
                    }
                } else {
                    try {
                        Thread.sleep(persistence.getPollingInterval());
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                        Thread.currentThread().interrupt();
                    }
                }
            } catch (Throwable e) {
                log.error(e.getMessage(), e);
            }
        }

    }

    private Future<Void> publishMqttMessage(MqttPersistentMessage message) {
        return tbClient.publish(message.getTopic(), Unpooled.wrappedBuffer(message.getPayload()), MqttQoS.AT_LEAST_ONCE).addListener(
                future -> incomingQueue.put(new MessageFuturePair(future, message))
        );
    }

    private boolean checkOutgoingQueueIsEmpty() {
        if (!outgoingQueue.isEmpty()) {
            int pendingCount = 0;
            boolean allFinished = true;
            for (Future<Void> future : outgoingQueue) {
                if (!future.isDone()) {
                    pendingCount++;
                }
                allFinished &= future.isDone();
            }
            if (allFinished) {
                outgoingQueue = new ConcurrentLinkedQueue();
                return true;
            }
            log.info("Outgoing queue is not empty. [{}] messages are still in progress", pendingCount);
            return false;
        }
        return true;
    }

    private boolean checkClientConnected() throws InterruptedException {
        if (!tbClient.isConnected()) {
            outgoingQueue.forEach(future -> future.cancel(true));
            outgoingQueue.clear();
            log.info("ThingsBoard MQTT connection failed. Reconnecting in [{}] milliseconds", connection.getRetryInterval());
            Thread.sleep(connection.getRetryInterval());
            try {
                MqttConnectResult result = tbClient.reconnect().get(connection.getConnectionTimeout(), TimeUnit.MILLISECONDS);
                if (result.isSuccess()) {
                    log.info("Successfully reconnected to ThingsBoard.");
                }
            } catch (TimeoutException e) {
                log.trace(e.getMessage(), e);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            return false;
        }
        return true;
    }

    private List<MqttPersistentMessage> getMessages() {
        try {
            List<MqttPersistentMessage> resendMessages = persistentFileService.getResendMessages();
            if (!resendMessages.isEmpty()) {
                return resendMessages;
            } else {
                return persistentFileService.getPersistentMessages();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
