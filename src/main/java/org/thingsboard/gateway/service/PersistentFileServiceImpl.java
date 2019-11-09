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

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.thingsboard.gateway.service.conf.TbPersistenceConfiguration;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Created by Valerii Sosliuk on 1/2/2018.
 */
@Slf4j
public class PersistentFileServiceImpl implements PersistentFileService {

    private static final String STORAGE_FILE_PREFIX = "tb-gateway-storage-";
    private static final String RESEND_FILE_PREFIX = "tb-gateway-resend-";
    private static final String STORAGE_FILE_NAME_REGEX = STORAGE_FILE_PREFIX + "\\d+";
    private static final String RESEND_FILE_NAME_REGEX = RESEND_FILE_PREFIX + "\\d+";
    public static final String DASH = "-";

    private TbPersistenceConfiguration persistence;
    private String tenantName;

    private ConcurrentLinkedDeque<MqttPersistentMessage> sendBuffer;
    private ConcurrentLinkedDeque<MqttPersistentMessage> resendBuffer;

    private ConcurrentMap<UUID, MqttCallbackWrapper> callbacks;
    private Map<UUID, MqttDeliveryFuture> futures;

    private List<File> storageFiles;
    private List<File> resendFiles;

    private int storageFileCounter;
    private int resendFileCounter;

    private File storageDir;

    @PostConstruct
    public void init() {
        callbacks = new ConcurrentHashMap<>();
        futures = new ConcurrentHashMap<>();
        initStorageDir();
        initFiles();
        initFileCounters();
        initBuffers();
    }

    private void initStorageDir() {
        String storageSubdir = tenantName.replaceAll(" ", "_");
        storageDir = new File(persistence.getPath(), storageSubdir);
        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }
    }

    private void initFiles() {
        storageFiles = getFiles(STORAGE_FILE_NAME_REGEX);
        resendFiles = getFiles(RESEND_FILE_NAME_REGEX);
    }

    private List<File> getFiles(String nameRegex) {
        File[] filesArray = storageDir.listFiles((file) -> {return !file.isDirectory() && file.getName().matches(nameRegex);});
        Arrays.sort(filesArray, Comparator.comparing(File::lastModified));
        return new ArrayList<>(Arrays.asList(filesArray));
    }

    private void initFileCounters() {
        storageFileCounter = getFileCounter(storageFiles);
        resendFileCounter = getFileCounter(resendFiles);
    }

    private int getFileCounter(List<File> files) {
        int counter = 0;
        if (files.isEmpty()) {
            return counter;
        } else {
            String lastFileName = files.get(files.size() - 1).getName();
            int lastFileCounter = Integer.parseInt(lastFileName.substring(lastFileName.lastIndexOf(DASH) + 1));
            if (lastFileCounter == Integer.MAX_VALUE) {
                // rename all storageFiles if overflow?
                counter = 0;
            } else {
                counter = lastFileCounter + 1;
            }
            return counter;
        }
    }

    private void initBuffers() {
        sendBuffer = new ConcurrentLinkedDeque<>();
        resendBuffer = new ConcurrentLinkedDeque<>();
    }

    @Override
    public MqttDeliveryFuture persistMessage(String topic,  int msgId, byte[] payload, String deviceId,
                                             Consumer<Void> onSuccess,
                                             Consumer<Throwable> onFailure) throws IOException {
        MqttPersistentMessage message = MqttPersistentMessage.builder().id(UUID.randomUUID())
                .topic(topic).deviceId(deviceId).messageId(msgId).payload(payload).build();
        MqttDeliveryFuture future = new MqttDeliveryFuture();
        addMessageToBuffer(message);
        callbacks.put(message.getId(), new MqttCallbackWrapper(onSuccess, onFailure));
        futures.put(message.getId(), future);
        return future;
    }

    @Override
    public List<MqttPersistentMessage> getPersistentMessages() throws IOException {
        return getMqttPersistentMessages(storageFiles, sendBuffer);
    }

    @Override
    public List<MqttPersistentMessage> getResendMessages() throws IOException {
        return getMqttPersistentMessages(resendFiles, resendBuffer);
    }

    private List<MqttPersistentMessage> getMqttPersistentMessages(List<File> files, ConcurrentLinkedDeque<MqttPersistentMessage> buffer) throws IOException {
        List<MqttPersistentMessage> messages;
        if (files.isEmpty()) {
            messages = new ArrayList<>(buffer);
            buffer.clear();
            return messages;
        }
        File oldestFile = files.remove(0);
        messages = readFromFile(oldestFile);
        oldestFile.delete();
        return messages;
    }

    @Override
    public void resolveFutureSuccess(UUID id) {
        callbacks.remove(id);
        MqttDeliveryFuture future = futures.remove(id);
        if (future != null) {
            future.complete(Boolean.TRUE);
        }
    }

    @Override
    public void resolveFutureFailed(UUID id, Throwable e) {
        MqttDeliveryFuture future = futures.remove(id);
        if (future != null) {
            future.completeExceptionally(e);
        }
    }

    @Override
    public Optional<MqttDeliveryFuture> getMqttDeliveryFuture(UUID id) {
        return Optional.of(futures.get(id));
    }

    @Override
    public boolean deleteMqttDeliveryFuture(UUID id) {
        return futures.remove(id) != null;
    }

    @Override
    public Optional<Consumer<Void>> getSuccessCallback(UUID id) {
        MqttCallbackWrapper mqttCallbackWrapper = callbacks.get(id);
        if (mqttCallbackWrapper == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mqttCallbackWrapper.getSuccessCallback());
    }

    @Override
    public Optional<Consumer<Throwable>> getFailureCallback(UUID id) {
        MqttCallbackWrapper mqttCallbackWrapper = callbacks.get(id);
        if (mqttCallbackWrapper == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(mqttCallbackWrapper.getFailureCallback());
    }

    @Override
    public void saveForResend(MqttPersistentMessage message) throws IOException {
        if (resendBuffer.size() >= persistence.getBufferSize()) {
            resendFileCounter = getFileCounter(resendFiles);
            resendFiles.add(flushBufferToFile(resendBuffer, RESEND_FILE_PREFIX + resendFileCounter));
        }
        resendBuffer.add(message);
    }

    @Override
    public void saveForResend(List<MqttPersistentMessage> messages) throws IOException {
       for (MqttPersistentMessage message : messages) {
           saveForResend(message);
       }
    }

    private void addMessageToBuffer(MqttPersistentMessage message) throws IOException {
        if (sendBuffer.size() >= persistence.getBufferSize()) {
            storageFiles.add(flushBufferToFile(sendBuffer, STORAGE_FILE_PREFIX + storageFileCounter++));
        }
        sendBuffer.add(message);
    }

    private File flushBufferToFile(ConcurrentLinkedDeque<MqttPersistentMessage> buffer, String fileName) throws IOException {
        ObjectOutputStream outStream = null;
        try {
            File newFile = new File(storageDir, STORAGE_FILE_PREFIX + storageFileCounter);
            outStream = new ObjectOutputStream(new FileOutputStream(newFile));
            for (MqttPersistentMessage message : buffer) {
                outStream.writeObject(message);
            }
            buffer.clear();
            return newFile;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            try {
                if (outStream != null)
                    outStream.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw e;
            }
        }
    }

    private List<MqttPersistentMessage> readFromFile(File file)  throws IOException {
        List<MqttPersistentMessage> messages = new ArrayList<>();
        ObjectInputStream inputStream = null;
        try {
            inputStream = new ObjectInputStream(new FileInputStream(file));
            while (true) {
                MqttPersistentMessage p = (MqttPersistentMessage) inputStream.readObject();
                messages.add(p);
            }
        } catch (EOFException e) {
            return messages;
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            try {
                if (inputStream != null)
                    inputStream.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw e;
            }
        }
        return messages;
    }

    public void setPersistence(TbPersistenceConfiguration persistence) {
        this.persistence = persistence;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }
}
