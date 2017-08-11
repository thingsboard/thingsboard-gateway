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
package org.thingsboard.gateway.extensions.file;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.thingsboard.gateway.extensions.file.conf.FileMonitorConfiguration;
import org.thingsboard.gateway.service.GatewayService;
import org.thingsboard.gateway.service.MqttDeliveryFuture;
import org.thingsboard.gateway.service.data.DeviceData;
import org.thingsboard.gateway.util.JsonTools;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by ashvayka on 15.05.17.
 */
@Slf4j
public class FileMonitor {

    private static final int OPERATION_TIMEOUT_IN_SEC = 10;

    private GatewayService gateway;
    private FileMonitorConfiguration configuration;
    private Thread reader;

    public FileMonitor(GatewayService service, FileMonitorConfiguration c) {
        this.gateway = service;
        this.configuration = c;
    }

    public void init() {
        reader = new Thread(new Runnable() {

            private long curPos;
            private int linesToSkip;

            @Override
            public void run() {
                curPos = 0;
                linesToSkip = configuration.getSkipLines();
                while (!Thread.interrupted()) {
                    RandomAccessFile raf = null;
                    try {
                        File f = new File(configuration.getFile());
                        long fileLength = f.length();
                        log.info("Processing iteration. CurPos: {}, File length: {}", curPos, fileLength);
                        if (curPos > fileLength) {
                            log.info("File {} was rotated", f.getAbsolutePath());
                            curPos = 0;
                            linesToSkip = configuration.getSkipLines();
                        }
                        raf = new RandomAccessFile(f, "r");
                        raf.seek(curPos);
                        while (curPos < f.length()) {
                            if (linesToSkip > 0) {
                                String skippedLine = raf.readLine();
                                log.info("Going to skip line: {}", skippedLine);
                                linesToSkip--;
                            } else {
                                String processedLine = raf.readLine();
                                log.info("Going to process line: {}", processedLine);
                                processBody(processedLine, configuration);
                            }
                            curPos = raf.getFilePointer();
                        }
                        raf.close();
                        Thread.sleep(configuration.getUpdateInterval());
                    } catch (Exception e) {
                        log.warn("Exception: {}", e.getMessage(), e);
                        if (raf != null) {
                            try {
                                raf.close();
                            } catch (IOException e1) {
                                log.warn("Failed to close the random access file!", e1);
                            }
                        }
                    }
                }
            }
        });
        reader.start();
    }

    public void stop() {
        if (reader != null) {
            reader.interrupt();
        }
    }

    private void processBody(String body, FileMonitorConfiguration configuration) throws Exception {
        String[] columns = configuration.getCsvColumns();
        String[] csv = body.split(",");

        if (csv.length < columns.length) {
            log.warn("Can't parse following line due to missing columns: {}", body);
            return;
        }

        ObjectNode node = JsonTools.newNode();
        for (int i = 0; i < columns.length; i++) {
            node.put(columns[i], trim(csv[i]));
        }

        DeviceData dd = configuration.getConverter().parseBody(JsonTools.toString(node));
        if (dd != null) {
            waitWithTimeout(gateway.onDeviceConnect(dd.getName(), dd.getType()));
            List<MqttDeliveryFuture> futures = new ArrayList<>();
            if (!dd.getAttributes().isEmpty()) {
                futures.add(gateway.onDeviceAttributesUpdate(dd.getName(), dd.getAttributes()));
            }
            if (!dd.getTelemetry().isEmpty()) {
                futures.add(gateway.onDeviceTelemetry(dd.getName(), dd.getTelemetry()));
            }
            for (Future future : futures) {
                waitWithTimeout(future);
            }
            Optional<MqttDeliveryFuture> future = gateway.onDeviceDisconnect(dd.getName());
            if (future.isPresent()) {
                waitWithTimeout(future.get());
            }
        } else {
            log.error("DeviceData is null. Body [{}] was not parsed successfully!", body);
            throw new IllegalArgumentException("Device Data is null. Body [" + body + "] was not parsed successfully!");
        }
    }

    private String trim(String input) {
        String s = input.trim();
        if (s.startsWith("\"") && s.endsWith("\"")) {
            s = s.substring(1, s.length() - 1);
        }
        return s;
    }

    private void waitWithTimeout(Future future) throws Exception {
        future.get(OPERATION_TIMEOUT_IN_SEC, TimeUnit.SECONDS);
    }
}
