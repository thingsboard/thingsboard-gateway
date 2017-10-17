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
package org.thingsboard.gateway.service.gateway;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;

import java.util.Optional;
import java.util.concurrent.*;

/**
 * Created by ashvayka on 23.03.17.
 */
public class MqttDeliveryFuture implements Future<Void> {

    private final IMqttDeliveryToken token;
    private final Exception e;

    public MqttDeliveryFuture(IMqttDeliveryToken token) {
        this.token = token;
        this.e = null;
    }

    public MqttDeliveryFuture(Exception e) {
        this.token = null;
        this.e = e;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        return e != null || token.isComplete();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        return get(Optional.empty());
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return get(Optional.of(unit.toMillis(timeout)));
    }

    private Void get(Optional<Long> duration) throws ExecutionException {
        try {
            if (e != null) {
                throw e;
            } else if (duration.isPresent()) {
                token.waitForCompletion(duration.get());
            } else {
                token.waitForCompletion();
            }
            return null;
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }
}
