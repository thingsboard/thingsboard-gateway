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
package org.thingsboard.gateway.service.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.thingsboard.gateway.service.RpcCommandListener;

/**
 * Created by ashvayka on 23.02.17.
 */
@Data
@AllArgsConstructor
public class RpcCommandSubscription {

    private String deviceNameFilter;
    private RpcCommandListener listener;

    public boolean matches(String deviceName) {
        return deviceName.matches(deviceNameFilter);
    }

}
