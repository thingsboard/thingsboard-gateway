/**
 * Copyright © ${project.inceptionYear}-2017 The Thingsboard Authors
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
package org.thingsboard.gateway.extensions.opc.conf.identity;

import lombok.Data;
import org.eclipse.milo.opcua.sdk.client.api.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.UsernameProvider;

/**
 * Created by ashvayka on 16.01.17.
 */
@Data
public class UsernameIdentityProviderConfiguration implements IdentityProviderConfiguration {

    private final String username;
    private final String password;

    @Override
    public IdentityProvider toProvider() {
        return new UsernameProvider(username, password);
    }
}
