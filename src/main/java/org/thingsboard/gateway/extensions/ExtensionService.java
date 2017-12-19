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
package org.thingsboard.gateway.extensions;

import org.thingsboard.gateway.service.conf.TbExtensionConfiguration;

/**
 * Created by ashvayka on 29.09.17.
 */
public interface ExtensionService {

    TbExtensionConfiguration getCurrentConfiguration();

    void init(TbExtensionConfiguration configuration, Boolean isRemote) throws Exception;

    void update(TbExtensionConfiguration configuration) throws Exception;

    void destroy() throws Exception;
}
