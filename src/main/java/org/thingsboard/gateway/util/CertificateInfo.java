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
package org.thingsboard.gateway.util;

import lombok.Data;

import java.security.KeyPair;
import java.security.cert.X509Certificate;

/**
 * Created by ashvayka on 16.01.17.
 */
@Data
public class CertificateInfo {

    private final X509Certificate certificate;
    private final KeyPair keyPair;

}
