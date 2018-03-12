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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.*;
import java.security.cert.X509Certificate;

/**
 * Created by ashvayka on 16.01.17.
 */
@Slf4j
public class ConfigurationTools {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static <T> T readConfiguration(JsonNode configurationNode, Class<T> clazz) throws IOException {
        try {
            return mapper.treeToValue(configurationNode, clazz);
        } catch (IOException e) {
            log.error("Failed to load {} configuration from {}", clazz, configurationNode);
            throw e;
        }
    }

    public static <T> T readFileConfiguration(String configurationFile, Class<T> clazz) throws IOException {
        try {
            return mapper.readValue(getFileAsStream(configurationFile), clazz);
        } catch (IOException e) {
            log.error("Failed to load {} configuration from {}", clazz, configurationFile);
            throw e;
        }
    }

    public static CertificateInfo loadCertificate(KeystoreConfiguration configuration, Boolean isRemote) throws GeneralSecurityException, IOException {
        try {
            KeyStore keyStore = KeyStore.getInstance(configuration.getType());
            if (isRemote) {
                keyStore.load(getResourceAsStream(configuration.getFileContent()), configuration.getPassword().toCharArray());
            } else {
                keyStore.load(getFileAsStream(configuration.getLocation()), configuration.getPassword().toCharArray());
            }

            Key key = keyStore.getKey(configuration.getAlias(), configuration.getKeyPassword().toCharArray());
            if (key instanceof PrivateKey) {
                X509Certificate certificate = (X509Certificate) keyStore.getCertificate(configuration.getAlias());
                PublicKey publicKey = certificate.getPublicKey();
                KeyPair keyPair = new KeyPair(publicKey, (PrivateKey) key);
                return new CertificateInfo(certificate, keyPair);
            } else {
                throw new GeneralSecurityException(configuration.getAlias() + " is not a private key!");
            }
        } catch (IOException | GeneralSecurityException e) {
            log.error("Keystore configuration: [{}] is invalid!", configuration, e);
            throw e;
        }
    }

    private static InputStream getResourceAsStream(String fileContent) {
        byte[] decoded = Base64.decodeBase64(fileContent);
        return new ByteArrayInputStream(decoded);
    }

    private static InputStream getFileAsStream(String configurationFile) {
        return ConfigurationTools.class.getClassLoader().getResourceAsStream(configurationFile);
    }
}
