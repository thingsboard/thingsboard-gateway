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
package org.thingsboard.gateway.extensions.mqtt.client.conf.credentials;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.codec.binary.Base64;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.util.StringUtils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;

@Data
@Slf4j
public class CertPemClientCredentials implements MqttClientCredentials {

    private static final String TLS_VERSION = "TLSv1.2";
    private final JcaX509CertificateConverter certificateConverter = new JcaX509CertificateConverter().setProvider("BC");

    private String caCertFileName;
    private String privateKeyFileName;
    private String certFileName;

    private String caCert;
    private String privateKey;
    private String cert;
    private String password;

    @Override
    public void configure(MqttConnectOptions clientOptions) {
        clientOptions.setSocketFactory(getSocketFactory());
    }

    private SSLSocketFactory getSocketFactory() {
        try {
            Security.addProvider(new BouncyCastleProvider());

            TrustManagerFactory trustManagerFactory = createAndInitTrustManagerFactory();
            KeyManagerFactory keyManagerFactory = createAndInitKeyManagerFactory();

            SSLContext context = SSLContext.getInstance(TLS_VERSION);
            context.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

            return context.getSocketFactory();
        } catch (Exception e) {
            log.error("[{}:{}:{}:{}] Creating TLS factory failed!", caCert, cert, privateKey, password, e);
            throw new RuntimeException("Creating TLS factory failed!", e);
        }
    }

    private KeyManagerFactory createAndInitKeyManagerFactory() throws Exception {
        X509Certificate certHolder;
        Object keyObject;
        if (certFileName != null && privateKeyFileName != null) {
            certHolder = readCertFile(cert);
            keyObject = readPrivateKeyFile(privateKey);
        } else {
            certHolder = certificateConverter.getCertificate((X509CertificateHolder) readPEMFile(cert));
            keyObject = readPEMFile(privateKey);
        }

        char[] passwordCharArray = "".toCharArray();
        if (!StringUtils.isEmpty(password)) {
            passwordCharArray = password.toCharArray();
        }

        JcaPEMKeyConverter keyConverter = new JcaPEMKeyConverter().setProvider("BC");

        KeyPair key;
        if (keyObject instanceof PEMEncryptedKeyPair) {
            PEMDecryptorProvider provider = new JcePEMDecryptorProviderBuilder().build(passwordCharArray);
            key = keyConverter.getKeyPair(((PEMEncryptedKeyPair) keyObject).decryptKeyPair(provider));
        } else {
            key = keyConverter.getKeyPair((PEMKeyPair) keyObject);
        }

        KeyStore clientKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        clientKeyStore.load(null, null);
        clientKeyStore.setCertificateEntry("cert", certHolder);
        clientKeyStore.setKeyEntry("private-key",
                key.getPrivate(),
                passwordCharArray,
                new Certificate[]{certHolder});

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(clientKeyStore, passwordCharArray);
        return keyManagerFactory;
    }

    private TrustManagerFactory createAndInitTrustManagerFactory() throws Exception {
        X509Certificate caCertHolder;
        if (caCertFileName != null) {
            caCertHolder = readCertFile(caCert);
        } else {
            caCertHolder = certificateConverter.getCertificate((X509CertificateHolder) readPEMFile(caCert));
        }

        KeyStore caKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        caKeyStore.load(null, null);
        caKeyStore.setCertificateEntry("caCert-cert", caCertHolder);

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(caKeyStore);
        return trustManagerFactory;
    }

    private X509Certificate readCertFile(String fileContent) throws Exception {
        X509Certificate certificate = null;
        if (fileContent != null && !fileContent.trim().isEmpty()) {
            fileContent = fileContent.replace("-----BEGIN CERTIFICATE-----\n", "")
                    .replace("-----END CERTIFICATE-----", "");
            byte[] decoded = Base64.decodeBase64(fileContent);
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
            certificate = (X509Certificate) certFactory.generateCertificate(new ByteArrayInputStream(decoded));
        }
        return certificate;
    }

    private PrivateKey readPrivateKeyFile(String fileContent) throws Exception {
        RSAPrivateKey privateKey = null;
        if (fileContent != null && !fileContent.isEmpty()) {
            fileContent = fileContent.replace("-----BEGIN PRIVATE KEY-----\n", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replaceAll("\\s", "");
            byte[] decoded = Base64.decodeBase64(fileContent);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            privateKey = (RSAPrivateKey) keyFactory.generatePrivate(new PKCS8EncodedKeySpec(decoded));
        }
        return privateKey;
    }

    private Object readPEMFile(String filePath) throws Exception {
        PEMParser reader = new PEMParser(new FileReader(filePath));
        Object fileHolder = reader.readObject();
        reader.close();
        return fileHolder;
    }
}
