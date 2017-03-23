package org.thingsboard.gateway.extensions.sigfox;

public interface SigfoxService {
    void processRequest(String deviceTypeId, String token, String body) throws Exception;
}
