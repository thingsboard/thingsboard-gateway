package org.thingsboard.gateway.extensions.sigfox;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/sigfox")
@ConditionalOnProperty(prefix = "sigfox", value = "enabled", havingValue = "true")
@Slf4j
public class SigfoxController {

    private static final String TOKEN_HEADER = "Authorization";

    @Autowired
    private SigfoxService service;

    @RequestMapping(value = "/{deviceTypeId}", method = RequestMethod.POST)
    public void handleRequest(@PathVariable String deviceTypeId,
                              @RequestHeader(TOKEN_HEADER) String token,
                              @RequestBody String body) throws Exception {
        service.processRequest(deviceTypeId, token, body);
    }
}
