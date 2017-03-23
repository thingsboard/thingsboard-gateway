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
package org.thingsboard.gateway.extensions.sigfox;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.thingsboard.gateway.extensions.sigfox.conf.SigfoxRequestProcessingError;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@RestController
@RequestMapping("/sigfox")
@ConditionalOnProperty(prefix = "sigfox", value = "enabled", havingValue = "true")
@Slf4j
public class SigfoxController {
    private static final String TOKEN_HEADER = "Authorization";

    @Autowired
    private SigfoxService service;

    private ObjectMapper mapper = new ObjectMapper();


    @RequestMapping(value = "/{deviceTypeId}", method = RequestMethod.POST)
    public void handleRequest(@PathVariable String deviceTypeId,
                              @RequestHeader(TOKEN_HEADER) String token,
                              @RequestBody String body) throws Exception {
        service.processRequest(deviceTypeId, token, body);
    }

    @ExceptionHandler(Exception.class)
    public void handleThingsboardException(Exception exception, HttpServletResponse response) {
        log.debug("Processing exception {}", exception.getMessage(), exception);
        if (!response.isCommitted()) {
            try {
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                if (exception instanceof SecurityException) {
                    response.setStatus(HttpStatus.FORBIDDEN.value());
                    mapper.writeValue(response.getWriter(),
                            new SigfoxRequestProcessingError("You don't have permission to perform this operation!"));
                } else {
                    response.setStatus(HttpStatus.INTERNAL_SERVER_ERROR.value());
                    mapper.writeValue(response.getWriter(), new SigfoxRequestProcessingError(exception.getMessage()));
                }
            } catch (IOException e) {
                log.error("Can't handle exception", e);
            }
        }
    }
}
