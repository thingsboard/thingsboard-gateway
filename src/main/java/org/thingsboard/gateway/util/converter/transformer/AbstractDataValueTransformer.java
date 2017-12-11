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
package org.thingsboard.gateway.util.converter.transformer;

public abstract class AbstractDataValueTransformer implements DataValueTransformer {

    @Override
    public Double transformToDouble(String strValue) {
        throw new UnsupportedOperationException(String.format("%s doesn't support transforming to double value", this.getClass().getSimpleName()));
    }

    @Override
    public Long transformToLong(String strValue) {
        throw new UnsupportedOperationException(String.format("%s doesn't support transforming to long value", this.getClass().getSimpleName()));
    }

    @Override
    public String transformToString(String strValue) {
        throw new UnsupportedOperationException(String.format("%s doesn't support transforming to string value", this.getClass().getSimpleName()));
    }

    @Override
    public Boolean transformToBoolean(String strValue) {
        throw new UnsupportedOperationException(String.format("%s doesn't support transforming to boolean value", this.getClass().getSimpleName()));
    }

    @Override
    public boolean isApplicable(String strValue) {
        return true;
    }
}

