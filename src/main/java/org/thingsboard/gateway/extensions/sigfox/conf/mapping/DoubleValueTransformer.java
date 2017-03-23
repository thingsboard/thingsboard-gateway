package org.thingsboard.gateway.extensions.sigfox.conf.mapping;

public class DoubleValueTransformer implements DataValueTransformer<Double> {

    static final String INT_TO_DOUBLE_TRANSFORMER_NAME = "intToDouble";

    private static final int MAX_DOUBLE_VALUE = 65536;
    private static final int DIVIDE_POWER = 10;

    @Override
    public Double transform(Double value) {
        if (value <= MAX_DOUBLE_VALUE) {
            return value / DIVIDE_POWER;
        } else {
            return (MAX_DOUBLE_VALUE - value) / DIVIDE_POWER;
        }
    }

    @Override
    public String getName() {
        return INT_TO_DOUBLE_TRANSFORMER_NAME;
    }
}

