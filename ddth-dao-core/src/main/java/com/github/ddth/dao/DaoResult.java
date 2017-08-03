package com.github.ddth.dao;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Result from a DAO operation.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class DaoResult {
    /**
     * Status code returned from a DAO operation.
     */
    public static enum DaoOperationStatus {
        ERROR(0), SUCCESSFUL(1), DUPLICATED(2), NOT_FOUND(3);

        private final int value;

        DaoOperationStatus(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    private final DaoOperationStatus status;
    private final Object output;

    public DaoResult(DaoOperationStatus status) {
        this(status, null);
    }

    public DaoResult(DaoOperationStatus status, Object output) {
        this.status = status;
        this.output = output;
    }

    public DaoOperationStatus getStatus() {
        return status;
    }

    public Object getOutput() {
        return output;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        tsb.append("status", status).append("output", output);
        return tsb.toString();
    }

}
