package com.github.ddth.dao.jdbc.impl;

import com.github.ddth.dao.jdbc.IRowMapper;
import com.github.ddth.dao.utils.DaoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Iterator;

/**
 * {@link Iterator} implementation that support iterating through a {@link ResultSet}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.3
 */
public class ResultSetIterator<T> implements Iterator<T>, Cloneable, AutoCloseable {

    private Logger LOGGER = LoggerFactory.getLogger(ResultSetIterator.class);

    protected Connection connection;
    protected ResultSet resultSet;
    protected ResultSetMetaData resultSetMetaData;
    protected Statement selectStatement;
    protected IRowMapper<T> rowMapper;
    protected int rowNum = 0;
    protected boolean closed = false;

    /**
     * Construct a new {@link ResultSetIterator} object, supplied with an open
     * {@link ResultSet}.
     *
     * @param rowMapper
     * @param resultSet must be an open {@link ResultSet}. This result set will be closed by this
     *                  iterator.
     */
    public ResultSetIterator(IRowMapper<T> rowMapper, ResultSet resultSet) {
        this.rowMapper = rowMapper;
        this.resultSet = resultSet;
    }

    /**
     * Construct a new {@link ResultSetIterator} object, supplied with an open
     * {@link ResultSet}.
     *
     * @param conn
     * @param rowMapper
     * @param resultSet must be an open {@link ResultSet}. This result set will be closed by this
     *                  iterator.
     */
    public ResultSetIterator(Connection conn, IRowMapper<T> rowMapper, ResultSet resultSet) {
        this.connection = conn;
        this.rowMapper = rowMapper;
        this.resultSet = resultSet;
    }

    /**
     * Construct a new {@link ResultSetIterator} object, supplied with a ready-to-execute
     * SELECT-{@link Statement}.
     *
     * @param rowMapper
     * @param selectStatement must be a ready-to-execute SELECT statement
     */
    public ResultSetIterator(IRowMapper<T> rowMapper, PreparedStatement selectStatement) {
        this.rowMapper = rowMapper;
        this.selectStatement = selectStatement;
    }

    /**
     * Construct a new {@link ResultSetIterator} object, supplied with a ready-to-execute
     * SELECT-{@link Statement}.
     *
     * @param conn
     * @param rowMapper
     * @param selectStatement must be a ready-to-execute SELECT statement
     */
    public ResultSetIterator(Connection conn, IRowMapper<T> rowMapper, PreparedStatement selectStatement) {
        this.connection = conn;
        this.rowMapper = rowMapper;
        this.selectStatement = selectStatement;
    }

    protected void init() {
        if (closed) {
            return;
        }
        try {
            if (resultSet == null && selectStatement == null) {
                throw new IllegalStateException("Both result-set and select-statement are null.");
            }
            if (selectStatement == null) {
                selectStatement = resultSet.getStatement();
            }
            if (resultSet == null) {
                resultSet = ((PreparedStatement) selectStatement).executeQuery();
            }
        } catch (Exception e) {
            close();
            throw e instanceof DaoException ? (DaoException) e : new DaoException(e);
        }
    }

    public void close() {
        if (!closed) {
            try {
                if (resultSet != null)
                    try {
                        resultSet.close();
                    } catch (Exception e) {
                        LOGGER.warn(e.getMessage(), e);
                    }
                if (selectStatement != null)
                    try {
                        selectStatement.close();
                    } catch (Exception e) {
                        LOGGER.warn(e.getMessage(), e);
                    }
                if (connection != null) {
                    try {
                        connection.setAutoCommit(true);
                    } catch (Exception e) {
                        LOGGER.warn(e.getMessage(), e);
                    }
                    try {
                        connection.close();
                    } catch (Exception e) {
                        LOGGER.warn(e.getMessage(), e);
                    }
                }
            } finally {
                closed = true;
            }
        }
    }

    protected Connection getConnection() {
        return connection;
    }

    protected ResultSet getResultSet() {
        return resultSet;
    }

    protected ResultSetMetaData getResultSetMetaData() {
        return resultSetMetaData;
    }

    protected Statement getSelectStatement() {
        return selectStatement;
    }

    /**
     * Is this iterator closed?
     *
     * @return
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Get current row number.
     *
     * @return
     */
    public int getRowNum() {
        return rowNum;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        if (closed)
            return false;
        try {
            if (resultSet == null || selectStatement == null) {
                init();
            }
            boolean hasMore = resultSet.next();
            if (!hasMore) {
                close();
            }
            return hasMore;
        } catch (Exception e) {
            close();
            throw e instanceof DaoException ? (DaoException) e : new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T next() {
        if (closed)
            throw new IllegalStateException("This iterator has been closed.");
        try {
            return rowMapper.mapRow(resultSet, rowNum);
        } catch (Exception e) {
            close();
            throw e instanceof DaoException ? (DaoException) e : new DaoException(e);
        } finally {
            rowNum++;
        }
    }
}
