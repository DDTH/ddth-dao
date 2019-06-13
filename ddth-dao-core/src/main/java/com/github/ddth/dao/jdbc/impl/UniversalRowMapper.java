package com.github.ddth.dao.jdbc.impl;

import com.github.ddth.dao.jdbc.IRowMapper;
import com.github.ddth.dao.jdbc.annotations.AnnotatedGenericRowMapper;
import com.github.ddth.dao.utils.JdbcHelper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Universal implementation of {@link IRowMapper} that transform a {@link ResultSet}'s row to
 * {@code Map<String, Object>}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.3
 */
public class UniversalRowMapper extends AnnotatedGenericRowMapper<Map<String, Object>>
        implements IRowMapper<Map<String, Object>> {

    public final static UniversalRowMapper INSTANCE = new UniversalRowMapper();

    private ThreadLocal<Cache<ResultSet, String[]>> localColNames = ThreadLocal
            .withInitial(() -> CacheBuilder.newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS).build());

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        Cache<ResultSet, String[]> cache = localColNames.get();
        try {
            String[] colLabels = cache.get(rs, () -> JdbcHelper.extractColumnLabels(rs));
            Map<String, Object> row = new HashMap<>();
            for (int i = 0; i < colLabels.length; i++) {
                row.put(colLabels[i], rs.getObject(colLabels[i]));
            }
            return row;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
