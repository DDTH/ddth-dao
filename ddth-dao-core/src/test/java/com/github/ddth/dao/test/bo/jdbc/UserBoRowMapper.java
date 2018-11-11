package com.github.ddth.dao.test.bo.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.github.ddth.dao.jdbc.IRowMapper;
import com.github.ddth.dao.test.bo.UserBo;

public class UserBoRowMapper implements IRowMapper<UserBo> {
    @Override
    public UserBo mapRow(ResultSet rs, int rowNum) throws SQLException {
        UserBo bo = new UserBo();
        bo.setId(rs.getLong("id"));
        bo.setUsername(rs.getString("username"));
        bo.setYob(rs.getInt("yob"));
        bo.setFullname(rs.getString("fullname"));
        bo.setDataDate(rs.getDate("data_date"));
        bo.setDataTime(rs.getTime("data_time"));
        bo.setDataDatetime(rs.getTimestamp("data_datetime"));
        bo.setDataBytes(rs.getBytes("data_bin"));
        bo.setNotNull(rs.getInt("data_notnull"));
        return bo;
    }
}
