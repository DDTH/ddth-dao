package com.github.ddth.dao.test.bo.jdbc;

import com.github.ddth.dao.jdbc.AbstractGenericBoJdbcDao;

public class UserBoJdbcDao extends AbstractGenericBoJdbcDao<UserBo> {
    public static void main(String[] args) {
        UserBoJdbcDao dao = new UserBoJdbcDao();
        System.out.println(dao);
    }
}
