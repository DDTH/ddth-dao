package com.github.ddth.dao.test.bo.jdbc;

import com.github.ddth.dao.jdbc.GenericBoJdbcDao;

public class UserBoJdbcDao extends GenericBoJdbcDao<UserBo> {
    public static void main(String[] args) {
        UserBoJdbcDao dao = new UserBoJdbcDao();
        System.out.println(dao);
    }
}
