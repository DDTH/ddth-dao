package com.github.ddth.dao.test.bo.jdbc;

import java.util.Date;

import com.github.ddth.dao.jdbc.annotations.AnnotatedGenericRowMapper;
import com.github.ddth.dao.jdbc.annotations.ColumnAttribute;
import com.github.ddth.dao.jdbc.annotations.PrimaryKeyColumns;
import com.github.ddth.dao.jdbc.annotations.UpdateColumns;

@ColumnAttribute(column = "id", attr = "id", attrClass = long.class)
@ColumnAttribute(column = "username", attr = "username", attrClass = String.class)
@ColumnAttribute(column = "yob", attr = "yob", attrClass = Integer.class)
@ColumnAttribute(column = "fullname", attr = "fullname", attrClass = String.class)
@ColumnAttribute(column = "data_date", attr = "dataDate", attrClass = Date.class)
@ColumnAttribute(column = "data_time", attr = "dataTime", attrClass = Date.class)
@ColumnAttribute(column = "data_datetime", attr = "dataDatetime", attrClass = Date.class)
@ColumnAttribute(column = "data_bin", attr = "dataBytes", attrClass = byte[].class)
@ColumnAttribute(column = "data_notnull", attr = "notNull", attrClass = int.class)
@PrimaryKeyColumns({ "id" })
@UpdateColumns({ "id", "username", "yob", "fullname", "data_date", "data_time", "data_datetime",
        "data_bin", "data_notnull" })
public class GenericUserBoRowMapper extends AnnotatedGenericRowMapper<UserBo> {
}
