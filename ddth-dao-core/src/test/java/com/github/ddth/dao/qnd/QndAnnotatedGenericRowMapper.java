package com.github.ddth.dao.qnd;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.github.ddth.dao.BaseBo;
import com.github.ddth.dao.jdbc.AbstractGenericBoJdbcDao;
import com.github.ddth.dao.jdbc.annotations.AnnotatedGenericRowMapper;
import com.github.ddth.dao.jdbc.annotations.ChecksumColumn;
import com.github.ddth.dao.jdbc.annotations.ColumnAttribute;
import com.github.ddth.dao.jdbc.annotations.PrimaryKeyColumns;
import com.github.ddth.dao.jdbc.annotations.UpdateColumns;

public class QndAnnotatedGenericRowMapper {

    public static class MyBo extends BaseBo {
    }

    @PrimaryKeyColumns(columns = { "pk1", "pk2" })
    @UpdateColumns(columns = { "col1", "col2", "col3" })
    @ChecksumColumn("checksum")
    @ColumnAttribute(column = "pk1", attr = "pk1", attrClass = int.class)
    @ColumnAttribute(column = "pk2", attr = "pk2", attrClass = String.class)
    @ColumnAttribute(column = "col1", attr = "col1", attrClass = int.class)
    @ColumnAttribute(column = "col2", attr = "col2", attrClass = float.class)
    @ColumnAttribute(column = "col3", attr = "col3", attrClass = boolean.class)
    @ColumnAttribute(column = "checksum", attr = "checksum", attrClass = long.class)
    public static class MyRowMapper extends AnnotatedGenericRowMapper<MyBo> {
    }

    public static class MyJdbcDao extends AbstractGenericBoJdbcDao<MyBo> {
    }

    public static void main(String[] args) {
        MyRowMapper rowMapper = new MyRowMapper();

        MyJdbcDao jdbcDao = new MyJdbcDao();
        jdbcDao.setRowMapper(rowMapper);
        jdbcDao.init();

        System.out.println(new ToStringBuilder(null).append(rowMapper.getPrimaryKeyColumns()));
        System.out.println(new ToStringBuilder(null).append(rowMapper.getInsertColumns()));
        System.out.println(new ToStringBuilder(null).append(rowMapper.getUpdateColumns()));
        System.out.println(new ToStringBuilder(null).append(rowMapper.getAllColumns()));
        System.out.println(rowMapper.getChecksumColumn());
    }
}
