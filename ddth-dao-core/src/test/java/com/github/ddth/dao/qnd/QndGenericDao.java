package com.github.ddth.dao.qnd;

import java.lang.reflect.Field;

import com.github.ddth.dao.BaseDataJsonFieldBo;
import com.github.ddth.dao.jdbc.GenericBoJdbcDao;
import com.github.ddth.dao.jdbc.annotations.AnnotatedGenericRowMapper;
import com.github.ddth.dao.jdbc.annotations.ColumnAttribute;
import com.github.ddth.dao.jdbc.annotations.PrimaryKeyColumns;
import com.github.ddth.dao.jdbc.annotations.UpdateColumns;

public class QndGenericDao {
    public static class Bo extends BaseDataJsonFieldBo {
        public int getId() {
            return getAttributeOptional("id", Integer.class).orElse(0);
        }

        public Bo setId(int value) {
            setAttribute("id", value);
            return this;
        }
    }

    @ColumnAttribute(column = "id", attr = "id", attrClass = int.class)
    @ColumnAttribute(column = "data", attr = "data", attrClass = String.class)
    @PrimaryKeyColumns({ "id" })
    @UpdateColumns({ "data" })
    public static class BoMapper extends AnnotatedGenericRowMapper<Bo> {
        public static BoMapper instance = new BoMapper();
    }

    @ColumnAttribute(column = "*", attr = "*", attrClass = Object.class)
    @PrimaryKeyColumns("id")
    public static class BoMapperUniversal extends AnnotatedGenericRowMapper<Bo> {
        public static BoMapperUniversal instance = new BoMapperUniversal();
    }

    public static class Dao extends GenericBoJdbcDao<Bo> {
        public Dao init() {
            if (getRowMapper() == null) {
                setRowMapper(BoMapper.instance);
            }
            super.init();
            return this;
        }
    }

    public static Field getField(Class<?> clazz, String fieldName) {
        try {
            Field f = clazz.getDeclaredField(fieldName);
            f.setAccessible(true);
            return f;
        } catch (NoSuchFieldException e) {
            return null;
        }
    }

    public static void main(String[] args) {
        try (Dao dao = new Dao()) {
            dao.init();

            System.out.println("-= " + dao);
            String[] fieldNames = { "SQL_SELECT_ALL", "SQL_SELECT_ALL_SORTED", "SQL_SELECT_ONE",
                    "SQL_INSERT", "SQL_DELETE_ONE", "SQL_UPDATE_ONE" };
            for (String fieldName : fieldNames) {
                Field f = getField(dao.getClass().getSuperclass(), fieldName);
                try {
                    System.out.println(String.format("%1$-21s", fieldName) + ": " + f.get(dao));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        try (Dao dao = new Dao()) {
            dao.setRowMapper(BoMapperUniversal.instance).init();

            System.out.println("-= " + dao);
            String[] fieldNames = { "SQL_SELECT_ALL", "SQL_SELECT_ALL_SORTED", "SQL_SELECT_ONE",
                    "SQL_INSERT", "SQL_DELETE_ONE", "SQL_UPDATE_ONE" };
            for (String fieldName : fieldNames) {
                Field f = getField(dao.getClass().getSuperclass(), fieldName);
                try {
                    System.out.println(String.format("%1$-21s", fieldName) + ": " + f.get(dao));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
