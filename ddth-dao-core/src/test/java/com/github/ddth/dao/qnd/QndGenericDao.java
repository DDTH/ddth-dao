package com.github.ddth.dao.qnd;

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

    public static class Dao extends GenericBoJdbcDao<Bo> {
        public Dao init() {
            if (getRowMapper() == null) {
                setRowMapper(BoMapper.instance);
            }
            super.init();
            return this;
        }
    }

    public static void main(String[] args) {
        try (Dao dao = new Dao()) {
            dao.init();
        }
    }
}
