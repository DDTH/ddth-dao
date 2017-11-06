package com.github.ddth.dao.qnd;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.ddth.dao.utils.JdbcHelper;

public class QndDbcHelperBindParams {

    public static void main(String[] args) throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/test?useSSL=false",
                "travis", "");
        try {
            // PreparedStatement pstm = conn
            // .prepareStatement("SELECT * FROM tbl_test WHERE col_int=?");
            PreparedStatement pstm = conn
                    .prepareStatement("SELECT * FROM tbl_test WHERE col_int IN (?)");
            Array arr = pstm.getConnection().createArrayOf("VARCHAR", new Object[] { 1, 2, 3 });
            pstm.setArray(1, arr);
            JdbcHelper.bindParams(pstm, 1);
            ResultSet rs = pstm.executeQuery();
            while (rs.next()) {
                System.out.println(rs.getObject("col_int"));
            }
        } finally {
            conn.close();
        }

    }

}
