package com.zhiyou100.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


public class DBUtil {
    static Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    public static Connection getConnection(){
        try {
            conn = DriverManager.getConnection(MyProperties.getUrl(),MyProperties.getUser(),MyProperties.getPassword());
            if (!conn.isClosed()) {
                System.out.println("success");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }
    public ResultSet Query(String sql){
        try {
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }
    public int Update(String sql,Object[] o){
        int flag=0;
        try {
            ps = conn.prepareStatement(sql);
            for(int i =0;i<o.length;i++){
                ps.setObject(i+1, o[i]);
            }
            flag = ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return flag;
    }
    public void close(){
        try {
            if (rs!=null) {
                rs.close();
            }
            if (ps!=null) {
                ps.close();
            }
            if (conn!=null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
