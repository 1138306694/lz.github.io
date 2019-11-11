package com.zhiyou100.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HTableUtil {
    static Configuration conf = null;
    static Connection conn = null;
    static Admin admin = null;

    static {
        conn = getConnection();
    }

    /**
     * 获得连接
     *
     * @return
     */
    public static Connection getConnection() {
        //创建hbase连接配置
        Configuration conf = HBaseConfiguration.create();
        //设置zookeeper集群连接配置
        conf.set(MyProperties.getZookeeper_name(),MyProperties.getZookeeper_value());
        //连接工厂创建连接
        try {
            conn = ConnectionFactory.createConnection(conf);
            System.out.println("connect hbase success");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭连接
     */
    public static void close() {
        try {
            if (conn != null) {
                conn.close();
            }
            if (admin != null) {
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据表名创建hbaseTable
     *
     * @param tableName 表名
     * @param familys   列族名
     */
    public void createTable(String tableName, String[] familys) {
        try {
            //获取表
            admin = conn.getAdmin();
            //定义表名
            TableName tName = TableName.valueOf(tableName);
            HTableDescriptor tableDesc = new HTableDescriptor(tName);
            for (String family : familys) {
                //定义列族
                HColumnDescriptor colDesc = new HColumnDescriptor(family.getBytes());
                tableDesc.addFamily(colDesc);
            }
            //直接创建表
            admin.createTable(tableDesc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入一条记录
     *
     * @param tableName 表名
     * @param rowKey    行
     * @param family    列族
     * @param qualifier 列
     * @param value     值
     */
    public void addOneRecord(String tableName, String rowKey, String family, String qualifier, String value) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            //行号
            Put put = new Put(Bytes.toBytes(rowKey));
            //指定列族、列、值
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                    System.out.println("close table");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 删除row列族下的某列值
     *
     * @param tableName 表名
     * @param rowKey    行号
     * @param family    列族
     * @param qualifier 列
     */
    public void deleteQualifierValue(String tableName, String rowKey, String family, String qualifier) {
        Table table = null;
        try {
            //获取表
            table = conn.getTable(TableName.valueOf(tableName));
            //定义删除的行
            Delete delete = new Delete(rowKey.getBytes());
            //删除的列族、列
            delete.addColumn(family.getBytes(), qualifier.getBytes());
            //执行删除
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                    System.out.println("close table");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 删除一行
     *
     * @param tableName
     * @param rowKey
     * @param family
     */
    public void deleteOneRow(String tableName, String rowKey, String family) {
        Table table = null;
        try {
            //获取表连接
            table = conn.getTable(TableName.valueOf(tableName));
            //定义删除行
            Delete delete = new Delete(rowKey.getBytes());
            //定义删除列族
            delete.addFamily(family.getBytes());
            //执行删除
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 查询rowkey下某一列值
     *
     * @param tableName 表名
     * @param rowKey    行号
     * @param family    列族
     * @param qualifier 列
     * @return
     */
    public String getValue(String tableName, String rowKey, String family, String qualifier) {
        Table table = null;
        String value = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            //返回指定列族、列名，避免rowKey下所有数据
            get.addColumn(family.getBytes(), qualifier.getBytes());
            Result rs = table.get(get);
            Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier.getBytes());

            if (cell != null) {
                value = Bytes.toString(CellUtil.cloneValue(cell));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return value;
    }

    /**
     * 获取一行数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @return
     */
    public List<Cell> getRowCells(String tableName, String rowKey, String family) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            get.addFamily(family.getBytes());
            Result rs = table.get(get);
            List<Cell> cellList = rs.listCells();
            return cellList;
        } catch (IOException e) {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return null;
        /**
         * for (Cell cell : cells) {
         * String	qualifier = Bytes.toString( cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
         * String	value = Bytes.toString( cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
         * System.out.println(qualifier+"--"+value);
         * }
         * */
    }
}
