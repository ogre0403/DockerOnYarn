package org.nchc.yarnapp;

/**
 * Created by superorange on 11/30/15.
 */

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import com.mysql.jdbc.PreparedStatement;

public abstract class RemoteMysql {
    // JDBC driver name and database URL
    private final String DATABASETYPE = "jdbc:mysql://";
    private final String DATABASEPORT = ":3306/";

    private String USERNAME = null;
    private String PASSWORD = null;
    private String URL = null;

    private Connection con = null;

   public RemoteMysql(String mySQL_IP, String username, String password, String database) {

        try {
            // Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");
            USERNAME = username;
            PASSWORD = password;
            URL = DATABASETYPE + mySQL_IP + DATABASEPORT + database;

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void connect() {
        try {
            if (URL != null && USERNAME != null && PASSWORD != null) {
                con = DriverManager.getConnection(URL, USERNAME, PASSWORD);
            } else {
                System.err.println("URL USERNAME PASSWORD can't be null!");
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void conClose() {
        try {
            if (con != null) {
                con.close();
                con = null;
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // /-------------- DELETE (STATEMENT) ---------------------
    private void statement(String SQL) throws SQLException {

        Statement stmt = null;

        stmt = con.createStatement();
        stmt.executeUpdate(SQL);
        stmt.close();

    }

    // /-------------- UPDATE / INSERT (PREPARE STATEMENT)---------------------
    private void preStatement(String SQL, Object[] input) throws SQLException {

        PreparedStatement pst = null;
        pst = (PreparedStatement) con.prepareStatement(SQL);
        int i = 0;
        for (Object content : input) {
            i++;
            pst.setString(i, content.toString());
        }
        // Execute a query
        String output = pst.toString();
        System.err.println("[Mysql] -------- output:" + output);
        pst.executeUpdate();
        System.err.println("[Mysql]  " + output + " *** COMPLETE!");
        pst.close();

    }

    // /----------------------------------------------------

    // /----------------- SELECT (RETURN RESULTSET)---------------------------
    private ResultSet getResultSet(String SQL, Object[] input) throws SQLException {

        PreparedStatement pst = null;
        ResultSet Result = null;

        pst = (PreparedStatement) con.prepareStatement(SQL);

        if (input != null) {
            int i = 0;
            for (Object content : input) {
                i++;
                pst.setString(i, content.toString());
            }
        }

        String OUTPUT = pst.toString();
        Result = pst.executeQuery();
        System.err.println("[Mysql]  " + OUTPUT + " *** COMPLETE!");

        // pst.close();

        return Result;

    }

    public boolean tableExist(String table) throws SQLException {
        connect();

        String sql = "show tables like '" + table + "'";

        Statement stmt = null;
        stmt = con.createStatement();
        ResultSet result = stmt.executeQuery(sql);
        System.err.println(sql);

        while (result.next()) {
            conClose();
            return true;

        }

        conClose();
        return false;
    }

    // add columns
    public void add(String table, Object[] columnName, Object[] columnType) throws SQLException {
        connect();

        String sql = "Alter table " + table + " add column (";
        for (int i = 0; i < columnName.length; i++) {
            sql = sql + columnName[i] + " " + columnType[i] + ",";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1) + ")";
        }

        System.err.println(sql);
        statement(sql);
        conClose();
    }

    // -----------------count
    public class ColTypeField {
        private String TYPE = null;
        private String FIELD = null;

        public ColTypeField(String field, String type) {
            TYPE = type;
            FIELD = field;
        }

        public String getType() {
            return TYPE;
        }

        public String getField() {
            return FIELD;
        }
    }

    public ArrayList<ColTypeField> getColumn(String table) throws SQLException {
        // int IndexNum =6;
        ArrayList<ColTypeField> output = new ArrayList<ColTypeField>();
        ResultSet result = null;
        connect();
        String sql = "Show columns from " + table;

        result = getResultSet(sql, null);
        // -------*****--------
        result.last();
        // int size = result.getRow();
        result.beforeFirst();

        // Object[] output = new Object[size*2+1];
        // output[0] = size;
        // int i=1;
        while (result.next()) {
            ColTypeField col = new ColTypeField(result.getString("Field"), result.getString("Type"));
            output.add(col);
            // System.out.println(result.getString("type"));
            // i++;

        }

        // -------*****--------
        conClose();
        return output;
    }

    public int count(String table, String condition) throws SQLException {
        connect();
        String sql = "SELECT count(" + condition + ") from " + table;
        Statement stmt = null;
        stmt = con.createStatement();
        System.err.println(sql);
        ResultSet result = stmt.executeQuery(sql);
        int number = 0;
        while (result.next()) {
            number = result.getInt("count(" + condition + ")");

        }

        conClose();
        return number;
    }

    // CREATE table----
    public void create(String table, Object[] columnName, Object[] columnType) throws SQLException {
        connect();

        String sql = "CREATE table IF NOT EXISTS " + table + " (";
        for (int i = 0; i < columnName.length; i++) {
            sql = sql + columnName[i] + " " + columnType[i] + ",";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1) + ")";
        }
        System.err.println(sql);
        statement(sql);
        conClose();
    }

    // /----------------------------------------------------
    public void insert(String table, Object[] input) throws SQLException {
        connect();

        String sql = "INSERT " + table + " VALUES(";
        for (int i = 0; i < input.length; i++) {
            sql = sql + "?,";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1) + ")";
        }
        System.err.println(sql);
        preStatement(sql, input);

        conClose();

    }

    public void insert(String table, Object[] index, Object[] input) throws SQLException {
        connect();
        // INSERT INT (index...) VALUES(? ?)
        String sql = "INSERT INTO " + table + " (";
        for (Object content : index) {
            sql = sql + content + ",";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1) + ") VALUES(";
        }
        for (int i = 0; i < input.length; i++) {
            sql = sql + "?,";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1) + ")";
        }
        // System.out.println(sql);
        preStatement(sql, input);

        conClose();

    }

    public void insert(String tableName, String[] colNames, ArrayList<String[]> values) throws SQLException {
        connect();
        StringBuilder sql = new StringBuilder("INSERT INTO " + tableName);
        if (colNames != null) {
            sql.append(" (" + colNames[0]);
            for (int i = 1; i < colNames.length; i++) {
                sql.append("," + colNames[i]);
            }
            sql.append(")");
        }
        String[] rowValues = values.get(0);
        sql.append(" VALUES (" + rowValues[0]);
        for (int j = 1; j < rowValues.length; j++) {
            sql.append("," + rowValues[j]);
        }
        sql.append(")");
        for (int i = 1; i < values.size(); i++) {
            rowValues = values.get(i);
            sql.append(", (" + rowValues[0]);
            for (int j = 1; j < rowValues.length; j++) {
                sql.append("," + rowValues[j]);
            }
            sql.append(")");
        }
        statement(sql.toString());
        conClose();
    }

    public void update(String table, Object[] setIndex, Object[] input) throws SQLException {
        connect();

        String sql = "UPDATE " + table + " SET ";
        for (Object content : setIndex) {
            sql = sql + content + "=?,";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        // System.out.println(sql);
        preStatement(sql, input);
        conClose();

    }

    public void update(String table, Object[] setIndex, Object[] whereIndex, Object[] input) throws SQLException {
        connect();

        String sql = "UPDATE " + table + " SET ";
        for (Object content : setIndex) {
            sql = sql + content + "=?,";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        if (whereIndex != null) {
            sql = sql + " where";
            for (Object content : whereIndex) {
                sql = sql + " " + content + "=? &&";
            }
            if (sql.endsWith("&&")) {
                sql = sql.substring(0, sql.length() - 2);
            }
        }
        // System.out.println(sql);
        preStatement(sql, input);
        conClose();

    }

    public void dropTable(String table) throws SQLException

    {
        connect();
        // mysql.delete("delete from HBaseScan");
        String sql = "drop table if exists " + table;
        statement(sql);
        conClose();
    }

    public void delete(String table) throws SQLException {
        connect();
        // mysql.delete("delete from HBaseScan");
        String sql = "delete from " + table;
        statement(sql);
        conClose();
    }

    public void delete(String table, Object[] whereIndex, Object[] input) throws SQLException {
        connect();
        // mysql.delete("delete from HBaseScan");
        String sql = "delete from " + table;
        if (whereIndex != null) {
            sql = sql + " where";
            for (Object content : whereIndex) {
                sql = sql + " " + content + "=? &&";
            }
            if (sql.endsWith("&&")) {
                sql = sql.substring(0, sql.length() - 2);
            }
        }
        preStatement(sql, input);
        conClose();
    }

    public Object[] select(String table, Object[] getIndex, Object[] whereIndex, Object[] input) throws SQLException {
        ResultSet result = null;
        connect();
        String sql = "SELECT ";// +table+ " SET ";
        for (Object content : getIndex) {
            sql = sql + content + ",";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1) + " from " + table;
        }
        if (whereIndex != null) {
            sql = sql + " where";
            for (Object content : whereIndex) {
                sql = sql + " " + content + "=? &&";
            }
            if (sql.endsWith("&&")) {
                sql = sql.substring(0, sql.length() - 2);
            }
        }
        System.err.println(sql);

        result = getResultSet(sql, input);
        // -------*****--------
        result.last();
        int size = result.getRow();
        result.beforeFirst();

        Object[] output = new Object[getIndex.length * size + 1];
        output[0] = size;
        int i = 1;
        while (result.next()) {
            for (Object key : getIndex) {
                output[i] = result.getString(key.toString());
                i++;
            }
        }
        // -------*****--------
        conClose();
        return output;
    }

    public Object[] select(String table, Object[] getIndex, Object[] whereIndex, Object[] input, String order) throws SQLException {
        ResultSet result = null;
        connect();
        String sql = "SELECT ";// +table+ " SET ";
        for (Object content : getIndex) {
            sql = sql + content + ",";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1) + " from " + table;
        }
        if (whereIndex != null) {
            sql = sql + " where";
            for (Object content : whereIndex) {
                sql = sql + " " + content + "=? &&";
            }
            if (sql.endsWith("&&")) {
                sql = sql.substring(0, sql.length() - 2);
            }
        }
        sql = sql + " order by " + order;
        System.err.println(sql);

        result = getResultSet(sql, input);
        // -------*****--------
        result.last();
        int size = result.getRow();
        result.beforeFirst();

        Object[] output = new Object[getIndex.length * size + 1];
        output[0] = size;
        int i = 1;
        while (result.next()) {
            for (Object key : getIndex) {
                output[i] = result.getString(key.toString());
                i++;
            }
        }
        // -------*****--------
        conClose();
        return output;
    }

    public Object[] selectDistinct(String table, Object[] getIndex, Object[] whereIndex, Object[] input) throws SQLException {
        ResultSet result = null;
        connect();
        String sql = "SELECT distinct ";// +table+ " SET ";
        for (Object content : getIndex) {
            sql = sql + content + ",";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1) + " from " + table;
        }
        if (whereIndex != null) {
            sql = sql + " where";
            for (Object content : whereIndex) {
                sql = sql + " " + content + "=? &&";
            }
            if (sql.endsWith("&&")) {
                sql = sql.substring(0, sql.length() - 2);
            }
        }
        // System.out.println(sql);

        result = getResultSet(sql, input);
        // -------*****--------
        result.last();
        int size = result.getRow();
        result.beforeFirst();

        Object[] output = new Object[getIndex.length * size + 1];
        output[0] = size;
        int i = 1;
        while (result.next()) {
            for (Object key : getIndex) {
                output[i] = result.getString(key.toString());
                i++;
            }
        }
        // -------*****--------
        conClose();
        return output;
    }

    public Object[] selectDistinct(String table, Object[] getIndex, Object[] whereIndex, Object[] input, String order) throws SQLException {
        ResultSet result = null;
        connect();
        String sql = "SELECT distinct";// +table+ " SET ";
        for (Object content : getIndex) {
            sql = sql + content + ",";
        }
        if (sql.endsWith(",")) {
            sql = sql.substring(0, sql.length() - 1) + " from " + table;
        }
        if (whereIndex != null) {
            sql = sql + " where";
            for (Object content : whereIndex) {
                sql = sql + " " + content + "=? &&";
            }
            if (sql.endsWith("&&")) {
                sql = sql.substring(0, sql.length() - 2);
            }
        }
        sql = sql + " order by " + order;
        System.err.println(sql);

        result = getResultSet(sql, input);
        // -------*****--------
        result.last();
        int size = result.getRow();
        result.beforeFirst();

        Object[] output = new Object[getIndex.length * size + 1];
        output[0] = size;
        int i = 1;
        while (result.next()) {
            for (Object key : getIndex) {
                output[i] = result.getString(key.toString());
                i++;
            }
        }
        // -------*****--------
        conClose();
        return output;
    }

    public String[][] select(boolean distinct, String tableName, String[] targetFields, String[] condFields, String[] values, String orderField) {
        connect();
        StringBuilder sql = new StringBuilder("SELECT " + (distinct ? "DISTINCT " : "") + targetFields[0]);
        for (int i = 1; i < targetFields.length; i++) {
            sql.append("," + targetFields[i]);
        }
        sql.append(" FROM " + tableName);
        if (condFields != null) {
            sql.append(" WHERE " + condFields[0] + "=?");
            for (int i = 1; i < condFields.length; i++) {
                sql.append(" && " + condFields[i] + "=?");
            }
        }
        if (orderField != null) {
            sql.append(" ORDER BY " + orderField);
        }

        String[][] result;
        if (condFields != null) {
            result = executeQueryPrepareStatement(sql.toString(), values);
        } else {
            result = executeQueryStatement(sql.toString());
        }
        conClose();
        return result;
    }

    private String[][] executeQueryStatement(String sql) {
        Statement stmt = null;
        ResultSet rs = null;
        String[][] result = null;
        try {
            stmt = con.createStatement();
            System.err.println("[SQL] " + sql);
            rs = stmt.executeQuery(sql);
            if (rs != null) {
                result = outputResult(rs);
                rs.close();
            }
            stmt.close();
        } catch (SQLException e) {
            System.err.println("[ERROR] " + e.toString());
        }
        return result;
    }

    private String[][] executeQueryPrepareStatement(String sql, String[] values) {
        java.sql.PreparedStatement ps = null;
        ResultSet rs = null;
        String[][] result = null;
        try {
            ps = con.prepareStatement(sql);
            for (int i = 0; i < values.length; i++) {
                ps.setString(i + 1, values[i]);
            }
            System.err.println("[SQL] " + sql);
            rs = ps.executeQuery();
            if (rs != null) {
                result = outputResult(rs);
                rs.close();
            }
            ps.close();
        } catch (SQLException e) {
            System.err.println("[ERROR] " + e.toString());
        }
        return result;
    }

    private String[][] outputResult(ResultSet rs) {
        String[][] result = null;
        try {
            if (rs.last()) {
                int numRow = rs.getRow();
                rs.beforeFirst();
                ResultSetMetaData rsmd = rs.getMetaData();
                int numCol = rsmd.getColumnCount();
                result = new String[numRow + 2][numCol];

                for (int j = 0; j < numCol; j++) {
                    result[0][j] = rsmd.getColumnLabel(j + 1);
                    result[1][j] = rsmd.getColumnTypeName(j + 1);
                }
                int i = 2;
                while (rs.next()) {
                    for (int j = 0; j < numCol; j++) {
                        result[i][j] = rs.getString(result[0][j]);
                    }
                    i++;
                }
                System.err.println("[RESULT] Total: " + numRow + " rows.");
            }
        } catch (SQLException e) {
            System.err.println("[ERROR] " + e.toString());
        }
        return result;
    }

    public void UpdateSummaryTable(String summaryTableName, String fabID, String designData, String stepID, String recipeName, String startTimeStamp, String endTimeStamp) throws SQLException {

        Object[] getIndex = { "starttime" };
        Object[] whereIndex = { "fabID", "designData", "stepID", "recipeName" };
        Object[] input = { fabID, designData, stepID, recipeName };
        Object[] OUTPUT = select(summaryTableName, getIndex, whereIndex, input);

        // not exist
        if (OUTPUT.length == 1) {
            Object[] index2 = { "fabID", "designData", "stepID", "recipeName", "starttime", "endtime" };
            Object[] input2 = { fabID, designData, stepID, recipeName, startTimeStamp, endTimeStamp };
            insert(summaryTableName, index2, input2);

        } else {
            Object[] getIndex2 = { "starttime", "endtime" };
            Object[] whereIndex2 = { "fabID", "designData", "stepID", "recipeName" };
            Object[] input2 = { fabID, designData, stepID, recipeName };
            Object[] OUTPUT2 = select(summaryTableName, getIndex2, whereIndex2, input2);

            String[] timeRange = new String[2];
            timeRange[0] = OUTPUT2[OUTPUT2.length - 2].toString();
            timeRange[1] = OUTPUT2[OUTPUT2.length - 1].toString();
            // -------***-------
            Long originStartTime = Long.valueOf(timeRange[0]);
            Long originEndTime = Long.valueOf(timeRange[1]);
            Long updateStartTime = Long.valueOf(startTimeStamp);
            Long updateEndTime = Long.valueOf(endTimeStamp);
            updateEndTime = Math.max(originEndTime, updateEndTime);
            updateStartTime = Math.min(originStartTime, updateStartTime);

            Object[] setIndex3 = { "starttime", "endtime" };
            Object[] whereIndex3 = { "fabID", "designData", "stepID", "recipeName" };
            Object[] input3 = { String.valueOf(updateStartTime), String.valueOf(updateEndTime), fabID, designData, stepID, recipeName };
            update(summaryTableName, setIndex3, whereIndex3, input3);

        }

    }

    public void UpdateDesignData(String PRJname, String TIME) throws SQLException {
        String designDataTable1 = "designData";
        Object[] designDataIndex1 = { "designData" };
        Object[] designDataInput1 = { PRJname + ".PRJ" };
        insert(designDataTable1, designDataIndex1, designDataInput1);

        String designDataTable2 = "DesignDataJob";
        Object[] designDataIndex2 = { "username", "timestamp", "designDataName", "status" };
        Object[] designDataInput2 = { "default", TIME, PRJname, "success" };
        insert(designDataTable2, designDataIndex2, designDataInput2);

    }
    // /----------------------------------------------------

}