package org.nchc.yarnapp;


import java.sql.SQLException;

/**
 * Created by superorange on 11/30/15.
 */
public class SimpleMysql extends RemoteMysql {
    private static final String IP_TABLENAME = "ipStatus";
    private static final String IP_COLUMN = "ip";
    private static final String STATUS_COLUMN = "status";


    public SimpleMysql(String mySQL_IP, String username, String password, String database)
    {
        super(mySQL_IP,username,password,database);
    }
    public String getIP()
    {

        Object[] getIndex = new Object[]{IP_COLUMN};
        Object[] whereIndex = new Object[]{STATUS_COLUMN};
        Object[] input = new Object[]{"0"};
        try {
            Object[] result = select(IP_TABLENAME, getIndex, whereIndex, input);

            if(result.length > 1){
               return result[1].toString();
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void setIPStatus(String ip, String status)
    {
        Object[] setIndex = new Object[]{STATUS_COLUMN};
        Object[] whereIndex = new Object[]{IP_COLUMN};
        Object[] input = new Object[]{status,ip};
        try {
            update(IP_TABLENAME,setIndex,whereIndex,input);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

        String MysqlIp = "140.110.141.62";
        String MysqlUser = "root";
        String MysqlPwd = "Managers";
        String MysqlDb = "ipManager";
        SimpleMysql MYSQL = new SimpleMysql(MysqlIp,MysqlUser,MysqlPwd,MysqlDb);

        String ip = MYSQL.getIP();
        if (ip != null && !ip.isEmpty()) {
            System.out.println("getIP: " + ip);
            MYSQL.setIPStatus(ip, "1");
        }
        else{
            System.out.println("ip=null");
        }


    }
}
