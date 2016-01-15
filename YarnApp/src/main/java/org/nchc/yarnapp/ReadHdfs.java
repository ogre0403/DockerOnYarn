package org.nchc.yarnapp;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
/**
 * Created by superorange on 12/1/15.
 */
public class ReadHdfs {
    public static void main (String [] args) throws Exception{

    FileSystem fs = FileSystem.get(new URI("hdfs://140.110.141.62:8020/"), new Configuration());


        FileStatus[] status = fs.listStatus(new Path("/user/peggy"));

        for(int i=0;i<status.length;i++){
            System.out.println(status[i].getPath());
        }
        /*Path filePath = new Path("/user/peggy/kmeans_data.txt");
        FSDataInputStream fsDataInputStream = fs.open(filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream));

            String line;

            line=br.readLine();
            while (line != null){
                System.out.println(line);
                line=br.readLine();
            }

*/
        System.out.println("end");

    }
}
