import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import utils.Constants;
import HDFSConnector.HDFSConnector;


public class Main {


	public static void main(String[] args) throws IOException, InterruptedException {
		
		//Required for selecting the correct RAMSES user with permissions in the HDFS 
		UserGroupInformation ugi
        = UserGroupInformation.createRemoteUser(Constants.HDFS_REMOTE_USER);

		ugi.doAs(new PrivilegedExceptionAction<Void>() {
			 
		public Void run() throws Exception {

			//Initialize HDFS connection parameters
			HDFSConnector client = new HDFSConnector();
			String hdfsPath = Constants.HDFS_PATH;
			String hdfsuser = Constants.HDFS_REMOTE_USER;
			Configuration conf = new Configuration();
			conf.set("fs.default.name", hdfsPath);
			conf.set("hadoop.job.ugi", hdfsuser);
			FileSystem fs = new DistributedFileSystem();
		    
			
			
			
			
		      /******************************/
		     /**        OPERATIONS        **/
			/******************************/
			
			String localFile = "/home/mario.recio/Escritorio/test.txt";
			String hdfsFile = Constants.HDFS_PATH + "test.txt";
			//ADD file
		    client.addFile(localFile, hdfsPath, conf);
		    //SAVE file in local (from HDFS)
		    client.saveFileInLocal(hdfsFile, conf);
		    //SAVE content file in String from HDFS
		    System.out.println(client.getFileContent(hdfsFile, conf));
		    // client.deleteFile(hdfsFile, conf);
		    //MKDIR and RMDIR in HDFS
		    client.mkdir(Constants.HDFS_PATH + "test/", conf);
		    client.rmdir(Constants.HDFS_PATH + "test/", conf);
		    
		    client.list(Constants.HDFS_PATH, conf);
		    client .addString("probando hdfs", "pruebaStringHdfs", Constants.HDFS_PATH, conf);
		    
	
		    System.out.println("Finished!");
		    return null;
			 }
		 
	    });
	}	
	
}


