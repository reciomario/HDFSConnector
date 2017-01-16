package HDFSConnector;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * HDFS driver able to perform operations on a HDFS storage system
 * ADD
 * READ
 * DELETE
 * MKDIR 
 * RMDIR
 * LIST
 * ...
 */
public class HDFSConnector {
  public HDFSConnector() {

  }

  /**
   * ADD an existing file from local pc to a HDFS
   * @param source
   * @param dest
   * @param conf
   * @throws IOException
   */
  public void addFile(String source, String dest, Configuration conf) throws IOException {

    FileSystem fileSystem = FileSystem.get(conf);

    // Get the filename out of the file path
    String filename = source.substring(source.lastIndexOf('/') + 1,source.length());

    // Create the destination path including the filename.
    if (dest.charAt(dest.length() - 1) != '/') {
      dest = dest + "/" + filename;
    } else {
      dest = dest + filename;
    }
    System.out.println(dest);

    System.out.println("Adding file to " + dest);

    // Check if the file already exists
    Path path = new Path(dest);
    if (fileSystem.exists(path)) {
      System.out.println("File " + dest + " already exists");
      return;
    }

    // Create a new file and write data to it.
    FSDataOutputStream out = fileSystem.create(path);
    InputStream in = new BufferedInputStream(new FileInputStream(new File(
        source)));

    byte[] b = new byte[1024];
    int numBytes = 0;
    while ((numBytes = in.read(b)) > 0) {
      out.write(b, 0, numBytes);
    }

    // Close all the file descriptors
    in.close();
    out.close();
    fileSystem.close();
  }

  
  /**
   * READ a file from hdfs and saves a copy in local folder where this program is executed
   * @param file
   * @param conf
   * @throws IOException
   */
  public void saveFileInLocal(String file, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(file);
    if (!fileSystem.exists(path)) {
      System.out.println("File " + file + " does not exists");
      return;
    }

    FSDataInputStream in = fileSystem.open(path);

    String filename = file.substring(file.lastIndexOf('/') + 1,
        file.length());


    OutputStream out = new BufferedOutputStream(new FileOutputStream(
        new File(filename)));

    byte[] b = new byte[1024];
    int numBytes = 0;
    while ((numBytes = in.read(b)) > 0) {
      out.write(b, 0, numBytes);
    }

    in.close();
    out.close();
    fileSystem.close();
  }


  
  /**
   * READ a file from hdfs and returns a String with the content of that file
   * @param file
   * @param conf
   * @return String containing the content of the HDFS file
   * @throws IOException
   */
  public String getFileContent(String file, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(file);
    if (!fileSystem.exists(path)) {
      System.out.println("File " + file + " does not exists");
      return "File " + file + " does not exists";
    }

    FSDataInputStream in = fileSystem.open(path);

    String filename = file.substring(file.lastIndexOf('/') + 1,
        file.length());


    String out = new String("");
    
    byte[] b = new byte[1024];
    int numBytes = 0;
    while ((numBytes = in.read(b)) > 0) {
    	out = out + new String(b);
    }

    in.close();
    fileSystem.close();
    return out;
  }
  
  /**
   * DELETE a directory in hdfs
   * @param file
   * @throws IOException
   */
  public void deleteFile(String file, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(file);
    if (!fileSystem.exists(path)) {
      System.out.println("File " + file + " does not exists");
      return;
    }

    fileSystem.delete(new Path(file), true);

    fileSystem.close();
  }
  
  
  /**
   * CREATE a directory in hdfs
   * @param dir
   * @throws IOException
   */
  public void mkdir(String dir, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(dir);
    if (fileSystem.exists(path)) {
      System.out.println("Dir " + dir + " already exists");
      return;
    }

    fileSystem.mkdirs(path);

    fileSystem.close();
  }
  
  /**
   * REMOVE a directory in hdfs
   * @param dir
   * @throws IOException
   */
  public void rmdir(String dir, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(dir);
    if (!fileSystem.exists(path)) {
      System.out.println("Dir " + dir + " already not exists");
      return;
    }

    fileSystem.delete(path, true);

    fileSystem.close();
  }

  
  /**
   * LIST folder content in hdfs
   * @param dir
   * @throws IOException
   */
  public void list(String dir, Configuration conf) throws IOException {
    FileSystem fileSystem = FileSystem.get(conf);

    Path path = new Path(dir);
    if (!fileSystem.exists(path)) {
      System.out.println("Dir " + dir + " already not exists");
      return;
    }

    FileStatus[] fileStatus = fileSystem.listStatus(path);
    System.out.println("Content of " + path);
    System.out.println("PERMISSIONS" + "\t" + "NAME" + "\t\t\t\t\t\t" + "OWNER");
    for(FileStatus status : fileStatus){
        System.out.println(status.getPermission() + "\t" + status.getPath().toString() + "\t" + status.getOwner());
    }
    

    fileSystem.close();
  }

  
  /**
   * Creates a file in HDFSwith the content and filename defined by parameters
   * @param content
   * @param filename
   * @param dest
   * @param conf
   * @throws IOException
   */
  public void addString(String content, String filename, String dest, Configuration conf) throws IOException {

    FileSystem fileSystem = FileSystem.get(conf);


    // Create the destination path including the filename.
    if (dest.charAt(dest.length() - 1) != '/') {
      dest = dest + "/" + filename;
    } else {
      dest = dest + filename;
    }

    System.out.println("Creating file in " + dest);

    // Check if the file already exists
    Path path = new Path(dest);
    if (fileSystem.exists(path)) {
      System.out.println("File " + dest + " already exists");
      return;
    }

    // Create a new file and write data to it.
    FSDataOutputStream out = fileSystem.create(path);

    InputStream in = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    byte[] b = new byte[1024];
    int numBytes = 0;
    while ((numBytes = in.read(b)) > 0) {
      out.write(b, 0, numBytes);
    }

    // Close all the file descriptors
    in.close();
    out.close();
    fileSystem.close();
  }

  
}
