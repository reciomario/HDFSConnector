# HDFSConnector

HDFSConnector is a very simple tool for using a HDFS system in a encapsulated and clean way in Java. It contains methods for managing the Hadoop Distributed Filesystem in a simple way. 
In particular, it offers the following methods:

  - **addFile**: ADD an existing file from local path to a HDFS
- **saveFileInLocal**: READ a file from HDFS and saves a copy in the local folder where the app is executed
- **getFileContent**: READ a file from HDFS and returns a String with the content of that file
- **deleteFile**: DELETE a file in the HDFS
- **mkdir**: CREATE a directory in the HDFS 
- **rmdir**: REMOVE a directory in the HDFS
- **list**: LIST folder content in the HDFS
- **addString**: CREATE a file in HDFS with the content and filename defined by the user

# Installation and dependencies
---
#### Maven


HDFSConnector.java requires Hadoop dependencies in order to communicate with a HDFS. The following dependencies may be included:
```sh
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>2.7.3</version>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>2.7.3</version>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.7.3</version>
</dependency>
```

Once the dependencies have been included in the project, just drag and drop HDFSConnector.java into your project's folder and include it in your development classes to start using its functionalities.
