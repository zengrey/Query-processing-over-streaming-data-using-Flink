# Query-processing-over-streaming-data-using-Flink
A Project based on the Cquirrel system that perfrom the TCP-H Q3 in a quicker and more reliable way. The project use Cygwin to enable a Linux environment in Win 11 so that flink could be run in windows system.  

Instead of building the whole project, If you wish to directly test the project result using the Jar file, you can unzip the "Q3_With_algorithm.zip" and "Q3_without_algorithm.zip" and use the jar inside.
## Project environment
Flink-1.19.1  
apache-maven-3.9.9   
Cygwin-3.5.4   
Kafka-2.13  
Java-11.0.16.1
## Project implementation
### Step 1: Modify the Cygwin bash_profile so that it can compile Flink
Follow the instruction in the following link https://nightlies.apache.org/flink/flink-docs-release-1.9/getting-started/tutorials/flink_on_windows.html  
Find the .bash_profile file in the location ./Cygwin/home/{your name}/ open it with the notepad and add the following lines:   

export SHELLOPTS  
set -o igncr   
### Step 2:Prepare the dataset with TCP-H tools and data_generation
Download the TCP-H tools from the following links https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp, unzip the file and go to ./dbgen folder, find the makefile.suite and use the notepad to opne it, then modify the code in it as the following:  

    ## CHANGE NAME OF ANSI COMPILER HERE
    ################    
    CC      = GCC  
    # Current values for DATABASE are: INFORMIX, DB2, TDAT (Teradata)  
    #                                  SQLSERVER, SYBASE, ORACLE, VECTORWISE  
    # Current values for MACHINE are:  ATT, DOS, HP, IBM, ICL, MVS,   
    #                                  SGI, SUN, U2200, VMS, LINUX, WIN32   
    # Current values for WORKLOAD are:  TPCH  
    DATABASE= ORACLE  
    MACHINE = LINUX  
    WORKLOAD = TPCH  
    #  
save the file and then run the command: 'make makefile' in the Cygwin under the TCP-Tools location, this will generation a dbgen.exe file,then use the command './dbgen -s {size} -vf' to genrate the data size you want, size 1 means the data set size would be around 1GB.   
This will genrate 8 .tbl file, copy those .tbl file into the folder location ./Cquirrel-release/DemoTools/DataGenerator where you download the code from 'https://github.com/hkustDB/Cquirrel-release.git' and then run the 'DataGenerator.py'   
Remember that the code configure is designed for 1 GB data size, if you wish to use different data set, you should go to the 'config_all.ini' and modify the WindowSize and ScaleFactor to your own data size, where the ScaleFactor='size'.   
Final you will get a csv file which contains many deletion and insertion processes, move it to the project folder ./MavenProject/tpch-flink.  
### Step 3: Run the Kafka service and create the Kafka topic
In th project, I use 'upsert-kafka' to read the data stream in the csv file, so a Kafka topic should be ran locally to obtain the result from the Kafka data sink.  
To run the Kakfa service, go to the folder where you download the Kafka, and run the following command in Cygwin:  
bin/zookeeper-server-start.sh config/zookeeper.properties  

bin/kafka-server-start.sh config/server.properties  

bin/kafka-topics.sh --create --topic tpch-query3-results --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1  

Use:  
bin/kafka-topics.sh --list --bootstrap-server localhost:9092  
to see if the topic has been created or not.  
### Step 4: Build the jar file using maven
Go to the folder you donwload the project, run the following command in the Cygwin to prepare the enviroment needed for Maven and build flink job:  
mvn clean package  
remember your Cygwin should first download the maven. The compiled jar file will be stored in the ./target.
### Step 5:Modify the Flink configuration and run the Flink
Go to the floder you download Flink, create a new folder called 'temp', which will be used to store the taskmanager in the furture.In addtion, make sure you have the full control over the folder you create. Go to ./conf to find the 'config.yaml' file for Flink. Modify as the follwoing:  
  memory:  
    process:  
      size: 4096m  
      taskmanager:  
    bind-host: 0.0.0.0  
    tmp.dirs: {your location that create the temp folder}   
    resource-id : 50329fc0146  
This step is needed because the Windows seems to have some compile problem for the taskmanger folder.   
After modifing the Flink configuration, you can run the Flink locally by the following command in Cygwin:  
./bin/start-cluster.sh
### Step 6: Sumbit the job and get the result
Go to localhost:8081, you will see a Web UI, go to 'submit job' and upload jar you built. Enter the following parameters to excute the Flink job:  
Enterpoint: org.example.App  
After the job finished, you can run the following command to get the result in kafka:  
.\bin\kafka-console-consumer.sh --topic tpch-query3-results --bootstrap-server localhost:9092 --from-beginning





