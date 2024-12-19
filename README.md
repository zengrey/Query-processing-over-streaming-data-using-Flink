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
### Step 2:Prepare the dataset with TCP-H tools and data_genration
