# Assignment 03
```text
parallel-local-to-hdfs-copy
.
├── build
│   └── classes
│       └── bdpuh
│           └── hw03
│               └── ParallelLocalToHdfsCopy.class
├── README.md
├── src
│   └── main
│       └── java
│           └── bdpuh
│               └── hw03
│                   └── ParallelLocalToHdfsCopy.java
└── target
    └── parallel-local-to-hdfs-copy-1.0.0.jar

```
# Overview

ParallelLocalToHdfsCopy copies files from a local source path to an HDFS destination path in parallel, and compresses each file as .gz during transfer. 

Program requirements:

- bdpuh.hw03.ParallelLocalToHdfsCopy class
- args:
    - absolute local source path
    - absolute HDFS destination path
    - number of threads
- rules
    - local source must exist, else return "Source path does not exist" and exit.
    - HDFS destination path must not exist, else return "Destination path already exists. Please delete before running the program" and exit.
- copy files in parallel and compress to .gz file
- ignore local source path subdirectories
- demonstrate with 10 local files

Submission Requirements:

- executable JAR
- source code packaged in zip
- Solutions Doc with screenshot showing:
    - local source path contents
    - HDFS destination path missing files
    - Run program
    - HDFS destination path with gz files


# compile jar
```bash

javac -cp "$(hadoop classpath)" \
  -d build/classes src/main/java/bdpuh/hw03/ParallelLocalToHdfsCopy.java

jar cfe target/parallel-local-to-hdfs-copy-1.0.0.jar \
  bdpuh.hw03.ParallelLocalToHdfsCopy -C build/classes .

```
# setup environment
```bash
NN=$(hdfs getconf -confKey fs.defaultFS)
DEST=/user/hdadmin/hw03_out_$(date +%s)
hdfs dfs -test -e "$DEST" && echo "exists" || echo "$DEST path does not exist in Hadoop HDFS"
mkdir -p /home/hdadmin/local-src
echo ${NN}
echo ${DEST}

```

 # Create test files
 ```bash
for i in $(seq 1 10); do echo "file $i - $(date)" > /home/hdadmin/local-src/file_$i.txt; done
ls -l /home/hdadmin/local-src
```

# parallel copy with 4 threads
```bash
java -cp target/parallel-local-to-hdfs-copy-1.0.0.jar:$(hadoop classpath) \
  bdpuh.hw03.ParallelLocalToHdfsCopy \
  /home/hdadmin/local-src \
  "${NN}${DEST}" \
  4

hdfs dfs -ls "$DEST"
```

# zip assignment for submission
```bash
zip -r ../assignment_03.zip . \
  -x "build/*" ".git/*" ".idea/*" ".vscode/*" "*~" "*.class" ".DS_Store"

```