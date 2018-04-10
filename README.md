# edgar-analytics-solution 

#### Overview 
  1. Collect ip's requests (ip -> requests) 
  2. Increment an ip's request count (ip -> count) 
  3. Check for inactive sessions 
  4. When a session is detected, write `ip`, `start`, `end`, `duration` and `count` to `sessionization.txt` 

#### To run: 
1. Make sure Java 8 JDK (also known as 1.8) is installed 
i. `javac -version` in the command line is `javac 1.8.___` 
ii. If not, install the [JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html) 

2. Make sure sbt is installed 
[Mac](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Mac.html) 
[Linux](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html) 

3. In `edgar-analytics-solution/` run `./run.sh` 
