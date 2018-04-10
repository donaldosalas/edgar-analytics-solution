#!/bin/bash

if [[ -e output/sessionization.txt ]] ; then
  rm output/sessionization.txt && touch output/sessionization.txt
else
  touch output/sessionization.txt
fi

sbt clean assembly

java -jar ./target/scala-2.12/EdgarAnalyticsSolution-assembly-0.1.0-SNAPSHOT.jar
