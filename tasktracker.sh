#!/bin/bash

if [ $1 == '-c' ]	
then
	javac -sourcepath src -d bin src/**/**/**/*.java 
	echo "Compilation success"
else
	java -cp bin: com.mapreduce.tasktracker.TTrackerDriver $@
fi

