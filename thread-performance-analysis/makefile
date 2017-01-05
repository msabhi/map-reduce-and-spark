JARFILEPATH = "`pwd`"/target/mapreduce-assignment-0.0.1-SNAPSHOT-jar-with-dependencies.jar
FILEPATH = *.csv 

#
# the default make target entry
#

default: 
	mvn install

local:
	java -jar $(JARFILEPATH) $(FILEPATH)
