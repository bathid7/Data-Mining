Code can be executed from the command prompt as follows :

javac ProcessCandidate.java Apriori.java
java  -Xmx3g -XX:-UseGCOverheadLimit Apriori [Value of S] [Valueof K] [Path of DB]
pause

for example :

javac ProcessCandidate.java Apriori.java
java  -Xmx3g -XX:-UseGCOverheadLimit Apriori 10 2 "C://users//desktop//transactiondB.txt"
pause

Here -Xmx3g is for increasing the heap size.
     -XX:-UseGCOverheadLimit is for removing overhead limit.
		 
Note : 1.No need to specify the output file name in the command line. 
       2.Output file with the same name mentioned in the demo outputs based on k and s is created in the current directory.
       2.The code is written using lambdas and streams for effective processing. Please make sure to run in jdk 1.8 (Java8).
       3.Please make sure the format of path is correct i.e., use '//' for separators. 