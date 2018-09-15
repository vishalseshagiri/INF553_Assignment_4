#### Execution Syntax with output files
-	**Running Python Code**
	- Task 1
		- ```spark-submit Vishal_Seshagiri_Betweenness.py <ratings_file_path>```
		- Output file -> Vishal_Seshagiri_Betweenness.txt
	- Task 2
		- ```spark-submit Vishal_Seshagiri_Community.py <ratings_file_path>```
		- Output file -> Vishal_Seshagiri_Community.txt
	- Bonus task - using networkx library (python)
		- ```spark-submit Vishal_Seshagiri_Community_Bonus.py <ratings_file_path>```
		- Output file -> Vishal_Seshagiri_Community_Bonus.txt
-	Running Scala Code
	-	Task 1
		-	```spark-submit – class Betweenness Vishal_Seshagiri_hw4.jar <ratings_file_path>```
		-	Output_file -> Vishal_Seshagiri_Betweenness_Scala.txt
	-	Task 2
		-	```spark-submit – class Community Vishal_Seshagiri_hw4.jar <ratings_file_path>```
		-	Output file -> Vishal_Seshagiri_Community_Scala.txt

#### Spark Version => 2. 2. 1
#### Python Version => 2.7.12
#### Scala Version => 2.11.8
#### sbt Version => 0.13.8