Execution steps:

Step 1: Prepare Jars
Make to root folder of each maven project and execute "mvn package"

Step 2: Execute in spark
Input: Path to input files
bin\spark-submit --class DataAggregation --master local[2] D:\cloud\Assign1\PrepareData\target\Prepare-Data-1.0.jar D:\cloud\Assign1\PrepareData\src\test\resources\ D:\cloud\Assign1\CanopyMaker\src\test\resources\

Input: Output of step 1, path to output 
bin\spark-submit --class CanopyMaker --master local[2] D:\cloud\Assign1\CanopyMaker\target\Canopy-Maker-1.0.jar D:\cloud\Assign1\CanopyMaker\src\test\resources\part-00000 D:\cloud\Assign1\CanopyCluster\src\test\resources\ 

Input: Output of step 2, output of step 1, path to output 
bin\spark-submit --class CanopyCluster --master local[2] D:\cloud\Assign1\CanopyCluster\target\Canopy-Cluster-1.0.jar D:\cloud\Assign1\CanopyCluster\src\test\resources\part-00000 D:\cloud\Assign1\CanopyMaker\src\test\resources\part-00000 D:\cloud\Assign1\CanopyCluster\output\

Input: Maximum iterations, canopy centres, output of step 3, path to output 
bin\spark-submit --class KMeans --master local[2] D:\cloud\Assign1\KMeans\target\KMeans-Cluster-1.0.jar 3 D:\cloud\Assign1\CanopyCluster\src\test\resources\part-00000  D:\cloud\Assign1\CanopyCluster\output\part-00000 D:\cloud\Assign1\KMeans\output\