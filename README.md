# SPARK-BASED ETL on EMR


<p> In this article we will see how to send Spark-based ETL studies to an Amazon EMR cluster.
<p> Alternatives:
    
    - You can submit the work as an EMR step using the console, CLI, or API.
    
    - You can submit steps when the cluster is started or you can submit steps to a running cluster. 

    
<p></br>

## CLI JOB EXECUTION 
    
### - Open the AWS console and navigate to the S3 service.
    
### - Create an S3 bucket with folders
    - Datasets
    - logs
    - input
    - output    
### Let's upload the sample csv file to the "input folder" in the S3 bucket we just created.
<img width="429" alt="Ekran Resmi 2022-03-26 19 20 47" src="https://user-images.githubusercontent.com/91700155/160248303-ff9572ad-c09d-4fc8-880f-5e3bc56b65c8.png">
  
### SSH into your cluster. We can copy from the EMR summary tab.
```console
ssh -i <your-key-pair> hadoop@<emr-master-public-dns-address>    
```
<img width="926" alt="Ekran Resmi 2022-03-26 19 13 53" src="https://user-images.githubusercontent.com/91700155/160248174-cd3a6ac5-f2fa-4b97-aa4e-0759f8dccc1d.png">
  
### In the EMR terminal, open a new file named "spark-etl-examples.py" using the following command.
```console
nano spark-etl-examples.py
```
<img width="412" alt="Ekran Resmi 2022-03-26 19 32 26" src="https://user-images.githubusercontent.com/91700155/160248930-e8409366-4a4a-432e-b6e3-8ba0d9d87feb.png">
  
### Copy-Paste the following code into that file and save the changes you made.
```console
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    print(len(sys.argv))
    if (len(sys.argv) != 3):
        print("Spark-ETL: [input-folder] [output-folder]")
        sys.exit(0)

    spark = SparkSession\
        .builder\
        .appName("Spark-ETL")\
        .getOrCreate()

    btc = spark.read.option("inferSchema", "true").option("header", "true").csv(sys.argv[1])

    updatedBTC = btc.withColumn("current_date", lit(datetime.now()))

    updatedBTC.printSchema()

    print(updatedBTC.show())

    print("Total number: " + str(updatedBTC.count()))

    updatedBTC.write.parquet(sys.argv[2])
```
<img width="703" alt="Ekran Resmi 2022-03-26 19 32 51" src="https://user-images.githubusercontent.com/91700155/160248938-2bc2f6ba-f0d4-4962-8991-8a7ba27419da.png">
  
### Run the commands to update the path.
```console
export PATH=$PATH:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/:/usr/share/aws/aws-java-sdk/:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/:/usr/share/aws/emr/emrfs/auxlib/
```
```console
export PATH=$PATH:spark.driver.extraClassPath/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/:/usr/share/aws/aws-java-sdk/:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/:/usr/share/aws/emr/emrfs/auxlib/
```   
 
### This Spark job will query the BTC data from input location, add a new column “current_date” and write transformed data in the output location in Parquet format. Replace with the name of the bucket you created earlier.
```console    
spark-submit spark-etl-examples.py s3://<YOUR-BUCKET>/input/ s3://<YOUR-BUCKET>/output/spark
```     
### Now, check the “output/spark” folder in your S3 bucket to see the results.

<img width="549" alt="Ekran Resmi 2022-03-26 20 07 10" src="https://user-images.githubusercontent.com/91700155/160250023-6ed13261-faf2-45cf-99de-e7efd97d6002.png">
 
### Summary of the chapter:
    - Read CSV data from Amazon S3
    - Add current date to the dataset
    - Write updated data back to Amazon S3 in Parquet format

    
    
<p></br>
    
## CREATE JUPYTERHUB or EMR NOTEBOOKS
<p> With the IAM permission set, you can now create your EMR Notebook. EMR Notebooks are serverless Jupyter notebooks that connect to an EMR cluster using Apache Livy. They come preconfigured with Spark and allow you to run Spark jobs interactively in a familiar Jupyter environment. The code and visualizations you create in the notebook are permanently saved to S3.

### In the EMR console, click ‘Notebooks’. Click ‘Create notebook’.
<img width="1417" alt="Ekran Resmi 2022-03-26 20 25 25" src="https://user-images.githubusercontent.com/91700155/160250657-a0bda2c4-51a5-4da7-81d4-89bb08441c17.png">
  
### Name the notebook and add an optional description.   
### Choose an existing cluster, and click ‘Choose’.   
<img width="893" alt="Ekran Resmi 2022-03-26 20 41 55" src="https://user-images.githubusercontent.com/91700155/160251299-d3dbee75-9b83-4e03-bd64-d40a40ffc915.png">
  
### Click the radio button next to the cluster you created in Lab cluster creation lab and click ‘Choose cluster’. 
<img width="688" alt="Ekran Resmi 2022-03-26 19 07 45" src="https://user-images.githubusercontent.com/91700155/160251352-b2db1fac-a558-4fa9-ac2a-c87a4dae786e.png">

### Click ‘Create notebook’. When you close the EMR and then reopen it, you have to change cluster which is active cluster before you work on it.
    
### Refresh the screen until ‘Starting’ changes to ‘Pending’ and then ‘Ready’. Click ‘Open in Jupyter’ to open your EMR Notebook.
<img width="728" alt="Ekran Resmi 2022-03-26 20 50 06" src="https://user-images.githubusercontent.com/91700155/160251450-e72ffbcb-3bcd-4593-b0c3-778b8cfb8531.png">
   
### Create a new PySpark Notebook.
 <img width="423" alt="Ekran Resmi 2022-03-26 21 10 05" src="https://user-images.githubusercontent.com/91700155/160252021-720c9d8f-956e-4523-b9e0-f3821519de96.png">
   
### Paste the following code, and click Run. 
```console
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
```
<img width="1013" alt="Ekran Resmi 2022-03-26 21 24 52" src="https://user-images.githubusercontent.com/91700155/160252471-b371676c-e6c2-48b6-ade2-a482f2e88bdd.png">
    
### Paste the following code, and click Run. 
```console
input_path = "s3://<YourS3BucketName>/input/<YourCSVFile>.csv"
output_path = "s3://<YourS3BucketName>/output/"
```
 <img width="816" alt="Ekran Resmi 2022-03-26 21 27 47" src="https://user-images.githubusercontent.com/91700155/160252518-aaf0d5cb-906b-449d-aee2-38ad89886e6e.png">
  
### Paste the following code, and click Run. 
```console
btc = spark.read.option("inferSchema", "true").option("header", "true").csv(input_path)
```
 <img width="817" alt="Ekran Resmi 2022-03-26 21 28 01" src="https://user-images.githubusercontent.com/91700155/160252525-467f0c97-b088-4cda-9ca6-eb6367c769b0.png">
     
### Paste the following code, and click Run. 
```console
btc.count()
```
<img width="1019" alt="Ekran Resmi 2022-03-26 21 29 03" src="https://user-images.githubusercontent.com/91700155/160252590-f11cf16a-0bab-4705-976d-da728ea23099.png">
    
### Paste the following code, and click Run. 
```console
btc.show()
```
<img width="816" alt="Ekran Resmi 2022-03-26 21 29 34" src="https://user-images.githubusercontent.com/91700155/160252596-e56a42a0-dc58-4d87-9105-4d72e89e5d52.png">
    
### Paste the following code, and click Run. 
```console
btc.printSchema()   
```
<img width="811" alt="Ekran Resmi 2022-03-26 21 29 49" src="https://user-images.githubusercontent.com/91700155/160252599-0eae5c71-fd61-4008-8b67-b1869840d5c5.png">

### Paste the following code, and click Run. 
```console
updatedBTC = btc.withColumn("current_date", lit(datetime.now()))
updatedBTC.printSchema()    
```
<img width="813" alt="Ekran Resmi 2022-03-26 21 30 22" src="https://user-images.githubusercontent.com/91700155/160252603-e1af7384-236b-40d6-9cdb-7fb14517f58e.png">

#### We can can see on the Emr -> Steps -> stdout under Log Files to view the logs that were printed during the execution of the step.

<p></br>


    
    
    
 <p>   
 Thank you :)
        
 
 <p> 
 <p>
    
 Seda Atalay.

