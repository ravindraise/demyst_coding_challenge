# Data Engineering Coding Challenges

## Problem 1

### Parse fixed width file

- Generate a fixed width file using the provided spec (offset provided in the spec file represent the length of each field).
- Implement a parser that can parse the fixed width file and generate a delimited file, like CSV for example.
- DO NOT use python libraries like pandas for parsing. You can use the standard library to write out a csv file (If you feel like)
- Language choices (Python or Scala)
- Deliver source via github or bitbucket
- Bonus points if you deliver a docker container (Dockerfile) that can be used to run the code (too lazy to install stuff that you might use)
- Pay attention to encoding

### Solution to problem 1:
- The Python script(generate_and_parse.py) generates a fixed-width file based on the spec.json file, parses it, and outputs a CSV file.
- By default, it generates 1000 rows file, if you need more rows, add "NoOfRows":xxx to spec.json, it generates no of rows mentioned as per spec.json
#### Run the solution using docker image
- Please create a docker image from the Dockerfile inside the project folder 
```text
 demyst_coding_challenge/Dockerfile_fixed_width 
```
```shell
 cd demyst_coding_challenge 
 docker build -t fixed-width-parser . -f Dockerfile_fixed_width
```
- check if docker image has been saved in local repo
 ```shell
 docker image ls | grep fixed
  ```
```text
 output: fixed-width-parser     latest   6a6694b366fc   About a minute ago   156MB
```
- run the docker image
```shell
docker run --rm -v $(pwd)/fixed_width:/app/fixed_width fixed-width-parser
or 
# more recent version of docker image
docker run --rm -v $(pwd)/fixed_width:/app/fixed_width fixed-width-parser:latest
```
```text
output:
Fixed-width file 'fixed_width/fixed_width_data.txt' generated successfully.
CSV file 'fixed_width/parsed_data.csv' generated successfully.
```
- Python script(generate_and_parse.py) generates a fixed-width file and parses it into a CSV file.
- Dockerfile containerizes the solution and ensures that the files are saved inside the fixed_width folder
  - fixed_width/fixed_width_data.txt- This file contains fixed with data
  - fixed_width/parsed_data.csv - This file parsed fixed with file saved in CSV file format
- Python script handles encoding on the provided spec.json
- Docker container will save the output files inside fixed_width folder inside current working dir

## Problem 2

### Data processing

- Generate a csv file containing first_name, last_name, address, date_of_birth
- Process the csv file to anonymise the data
- Columns to anonymise are first_name, last_name and address
- You might be thinking  that is silly
- Now make this work on 2GB csv file (should be doable on a laptop)
- Demonstrate that the same can work on bigger dataset
- Hint - You would need some distributed computing platform

### Solution to problem 2:
- Pyspark job data_proc_generate_csv.py uses python faker library, generates 2gb csv file 
and saves inside data/2gb_csv_file folder in the current working dir.
- Logic to generate 2gb csv data is there in the python script file
```python
approx_row_size = 80  # Estimated size of each row in bytes
target_size = 2 * 1024 * 1024 * 1024  # 2GB in bytes
num_rows = target_size // approx_row_size
```
- Pyspark job data_proc_anonymize_data.py reads the 2gb csv file and anonymise the csv data using
  - hashing (sha 256)  
  - Anonymization with Random Strings
- Anonymized data will be saved under data/anonymize_output under current working dir
  - data/anonymize_output/sha_256 - Has anonymized data for hashed sha 256.
  - data/anonymize_output/random  - Has anonymized data for random string.

#### Run the solution using docker  locally 
- Please create a docker image from the Dockerfile inside the project folder 
```text
demyst_coding_challenge/Dockerfile_data_process
```
```shell
 cd demyst_coding_challenge
 # Dockerfile uses spark image to run the spark job with docker container. 
 docker build -t data-anonymise-csv . -f Dockerfile_data_process
```
- check if docker image has been saved in local repo
 ```shell
 docker image ls | grep data-anonymise
  ```
```text
 output: data-anonymise-csv     latest   0a3d49f193e9   About a minute ago   1.71GB
```
- run the docker image to generate 2gb csv file
```shell
# data_proc_generate_csv.py is the default spark-submit in Docker file
docker run --rm -v $(pwd)/data:/opt/spark_scripts/data data-anonymise-csv:latest 
or 
docker run --rm -v $(pwd)/data:/opt/spark_scripts/data data-anonymise-csv:latest spark-submit /opt/spark_scripts/data_proc_generate_csv.py

# you can also pass spark conf
docker run --rm -v $(pwd)/data:/opt/spark_scripts/data data-anonymise-csv:latest \
  spark-submit \
  --master "local[*]" \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=4 \
  /opt/spark_scripts/data_proc_generate_csv.py
```
- run the docker image to anonymize input data
```shell
docker run --rm -v $(pwd)/data:/opt/spark_scripts/data data-anonymise-csv:latest spark-submit /opt/spark_scripts/data_proc_anonymize_data.py

# you can also pass spark conf
docker run --rm -v $(pwd)/data:/opt/spark_scripts/data data-anonymise-csv:latest \
  spark-submit \
  --master "local[*]" \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=4 \
  /opt/spark_scripts/data_proc_anonymize_data.py
```
#### Run Docker Container in a Spark Cluster
- Push Docker Image to a Registry
```shell
# Tag the Docker image
docker tag data-anonymise-csv:latest <your-registry>/data-anonymise-csv:latest

# Push the Docker image
docker push <your-registry>/data-anonymise-csv:latest
```
- Submit spark jobs
```shell
# generate 2gb csv file
spark-submit --master <spark-master-url> \
    --conf spark.kubernetes.container.image=<your-registry>/data-anonymise-csv:latest \
    /opt/spark_scripts/data_proc_generate_csv.py
# anonymize input data
spark-submit --master <spark-master-url> \
    --conf spark.kubernetes.container.image=<your-registry>/data-anonymise-csv:latest \
    /opt/spark_scripts/data_proc_anonymize_data.py
# you can also pass other spark conf in spark submit
spark-submit \
--master <cluster-manager-url> \
--deploy-mode <deploy-mode> \
--conf spark.kubernetes.container.image=<your-registry>/data-anonymise-csv:latest \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=<min executors> \
--conf spark.dynamicAllocation.maxExecutors=<max executors> \
/opt/spark_scripts/data_proc_generate_csv.py or data_proc_anonymize_data.py
```