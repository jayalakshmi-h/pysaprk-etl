#Approach
The approach is to make it as a config driven ETL pipeline so that it can be reused for either a different ingredient or for a different set of classification rules without making any changes
to the code. Configuration file contains the ingredient for which we wanted to extract the recipe (in this case, it is beef) and condition to classify the difficulty levels.

The approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform, Load parts and Data quality checks as well into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook)

#Assumptions/Consideration
Since the calculation logic relies on Cook time an Prep time, data quality check is performed on those columns to ensure that the results produced are accurate.
Following cases are considered to filter invalid records:
1. Recipe having both cookTime and prepTime as either Null/Empty string
2. Recipe with cookTime/prepTime having values not in ISO8601 duration format

#Steps to run the application
It can be submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions. For
example, this example script can be executed as follows,
    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

#CI/CD for this application
Pull request can be set to merge only when all the tests are able to run successfully and meets the code coverage threshold.
1. Dockerfile — Contains the dockerized container with the virtual environment set up for the Jenkins agent.
2. Build_dependencies script zips all the code, dependencies, and config in the packages.zip file so that Jenkins can create the artifact, and the CD process can upload it into a repository.
3. Jenkinsfile — It defines the CICD process. where the Jenkins agent runs the docker container defined in the Dockerfile in the prepare step followed by running the test. Once the test is successful in the prepare artifact step, it uses the build_dependencies to create a zipped artifact. The final step is to publish the artifact which is the deployment step.

#Performance Issues - Diagnosis and Tuning
Following are few things which can be looked at for performance improvement:
1. Check if there are any UDF's being used and see if there is a possibility to convert to column based functions. Spark catalyst optimizer could not optimize UDF's. Hence it is better to avoid UDF's for performance improvement.
2. Look at the physical and logical query plan to check pushed down and partition filters. Modify the query for pushed down and partition filters so that only the required set of records are passed to the next stage.
3. Analyse the long running task/stage in Spark UI and check if there are any data skewness
4. Ensure that all the executors and cores are completely utilized

#Schedule Spark Jobs
If there are no inter job dependency, this pipeline can be scheduled to run as a cron job. Airflow can be used for scheduling inter dependent jobs.