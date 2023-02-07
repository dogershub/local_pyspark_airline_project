
#  Dataframe Transformation and Actions
Transformation is function that changes rdd data and Action is a function that doesn't change the data but gives an output.


    
# Pyspark
PySpark is an interface for Apache Spark in Python. It not only allows you to write Spark applications using Python APIs, but also provides the PySpark shell for interactively analyzing your data in a distributed environment.

## DataFrame

A DataFrame is equivalent to a relational table in Spark SQL, and can be created using various functions in SparkSession.

# Operations In Spark
- Transformation
- Action

# Business Overview Of Airlines Industry

- https://openflights.org/
- https://developer.ibm.com/exchanges/data/all/airline/
- https://www.bts.dot.gov/topics/airlines-and-airports/quick-links-popular-air-carrier-statistics

 
# Code Description
    File Name : dataframe1.ipynb , dataframe.ipynb, dataframe1.py and dataframe2.py
    DataSets : airlines1.csv
    File Description : Implement Transformations and Actions Functions on DataFrame
    
## Steps to Run
There are two ways to execute the end to end flow.
- Command Prompt => python script
- spark_path spark-submit file_path
- spark_path => <path_to_spark>>
- file_path => <path_to_file>
- Data file path is same as script file path

eg. <C:\Users\admin\Desktop\spark\bin>spark-submit C:\Users\admin\Desktop\Business_Case\dataframe1.py>


- IPython

### Modular code
- Create virtualenv
- Install requirements `pip install -r requirements.txt`
- Run Code `python dataframe1.py`
- Run Code `python dataframe2.py`
- Check output for all the visualization
### IPython
Follow the instructions in the notebook `dataframe1.ipynb`
Follow the instructions in the notebook `dataframe2.ipynb`

 
