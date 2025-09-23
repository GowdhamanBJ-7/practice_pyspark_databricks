import file_path
'''
You are given two datasets: one containing employee details(with some missing salary values)
and another containing department information.Create DataFrames on top of these datasets,
join them, and fill the missing salary values with the average salary of the respective department
'''
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("practice").getOrCreate()

employee_data = [
    (1, "Alice", 70000, 10),
    (2, "Bob", None, 20),  # Missing salary
    (3, "Charlie", 80000, 10),
    (4, "David", None, 30),  # Missing salary
    (5, "Eve", 75000, 20),
    (6, "Frank", 90000, 10),
    (7, "Grace", 52000, 30),
    (8, "Hannah", 62000, 20),
    (9, "Isaac", None, 30),  # Missing salary
    (10, "Jack", 71000, 20)
]
# data for departments
department_data = [
    (10, "Engineering"),
    (20, "HR"),
    (30, "Marketing")
]

employee_df = spark.createDataFrame(employee_data, ["id","name","salary","dept_id"])
department_df = spark.createDataFrame(department_data,["dept_id","dept_name"])

avg_df=employee_df.groupBy("dept_id").agg(avg("salary").alias("avg_salary"))
avg_df.show()

join_df = employee_df.join(avg_df, on="dept_id", how="left")
join_df.show()

final_df = join_df.withColumn("salary", when(col("salary").isNull(), col("avg_salary")).otherwise(col("salary")))
final_df.show()

#----------------------------------------------

car_list = [
    {'Brand': 'Hyundai', 'Year': 2021, 'Colors': ['Red', 'Blue', 'While', 'Black']},
    {'Brand': 'Maruthi', 'Year': 2022, 'Colors': ['Red', 'Blue', 'While', 'Green']},
    {'Brand': 'TATA', 'Year': 2022, 'Colors': ['Orange', 'Blue', 'While', 'Black']}
]
car_df = spark.createDataFrame(car_list)
car_df = car_df.filter(array_contains(col("Colors"),"Orange"))
car_df.show()

# car_list = [
#     'Brand': 'Hyundai', 'Year': 2021, 'Colors': White,
# 'Brand': 'Maruthi', 'Year': 2022, 'Colors': "Blue",
# 'Brand': 'TATA', 'Year': null, 'Colors': "Red"
# ]
#------------------------------------------------------
'''
First column is a transactionIDs, second one is the timing 9 to 16 is the working hours, So whatever
happens outside of the working hour, that is the invalid transaction and it should print
'''
data = [
    (1051, '2022-12-03 10:15'),
    (1052, '2022-12-03 17:00'),
    (1053, '2022-12-04 10:00'),
    (1054, '2022-12-04 14:00'),
    (1055, '2022-12-05 08:59'),
    (1056, '2022-12-05 16:01'),
    (1057, '2022-12-06 09:00'),
    (1058, '2022-12-06 15:59'),
    (1059, '2022-12-07 12:00'),
    (1060, '2022-12-08 09:00'),
    (1061, '2022-12-09 10:00'),
    (1062, '2022-12-10 11:00'),
    (1063, '2022-12-10 17:30'),
    (1064, '2022-12-11 12:00'),
    (106, '2022-12-12 13:00'),
    (1066, '2022-12-15 13:00'),
    (1067, '2022-12-24 13:00')
]
date_df = spark.createDataFrame(data, ["ID","date"])
date_df = date_df.withColumn("date", to_timestamp(col("date")))
date_df = date_df.withColumn("hours", hour(col("date")))

date_df = date_df.withColumn("check",
                             when(
                                 (col("hours") >= 9) & (col("hours") <= 16),
                                 lit("valid")
                             ).otherwise(lit("invalid"))
                             )
date_df.show()

#----------------------------------------------------------------
#write a query to find out the company who have atleast 2 users speaks both English and German
# https://www.youtube.com/watch?v=MNI6r2qz8oE
