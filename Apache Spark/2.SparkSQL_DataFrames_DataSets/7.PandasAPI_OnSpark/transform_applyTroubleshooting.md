### \#TRANSFORM AND APPLY TROUBLESHOOTING



#### CODE GENERATING ERROR:



def format\_row(row):
return f"{row\['name']} ({row\['age']} years old)"

Apply the function across rows

ps\_df\["name\_with\_age"] = ps\_df.apply(format\_row, axis=1)
print("\\nDataFrame after apply on rows (name\_with\_age):")
print(ps\_df)



#### \#CORRECT APPROACH



ps\_df\["salary\_category"] = ps\_df\["salary"].apply(categorize\_salary)

3\. Instead of apply on rows, create column by combining columns vectorially

ps\_df\["name\_with\_age"] = ps\_df\["name"] + " (" + ps\_df\["age"].astype(str) + " years old)"

print("\\nDataFrame after transformations:")
print(ps\_df)







#### \#REASONING



Reasoning (Why apply(axis=1) failed and vectorized version worked):



* apply(axis=1) tries to process one row at a time → not supported well in pandas-on-Spark.
* Spark is built to handle columns, not individual rows.
* Row-wise operations need full row in memory → breaks distributed processing model.
* Vectorized operations (like combining columns directly) are faster, scalable, and Spark-friendly.
* apply() on a single column works fine because it's column-wise, not row-wise.
