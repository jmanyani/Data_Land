from __future__ import print_function

from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    # Initialize Spark session (Windows config omitted)
    spark_session = SparkSession.builder.appName("LinearRegressionExample").getOrCreate()

    # Load data from local file and parse into RDD of (label, features) tuples
    raw_data = spark_session.sparkContext.textFile("############################")
    parsed_data = raw_data.map(lambda line: line.split(",")).map(lambda parts: (float(parts[0]), Vectors.dense(float(parts[1]))))

    # Convert RDD to DataFrame with column names for MLlib
    schema_names = ["label", "features"]
    data_frame = parsed_data.toDF(schema_names)

    # Split dataset into training and testing sets (50% each)
    train_data, test_data = data_frame.randomSplit([0.5, 0.5])

    # Create Linear Regression model with specified hyperparameters
    linear_regressor = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Train model on training dataset
    trained_model = linear_regressor.fit(train_data)

    # Use trained model to make predictions on test dataset
    prediction_df = trained_model.transform(test_data).cache()

    # Extract predictions and actual labels as RDDs
    predicted_values = prediction_df.select("prediction").rdd.map(lambda row: row[0])
    actual_labels = prediction_df.select("label").rdd.map(lambda row: row[0])

    # Combine predictions and labels for evaluation
    prediction_label_pairs = predicted_values.zip(actual_labels).collect()

    # Print predicted vs actual label for each test data point
    for pred, label in prediction_label_pairs:
        print(f"Predicted: {pred}, Actual: {label}")

    # Stop Spark session
    spark_session.stop()
