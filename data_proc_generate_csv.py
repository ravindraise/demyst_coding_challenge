from pyspark.sql import SparkSession
from faker import Faker
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import os

# Initialize faker lib
faker = Faker()


# Function to generate random data using faker library
def random_first_name():
    return faker.first_name()


def random_last_name():
    return faker.last_name()


def random_address():
    return repr(faker.address())


def random_date_of_birth():
    return str(faker.date_of_birth())


# main method
def main():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("Data processing 2gb file") \
        .getOrCreate()

    approx_row_size = 80  # Estimated size of each row in bytes
    target_size = 2 * 1024 * 1024 * 1024  # 2GB in bytes
    num_rows = target_size // approx_row_size
    print("No of rows generated is {}".format(num_rows))

    # Register UDFs
    random_first_name_udf = udf(random_first_name, StringType())
    random_last_name_udf = udf(random_last_name, StringType())
    random_address_udf = udf(random_address, StringType())
    random_dob_udf = udf(random_date_of_birth, StringType())

    # Create DataFrame with the desired number of rows
    df = spark.range(num_rows).withColumn("first_name", random_first_name_udf()) \
        .withColumn("last_name", random_last_name_udf()) \
        .withColumn("address", random_address_udf()) \
        .withColumn("date_of_birth", random_dob_udf())
    df = df.drop("id")
    print("2gb data df")
    df.show(truncate=False)

    # Write the DataFrame to a CSV file
    print("Saving 2gb file")
    output_dir = os.path.join("data", "2gb_csv_file")
    # Save DataFrame as CSV to the 'data' directory
    df.write.mode("overwrite").csv(output_dir, header=True)

    print("completed")
    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
