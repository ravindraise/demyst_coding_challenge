from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, sha2, length
from pyspark.sql.types import StringType
import string
import random
import os


# generate random string
def random_string(str_len):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(str_len))


# anonymize data using hashing (sha 256)
def anonymize_sha256(input_df):
    input_df = (input_df.withColumn("first_name", sha2("first_name", 256)).
                withColumn("last_name", sha2("last_name", 256)).
                withColumn("address", sha2("address", 256)))

    return input_df


# Anonymization with Random Strings
def anonymize_random(input_df):
    random_string_udf = udf(lambda x: random_string(x), StringType())
    input_df = (input_df.withColumn("first_name", random_string_udf(length("first_name"))).
                withColumn("last_name", random_string_udf(length("last_name"))).
                withColumn("address", random_string_udf(length("address"))))

    return input_df


# main method
def main():
    # Initialize a Spark session
    spark = SparkSession.builder \
        .appName("Data processing-AnonymizeData") \
        .getOrCreate()

    output_dir = os.path.join("data", "2gb_csv_file")
    if not os.path.exists(output_dir):
        print("Input CSV data does not exist in the path {}".format(output_dir))
        spark.stop()
        exit(1)
    # read generated csv file
    df = spark.read.csv(output_dir, header=True)
    df.show()

    # Anonymize columns using SHA-256 hashing
    df_anonymize_sha_256 = anonymize_sha256(input_df=df)
    print("Anonymize sha_256 df")
    df_anonymize_sha_256.show(truncate=False)

    # anonymize using random string
    df_anonymize_random = anonymize_random(input_df=df)
    print("Anonymize random df")
    df_anonymize_random.show(truncate=False)

    # Anonymize sha_256 df
    print("Saving anonymize sha256 file")
    output_dir = os.path.join("data", "anonymize_output", "sha_256")
    df_anonymize_sha_256.write.mode(saveMode='overwrite'). \
        csv(output_dir, header=True)

    # Anonymize random df
    print("Saving anonymize using random")
    output_dir = os.path.join("data", "anonymize_output", "random")
    df_anonymize_random.write.mode(saveMode='overwrite'). \
        csv(output_dir, header=True)
    print("completed")
    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
