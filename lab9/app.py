from pyspark.sql import SparkSession
from pyspark.sql.functions import length


def load_csv(spark, path):
    return spark.read.option("header", "true").csv(path)


def transform_data(df):
    return df.withColumn("length", length(df["name"]))


def save_data(df, output_path):
    df.coalesce(1).write.option("header", "true").csv(output_path)


def main():
    spark = SparkSession.builder.appName("PySparkApp").getOrCreate()
    df = load_csv(spark, "data/sample.csv")
    df_transformed = transform_data(df)
    save_data(df_transformed, "output")
    spark.stop()


if __name__ == "__main__":
    main()


