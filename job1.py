from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, to_date, regexp_replace, regexp_extract,
    trim, monotonically_increasing_id, upper,row_number
)

# ✅ Initialize Spark with Hive support
spark = SparkSession.builder \
    .appName("HotelReviewsETL") \
    .enableHiveSupport() \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# ✅ USE the correct Hive database
spark.sql("USE hotel_reviews")

# ✅ Load the dataset
df = spark.read.csv(
    "gs://dataproc-staging-asia-south1-736700985824-9bgkjqm7/google-cloud-dataproc-metainfo/data/hotel_dataset.csv",
    header=True,
    inferSchema=True
)

# ✅ Data Cleaning
df_clean = df.dropna().dropDuplicates()

# Convert Review_Date to standard format
df_clean = df_clean.withColumn(
    "Review_Date",
    to_date(
        regexp_replace(col("Review_Date"), "/", "-"),
        "M-d-yyyy"  # or "dd-MM-yyyy" based on your actual format
    )
)

# Convert days_since_review to integer
df_clean = df_clean.withColumn(
    "days_since_review",
    regexp_extract(col("days_since_review"), r"(\d+)", 1).cast("int")
)

# ✅ Create DIM TABLE: Hotel
dim_hotel = df_clean.select(
    "Hotel_Name", "Hotel_Address", "Average_Score"
).dropDuplicates().withColumn("Hotel_ID", monotonically_increasing_id()) \
 .select(
     col("Hotel_ID").cast("BIGINT"),
     col("Hotel_Name"),
     col("Hotel_Address"),
     col("Average_Score").cast("FLOAT")
 )

# ✅ Create DIM TABLE: Reviewer
dim_reviewer = df_clean.select("Reviewer_Nationality") \
    .dropDuplicates() \
    .withColumn("Reviewer_ID", monotonically_increasing_id()) \
    .select(
        col("Reviewer_ID").cast("BIGINT"),
        col("Reviewer_Nationality")
    )

# ✅ Create DIM TABLE: Review Text
dim_review_text = df_clean.select(
    "Negative_Review", "Positive_Review", "Tags"
).dropDuplicates().withColumn("Review_Text_ID", monotonically_increasing_id()) \
 .select(
     col("Review_Text_ID").cast("BIGINT"),
     col("Negative_Review"),
     col("Positive_Review"),
     col("Tags")
 )

# ✅ Create FACT TABLE
fact_reviews = df_clean \
    .join(dim_hotel, on=["Hotel_Name", "Hotel_Address", "Average_Score"], how="left") \
    .join(dim_reviewer, on=["Reviewer_Nationality"], how="left") \
    .join(dim_review_text, on=["Negative_Review", "Positive_Review", "Tags"], how="left") \
    .select(
        monotonically_increasing_id().alias("Review_ID").cast("BIGINT"),
        col("Hotel_ID"),
        col("Reviewer_ID"),
        col("Review_Text_ID"),
        col("Review_Date"),
        col("Reviewer_Score").cast("FLOAT"),
        col("Review_Total_Negative_Word_Counts").cast("INT"),
        col("Review_Total_Positive_Word_Counts").cast("INT"),
        col("Total_Number_of_Reviews_Reviewer_Has_Given").cast("INT"),
        col("Total_Number_of_Reviews").cast("INT"),
        col("Additional_Number_of_Scoring").cast("INT"),
        col("days_since_review").cast("INT")
    )

# ✅ Insert into Hive tables (overwrite if exists)
dim_hotel.write.mode("overwrite").insertInto("dim_hotel")
dim_reviewer.write.mode("overwrite").insertInto("dim_reviewer")
dim_review_text.write.mode("overwrite").insertInto("dim_review_text")
fact_reviews.write.mode("overwrite").insertInto("fact_reviews")

print("✅ JOB DONE")
