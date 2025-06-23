import csv
import os
import pandas as pd
import numpy as np
import re
filepath=r"C:\\Users\\USER\\OneDrive\\Desktop\\amazon project\best_sellers_data2.csv"
with open(filepath, 'r', newline='', encoding='utf-8') as file:
    reader = csv.reader(file)
    header = next(reader)
    print("Header:", header)

    for row in reader:
        # Encode each element to handle non-ASCII characters
        safe_row = [str(item).encode('ascii', 'replace').decode('ascii') for item in row]
        print(safe_row)

with open(filepath, 'r', newline='', encoding='utf-8') as file:
    reader = csv.reader(file)
    for row in reader:
        safe_row = [str(item).encode('ascii', 'replace').decode('ascii') for item in row]
        print(safe_row)

# Handle file reading errors and edge cases to ensure reliability

if not os.path.isfile(filepath):
    print(f"Error: File not found → {filepath}")
else:
    try:
        if not filepath.lower().endswith('.csv'):
            raise ValueError("Invalid file type. Please provide a .csv file.")

        with open(filepath, 'r', encoding='utf-8', newline='') as file:
            reader = csv.reader(file)
            header = next(reader, None)
            if header:
                print("Header:", header)
            else:
                print("Warning: File is empty or header is missing.")

            for row in reader:
                if not any(row):
                    continue
                safe_row = [str(item).encode('ascii', 'replace').decode('ascii') for item in row]
                print(safe_row)

    except FileNotFoundError:
        print(f"Error: Could not open file → {filepath}")
    except Exception as e:
        print(f"Error: {str(e)}")

# Load the dataset into a Pandas DataFrame.

try:
    df = pd.read_csv(filepath)
    print("Data loaded successfully!\n")
except FileNotFoundError:
    print(f"File not found: {filepath}")
    exit(1)
except pd.errors.ParserError:
    print("Error: CSV file is malformed.")
    exit(1)

#4.Perform cleaning operations 

df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(r'\s+', '_', regex=True)
    .str.replace(r'[^a-z0-9_]', '', regex=True)
)


df.dropna(inplace=True)


df['product_price'] = df['product_price'].replace(r'[\$,]', '', regex=True)
df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce')
df = df[df['product_price'].notnull() & (df['product_price'] > 0)]

df['product_title'] = df['product_title'].str.replace(r'[^\w\s]', '', regex=True).str.strip()


str_cols = df.select_dtypes(include='object').columns
df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())


df.drop_duplicates(inplace=True)
df.dropna(subset=['product_title', 'product_price'], inplace=True)


df = df[df['product_price'].notnull() & df['product_title'].notnull()]


df = df[df['product_price'] > 0]



output_filepath = r"C:\\Users\\USER\\OneDrive\\Desktop\\amazon project\\cleaned_best_sellers_data2.csv"
df.to_csv(output_filepath, index=False)
print(f"Data cleaned and saved as '{output_filepath}'")

#4.MySQL Integration:

import mysql.connector

try:
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="994894",
    
    )
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS best_sellers_data2")
    if connection.is_connected():
        cursor = connection.cursor()
        print("Connected to MySQL!")

    create_table_query = """
   CREATE TABLE IF NOT EXISTS best_sellers_data2 (
    id INT AUTO_INCREMENT PRIMARY KEY,
    product_title VARCHAR(255) NOT NULL,
    product_price DECIMAL(10,2),
    product_star_rating FLOAT,
    product_num_rating INT,
    `rank` INT,
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    connection.commit()

except mysql.connector.Error as e:
    print(f"MySQL Error: {e}")
finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
        connection.close()
        print("MySQL connection closed.")

# 5.Analytical Computation:
import matplotlib.pyplot as plt
import seaborn as sns
df = pd.read_csv(filepath)

print(" Data Preview:")
print(df.head())


print("\n Top-Rated Software Products (with > 100 ratings):")
top_rated = df[df['product_num_ratings'] > 100] \
    .sort_values(by='product_star_rating', ascending=False) \
    [['product_title', 'product_star_rating', 'product_num_ratings']] \
    .head(10)

print(top_rated)

plt.figure(figsize=(10, 5))
sns.histplot(df['product_price'], bins=30, kde=True)
plt.title(" Price Distribution of Software Products")
plt.xlabel("Price ($)")
plt.ylabel("Number of Products")
plt.grid(True)
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 5))
sns.scatterplot(x='rank', y='product_price', data=df)
plt.title("Price vs Rank (Lower Rank = More Popular)")
plt.xlabel("Rank")
plt.ylabel("Price ($)")
plt.grid(True)
plt.tight_layout()
plt.show()

most_reviewed = df.groupby('country') \
    .apply(lambda x: x.sort_values(by='product_num_ratings', ascending=False).head(1)) \
    .reset_index(drop=True)

print("\n Most Reviewed Product by Country:")
print(most_reviewed[['country', 'product_title', 'product_num_ratings']])

country_stats = df.groupby('country').agg({
    'product_star_rating': 'mean',
    'product_num_ratings': 'mean'
}).sort_values(by='product_star_rating', ascending=False)

print("\n  Country Stats (Average Rating & Number of Ratings):")
print(country_stats)

country_stats.plot(kind='bar', figsize=(12, 5), title='Average Rating and Review Count by Country')
plt.ylabel("Average")
plt.tight_layout()
plt.show()


df['is_free'] = df['product_price'] == 0

free_vs_paid = df.groupby('is_free').agg({
    'product_star_rating': 'mean',
    'product_num_ratings': 'mean'
})

print("\n Free vs Paid Software Performance:")
print(free_vs_paid)

free_vs_paid.plot(kind='bar', figsize=(8, 4), title='Free vs Paid - Avg Rating & Reviews')
plt.xticks([0, 1], ['Paid', 'Free'], rotation=0)
plt.ylabel("Average")
plt.tight_layout()
plt.show()


