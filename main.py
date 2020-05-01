#!/usr/bin/env python
# coding: utf-8


from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when, udf, trim
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from sqlalchemy import create_engine
import psycopg2 as pg

import pandas as pd

import os

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"


# initiate spark session and load xml plugin (https://github.com/databricks/spark-xml) jar
# https://repo1.maven.org/maven2/com/databricks/spark-xml_2.11/0.5.0/spark-xml_2.11-0.5.0.jar
spark = SparkSession.builder.config(
    "spark.jars", "spark-xml_2.11-0.5.0.jar").getOrCreate()


# load the movies csv
movies_metadata = spark.read.csv(
    "movies_metadata.csv",
    header=True,
    quote="\"",
    escape="\"")


# clean non digit values in revenue and budget columns
movies_metadata = movies_metadata.withColumn(
    "revenue",
    when(
        col("revenue").rlike(r'[\d]+'),
        col("revenue")).otherwise(None).cast(
            DoubleType())).withColumn(
                "budget",
                when(
                    col("budget").rlike(r'[\d]+'),
                    col("budget")).otherwise(None).cast(
                        DoubleType())).withColumn(
                            "release_year",
                            trim(
                                regexp_extract(
                                    col("release_date"),
                                    r'([\d]{4})\-',
                                    1)).cast(
                                        IntegerType()))


movies_metadata.printSchema()


# write a custom user defined funtion to write string ratio
@udf
def calculate_string_ratio(a, b):
    a_, b_ = a, b

    if (a in (None, 0)) or (b in (None, 0)):
        return None

    while(b):
        a, b = b, a % b

    return f"{int(a_/a)}:{int(b_/a)}"


# create new columns for string_ratio and float ratio
movies_metadata = movies_metadata.withColumn(
    "string_ratio", calculate_string_ratio(
        col("budget"), col("revenue"))).withColumn(
            "ratio", (col("budget") / col("revenue")))


# read wikipedia xml as spark dataframe using the spark-xml plugin
wikipedia_abstract_df = spark.read.format('xml').options(
    rowTag='doc').load('enwiki-latest-abstract.xml')
wikipedia_abstract_df.printSchema()


# All titles start with "Wikipedia:" prefix and some contain parenthesis with further category or year info
# clean title is the title without Wikipedia: prefix
# clean_title_no_brackets is the title without Wikipedia: prefix and
# without the extra info parenthesis ()
clean_movie_abs = wikipedia_abstract_df.withColumn(
    'clean_title',
    regexp_extract(
        col('title'),
        '([^:]+):(.*)',
        2)).withColumn(
            'clean_title_no_brackets',
            trim(
                regexp_extract(
                    col('title'),
                    '([^:]+):([^(]+)(.*)',
                    2)))

# some parenthesis (1968 film) or (film)
# we can estimate the year and category from the text in the parenthesis
# with regex. estimated_year and estimated_type columns
clean_movie_abs = clean_movie_abs .withColumn(
    'estimated_year',
    when(
        col('clean_title').rlike(r'.*\([\d]{4}[^\)]*\)$'),
        trim(
            regexp_extract(
                col('clean_title'),
                r'.*\(([\d]{4})[^\)]*\)$',
                1))).otherwise(None)) .withColumn(
                    'estimated_type',
                    when(
                        col('clean_title').rlike(r'.*\(([\d]{4})?[^\)]*\)$'),
                        trim(
                            regexp_extract(
                                col('clean_title'),
                                r'.*\(([\d]{4}[\s]+|)([^\)]*)\)$',
                                2))).otherwise(None))
# filter wikipedia dataframe to only include titles that are identical to
# the movie titles in imdb dataset
clean_movie_abs = clean_movie_abs.where(col("clean_title_no_brackets").isin(
    movies_metadata.select("title").toPandas()["title"].tolist()))
clean_movie_abs.printSchema()


# right join the movies and the filtered wikiepdia datasets on cleaned title (wikipedia) and imdb dataset title column
# only movies with revenue and budgets can have ratios so we use right
# join to movies dataset
movies_metadata_with_wiki_links = clean_movie_abs.drop('title').join(
    movies_metadata,
    clean_movie_abs.clean_title_no_brackets == movies_metadata.title,
    how="right")
movies_metadata_with_wiki_links.printSchema()


# there will be duplicate titles where one movie is matched to many wikiepdia articles
# we create a prioritisation strategy to prioritise titles that have the year mentioned and "film" category (2)
# then film category without a year (1) finally the rest (0)

# afterward we sort the records by title and priority (descending order)
# then we deduplicate by id

movies_metadata_with_wiki_links_test = movies_metadata_with_wiki_links.withColumn(
    'match_priority',
    when(
        (col("estimated_year").isNotNull()) & (
            col("estimated_type").isNotNull()) & (
                col('release_year') == col("estimated_year")) & (
                    col("estimated_type") == "film"),
        2).when(
        (col("estimated_year").isNull()) & (
            col("estimated_type").isNotNull()) & (
            col("estimated_type") == "film"),
        1). otherwise(0)).orderBy(
    [
        "clean_title_no_brackets",
        "match_priority"],
    ascending=False).drop_duplicates(
    subset=['id'])


# get the top 1000 ratios and rename the columns
# keep movies with more than 1000 USD budgets and revenue. Budget and revenue columns contain numbers
# representing 0s, multiples K or Mil numbers.
movies_metadata_with_wiki_links_test = movies_metadata_with_wiki_links_test.orderBy(
    "ratio",
    ascending=False).limit(1000). select(
        "title",
        "budget",
        "release_year",
        "revenue",
        "vote_average",
        "ratio",
        "production_companies",
        "url",
        "abstract"). where(
            (col("revenue") >= 1000) & (
                col("budget") >= 1000)). toDF(
                    'title',
                    'budget',
                    'year',
                    'revenue',
                    'rating',
                    'ratio',
                    'production_company',
                    'wikipedia_link',
    'wikipedia_abstract')


# extract dataframe to pandas
movies_metadata_with_wiki_links_final = movies_metadata_with_wiki_links_test.toPandas()

# connect to the SQL(postgres) docker image and create engine then a connection
engine = create_engine(
    'postgresql+psycopg2://postgres:docker@localhost:5432/postgres')
connection = engine.raw_connection()


# input the top 10000 movies into postgres database postgres and under a
# table named 'highest_budget_revenues_ratio_movies'
movies_metadata_with_wiki_links_final.to_sql(
    'highest_budget_revenues_ratio_movies',
    engine,
    index=False,
    if_exists='replace')
