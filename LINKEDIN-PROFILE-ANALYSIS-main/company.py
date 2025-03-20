from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, trim, lower, count, desc, when, split, size
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
spark = SparkSession.builder.appName("LinkedIn Data Analysis").getOrCreate()
json_file_path = "linked_data.json"
linkedin_df = spark.read.json(json_file_path, multiLine=True)
linkedin_df.printSchema()
linkedin_df.show(5, truncate=False)

def SetDegree(education_flat_df):
    return education_flat_df.withColumn(
    "degree_name",
    when(
        col("degree_name").like("%bachelor of technology%") | 
        col("degree_name").like("%btech%") | 
        col("degree_name").like("%técnico%") | 
        col("degree_name").like("%b.tech%"), 
        "bachelor of technology"
    )
    .when(
        col("degree_name").like("%bachelor of science%") | 
        col("degree_name").like("%bsc%") | 
        col("degree_name").like("%bs%") |
        col("degree_name").like("%b.s%"), 
        "bachelor of science"
    )
    .when(
        col("degree_name").like("%bachelor of business administration%") | 
        col("degree_name").like("%bba%"), 
        "Bachelor of business administration"
    )
    .when(
        col("degree_name").like("%master of business administration%") | 
        col("degree_name").like("%mba%"), 
        "master of business administration"
    )
    .when(
        col("degree_name").like("%bachelor of arts%") | 
        col("degree_name").like("%b.a%")|
        col("degree_name").like("%ba%")& ~col("degree_name").like("%mba%") &~col("degree_name").like("%bachelor%"),
        "bachelor of arts"
    )
    .when(
        col("degree_name").like("%master of science%") | 
        col("degree_name").like("%msc%") | 
        col("degree_name").like("%m.s%") | 
        col("degree_name").like("%ms%"), 
        "master of science"
    )
    .when(
        col("degree_name").like("%master of arts%") | 
        col("degree_name").like("%ma%") & ~col("degree_name").like("%master%") | 
        col("degree_name").like("%m.a%"), 
        "master of arts"
    )
    .when(
        col("degree_name").like("%master of engineering%") | 
        col("degree_name").like("%me%") | 
        col("degree_name").like("%m.e.%"), 
        "master of engineering"
    )
    .when(
        col("degree_name").like("%bachelor of engineering%") | 
        col("degree_name").like("%bacharelado em engenharia%") | 
        col("degree_name").like("%be%") | 
        col("degree_name").like("%engineering%") | 
        col("degree_name").like("%b.e.%"), 
        "bachelor of engineering"
    )
    .when(
        col("degree_name").like("%master of technology%") | 
        col("degree_name").like("%mtech%") | 
        col("degree_name").like("%m.tech%"), 
        "master of technology"
    )
    .when(
        col("degree_name").like("%master%") & ~col("degree_name").like("%science%") & ~col("degree_name").like("%arts%") & ~col("degree_name").like("%engineering%") & ~col("degree_name").like("%technology%"), 
        "master's degree"
    )
    .when(
        col("degree_name").like("%bachelor%") & ~col("degree_name").like("%science%") & ~col("degree_name").like("%arts%") & ~col("degree_name").like("%engineering%") & ~col("degree_name").like("%technology%")| 
        col("degree_name").like("%bacharelado%"),
        "bachelor's degree"
    )
    .when(
        col("degree_name").like("%phd%") | 
        col("degree_name").like("%ph.d%"), 
        "phd"
    )
    .when(
        col("degree_name").like("%high%") |
        col("degree_name").like("%graduação%") |
        col("degree_name").like("%school%") ,
        "high school"
    )
    .when(
        col("degree_name").like("%aa%")|
        col("degree_name").like("%a.a%"),
        "associate of arts"
    )
    .when(
        col("degree_name").like("%as%")|
        col("degree_name").like("%as.%")|
        col("degree_name").like("%associate of science%"),
        "associate of science"
    )
    .when(
        col("degree_name").like("%graduate%"),
        "graduate"
    )
    .otherwise(col("degree_name")) 
)

def getCompanyData(company):
    data = []
    linkedin_df_exploded = linkedin_df.withColumn("experiences", explode(col("experiences"))).withColumn(
    "education", explode(col("education"))
    )   
    linkedin_cleaned_df = linkedin_df_exploded.select(
        trim(lower(col("education.school"))).alias("university"),
        trim(lower(col("experiences.company"))).alias("company_name")
    )
    tcs_employees_df = linkedin_cleaned_df.filter(
        col("company_name").like(f"%{company}%")
    )
    tcs_university_count = tcs_employees_df.groupBy("university") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count"))
    total_employees = tcs_employees_df.count()
    top_university = tcs_university_count.limit(5).toPandas()
    data.append(top_university)

    linkedin_cleaned_df = linkedin_df_exploded.select(
        trim(lower(col("education.degree_name"))).alias("degree_name"),
        trim(lower(col("experiences.company"))).alias("company_name")
    )

    linkedin_cleaned_df = linkedin_cleaned_df.filter(
        col("degree_name").isNotNull() & (col("degree_name") != "") &
        col("company_name").isNotNull() & (col("company_name") != "")
    )

    linkedin_cleaned_df = SetDegree(linkedin_cleaned_df)
    tcs_employees_df = linkedin_cleaned_df.filter(
        col("company_name").like(f"%{company}%")
    )
    total_employees = tcs_employees_df.count()

    tcs_university_count = tcs_employees_df.groupBy("degree_name") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count"))
    top_university = tcs_university_count.limit(10).toPandas()
    data.append(top_university)

    linkedin_df_exploded = linkedin_df.withColumn("experiences", explode(col("experiences")))

    skills_df = linkedin_df_exploded.filter(
        col("skills").isNotNull() & (size(col("skills")) > 0)  
    ).select(
        explode(col("skills")).alias("skill"),  
        trim(lower(col("experiences.company"))).alias("company_name")  
    ).filter(
        col("company_name").like(f"%{company}%")  
    )

    top_skills_df = skills_df.groupBy("skill").agg(count("*").alias("count")).orderBy(col("count").desc())

    top_20_skills = top_skills_df.limit(20).toPandas()
    data.append(top_20_skills)
    return data
