def load_business():
    snowflake_query = """
        CREATE OR REPLACE TEMPORARY TABLE temp_business_class AS
        SELECT * FROM school.data_schema.business_class WHERE 1 = 0;  -- Create an empty table with the same structure as the target

        COPY INTO temp_business_class
        FROM @school.data_schema.student
        PATTERN = 'business_class.csv'
        FILE_FORMAT = csv_format;


        MERGE INTO school.data_schema.business_class AS target
        USING temp_business_class AS source
        ON target.student_id = source.student_id  -- Match based on the unique identifier, like student_id
        WHEN NOT MATCHED THEN
        INSERT (student_id, Maths, English, Civic_education, Economics, Government,
        CRS, Accounting, Commerce, Marketing, Total_Grade_Points, Average_Score)
        VALUES (source.student_id, source.Maths, source.English, source.Civic_education,
        source.Economics, source.Government, source.CRS, source.Accounting,
        source.Commerce, source.Marketing, source.Total_Grade_Points, source.Average_Score);

        drop TABLE temp_business_class;
    """
    return snowflake_query


def load_arts():
    snowflake_query="""
        CREATE OR REPLACE TEMPORARY TABLE temp_arts_class AS
            SELECT * FROM school.data_schema.arts_class WHERE 1 = 0;  -- Create an empty table with the same structure as the target

        COPY INTO temp_arts_class
        FROM @school.data_schema.student
        PATTERN = 'arts_class.csv'
        FILE_FORMAT = csv_format;

        MERGE INTO school.data_schema.arts_class AS target
        USING temp_arts_class AS source
        ON target.student_id = source.student_id  -- Match based on the unique identifier, like student_id
        WHEN NOT MATCHED THEN
        INSERT (student_id, Maths, English, Civic_education, Economics, Government,
          CRS, Lit_in_Eng, Yoruba, History, Total_Grade_Points, Average_Score)
        VALUES (source.student_id, source.Maths, source.English, source.Civic_education,
          source.Economics, source.Government, source.CRS, source.Lit_in_Eng, source.Yoruba, source.History, source.Total_Grade_Points, source.Average_Score);

        DROP TABLE temp_arts_class;
        """
    return snowflake_query


def load_science():
    snowflake_query = """
        CREATE OR REPLACE TEMPORARY TABLE temp_science_class AS
            SELECT * FROM school.data_schema.science_class WHERE 1 = 0;  -- Create an empty table with the same structure as the target

        COPY INTO temp_science_class
        FROM @school.data_schema.student
        PATTERN = 'science_class.csv'
        FILE_FORMAT = csv_format;

        MERGE INTO school.data_schema.science_class AS target
        USING temp_science_class AS source
        ON target.student_id = source.student_id  -- Match based on the unique identifier, like student_id
        WHEN NOT MATCHED THEN
        INSERT (student_id, Maths, English, Civic_education, Economics, Biology,
        Chemistry, Physics, Geography, Agric, Total_Grade_Points, Average_Score)
        VALUES (source.student_id, source.Maths, source.English, source.Civic_education,
        source.Economics, source.Biology, source.Chemistry, source.Physics,
        source.Geography, source.Agric, source.Total_Grade_Points, source.Average_Score);
        drop TABLE temp_science_class;
    """
    return snowflake_query

def 