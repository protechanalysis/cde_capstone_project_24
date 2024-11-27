import logging

from capstone_airflow.function.joining import tables_joining


def table_transformation():
    """
    This function transforms the country data by cleaning specific columns,
    creating new columns, and dropping unnecessary ones.
    """
    try:
        logging.info("Starting table transformation process.")

        # Retrieve the joined data from the previous function
        # country_join, status = tables_joining()
        country_join = tables_joining()
        # if country_join is None:
        #     logging.error(f"Error in table joining: {status}")
        #     return None, f"Error in table joining: {status}"

        logging.info("Data joined successfully for transformation.")

        # List of columns to clean and transform
        columns_to_check = ["continents", "idd.suffixes", "capital"]

        for col in columns_to_check:
            if col in country_join.columns:
                # Ensure the column is of string type
                country_join[col] = country_join[col].astype("string")
                # Remove unwanted characters (e.g., brackets, quotes)
                country_join[col] = country_join[col].str.replace(
                    r"[\[\]\']", "", regex=True
                )
                logging.info(f"Column {col} transformed successfully.")
            else:
                logging.warning(f"Column {col} not found in the data.")

        # Create the 'country_code' column by combining 'idd root and suffixes'
        if (
            "idd.root" in country_join.columns
            and "idd.suffixes" in country_join.columns
        ):
            country_join["country_code"] = (
                country_join["idd.root"] + country_join["idd.suffixes"]
            )
            logging.info("'country_code' column created successfully.")
        else:
            logging.warning(
                "Columns idd.root/suffixes not found to create country_code."
            )

        # Drop the 'idd root and suffixes' columns as they are no longer needed
        transform = country_join.drop(
            columns=["idd.root", "idd.suffixes"], errors="ignore"
        )
        logging.info("Unnecessary columns dropped successfully.")

        # Renaming columns
        logging.info("Starting the column renaming process.")

        transform.rename(
            columns={
                "name.common": "country_name",
                "independent": "independence",
                "unMember": "united_nation_members",
                "startOfWeek": "start_of_week",
                "name.official": "official_country_name",
                "name.nativeName.eng.common": "common_native_name",
            },
            inplace=True,
        )

        logging.info("Column renaming completed successfully.")

        return transform

    except Exception as e:
        raise Exception(f"Error during table transformation: {str(e)}")
