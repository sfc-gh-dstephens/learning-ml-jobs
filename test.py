from snowflake.snowpark import Session
import cleaner

def generate_dataset_sql(db, schema, table_name, num_rows, num_cols) -> str:
    cleaner.clean("     another   ")
    sql_script = f"CREATE TABLE IF NOT EXISTS {db}.{schema}.{table_name} AS \n"
    sql_script += f"SELECT \n"
    for i in range(1, num_cols):
        sql_script += f"uniform(0::FLOAT, 10::FLOAT, random()) AS FEATURE_{i}, \n"
    sql_script += f"FEATURE_1 + FEATURE_1 AS TARGET_1 \n"
    sql_script += f"FROM TABLE(generator(rowcount=>({num_rows})));"
    return sql_script

def main():
    # Configure dataset parameters
    num_rows = 1000
    num_cols = 10
    table_name = "LEARN_ML_JOB_DS"

    # Create the dataset in Snowflake
    session = Session.builder.getOrCreate()
    session.sql(generate_dataset_sql(session.get_current_database(), session.get_current_schema(),
                            table_name, num_rows, num_cols)).collect()

if __name__ == "__main__":
    main()