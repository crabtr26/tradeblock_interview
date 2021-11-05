from sqlalchemy import create_engine
from sqlalchemy.sql import text
from typing import Optional


def execute_sql(
    sql: str,
    user: str,
    password: str,
    host: str,
    database: Optional[str] = None,
) -> None:
    """Execute the provided SQL.

    Args:
        sql: A valid SQL statement to be executed.
        user: A username to login to the database with.
        password: The user's password.
        host: The host IP for the database.
        database: The name of the database.

    Returns:
        None.
    """

    if database:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}/{database}"
    else:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}"
    try:
        engine = create_engine(connection_string)
        engine.execute(text(sql))
    except Exception as e:
        print(f"Failed to execute the provided sql.")
        raise (e)
    else:
        print(f"SQL Execution was successful.")


if __name__ == "__main__":
    db_name = "book_db"
    table_name = "BooksToScrape"
    db_sql = f"CREATE DATABASE IF NOT EXISTS {db_name};"
    table_sql = (
        f"CREATE TABLE IF NOT EXISTS {db_name}.{table_name}("
        "title TEXT,"
        "upc TEXT,"
        "product_type TEXT,"
        "price_excl_tax DOUBLE,"
        "price_incl_tax DOUBLE,"
        "tax DOUBLE,"
        "availability INT,"
        "number_of_reviews INT,"
        "product_description TEXT,"
        "category TEXT"
        ");"
    )
    db_params = {
        "user": "jacob",
        "password": "password",
        "host": "localhost",
    }
    execute_sql(sql=db_sql, **db_params)
    execute_sql(sql=table_sql, database=db_name, **db_params)
