import os
import re
import sys
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine


def get_response(url: str) -> list:
    """Collect the HTTP response from the URL.

    Args:
        url: URL for an API endpoint.

    Returns:
        response: A requests.Response object containing the HTTP response.
        status_code: An int with the HTTP status code.
    """

    n_attempts = 5

    # Make {n_attempts} to get a response. Return [response, status_code] if successful.
    for attempt in range(1, n_attempts + 1):

        try:
            response = requests.get(url)

            # 200 response is OK. Return it.
            if response.status_code == 200:
                return [response, response.status_code]

            # 429 response means we are sending too many requests. Wait 60 seconds before trying again.
            elif response.status_code == 429:
                print(
                    f"Too many requests on attempt number {attempt}. Backing off for 60 seconds..."
                )
                time.sleep(60)
                continue

            # If we did not get a 200 response, retry. If retries are exhausted, return what we have.
            else:
                if attempt < n_attempts:
                    continue
                elif attempt == n_attempts:
                    return [response, response.status_code]

        except requests.ConnectTimeout:
            print(f"No response from {url} on attempt number {attempt}. Retrying...")
            continue

    # Return [None, None] if all attempts fail.
    return [None, None]


def fetch_titles(url: str) -> list:
    """Collect the links to all titles in a category.

    Args:
        url: URL for a particular category. Should look like this:
             http://books.toscrape.com/catalogue/category/books/<category>

    Returns:
        links: A list containing all of the links for titles in the given
               category. The output links will look like this:
               http://books.toscrape.com/catalogue/<book_title>_<book_id>/index.html
    """

    response, status_code = get_response(url)

    if status_code == 200:
        html_doc = response.text
        soup = BeautifulSoup(html_doc, "html.parser")

        # The only tags with a title are the tags for book titles
        tags = soup.find_all(title=True)
        hrefs = [tag["href"] for tag in tags]

        # The original path looks like '../../../<book_title>/index.html'
        base_url = "http://books.toscrape.com/catalogue"
        pages = [str(href.rsplit("../")[-1]) for href in hrefs]
        links = [os.path.join(base_url, page) for page in pages]

    else:
        print("Bad status code. Cannot fetch links.")
        links = []

    return links


def fetch_title_info(url: str) -> pd.DataFrame:
    """Collect the data for a particular title.

    Args:
        url: URL for a particular book title. Should look like:
             http://books.toscrape.com/catalogue/<book_title>_<book_id>/index.html

    Returns:
        df: A dataframe containing the following fields:
              Title: str
              UPC: str
              Product Type: str
              Price (excl. tax): str
              Price (incl. tax): str
              Tax: str
              Availability: str
              Number of reviews: str
              Product Description: str
    """

    response, status_code = get_response(url)

    if status_code == 200:
        html_doc = response.text
        soup = BeautifulSoup(html_doc, "html.parser")

        product_description_tag = soup.find_all(id="product_description")[0]
        product_description = product_description_tag.next_sibling.next_sibling.text
        product_info = [tag.text for tag in soup.find_all("td")]

        fields = [
            "UPC",
            "Product Type",
            "Price (excl. tax)",
            "Price (incl. tax)",
            "Tax",
            "Availability",
            "Number of reviews",
        ]
        data = dict(zip(fields, product_info))
        data["Product Description"] = product_description

        # The original url looks like 'http://books.toscrape.com/catalogue/<book_title>_<book_id>/index.html'
        data["Title"] = url.split("/")[-2].split("_")[0]

        columns = ["Title"] + fields + ["Product Description"]
        df = pd.DataFrame(data, index=[0], columns=columns)

    else:
        print("Bad status code. Cannot fetch data.")
        df = pd.DataFrame(
            columns=[
                "Title",
                "UPC",
                "Product Type",
                "Price (excl. tax)",
                "Price (incl. tax)",
                "Tax",
                "Availability",
                "Number of reviews",
                "Product Description",
            ]
        )

    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process the data for ingestion.

    Args:
        df: A dataframe containing the following fields:
              Title: str
              UPC: str
              Product Type: str
              Price (excl. tax): str
              Price (incl. tax): str
              Tax: str
              Availability: str
              Number of reviews: str
              Product Description: str
    Returns:
        df: A dataframe containing the following fields:
              title: str
              upc: str
              product_type: str
              price_excl_tax: float
              price_incl_tax: float
              tax: float
              availability: int
              number_of_reviews: int
              product_description: str
    """

    def convert_price(price: str) -> float:
        return list(re.findall(r"\d+\.\d+", price))[0]

    def convert_availability(availability: str) -> int:
        return list(re.findall(r"\d+", availability))[0]

    df["Availability"] = df["Availability"].apply(convert_availability)
    df["Price (excl. tax)"] = df["Price (excl. tax)"].apply(convert_price)
    df["Price (incl. tax)"] = df["Price (incl. tax)"].apply(convert_price)
    df["Tax"] = df["Tax"].apply(convert_price)

    schema = {
        "title": np.dtype("object"),
        "upc": np.dtype("object"),
        "product_type": np.dtype("object"),
        "price_excl_tax": np.dtype("float64"),
        "price_incl_tax": np.dtype("float64"),
        "tax": np.dtype("float64"),
        "availability": np.dtype("int64"),
        "number_of_reviews": np.dtype("int64"),
        "product_description": np.dtype("object"),
    }
    columns = list(schema.keys())

    df.columns = columns
    df = df.astype(schema).reset_index(drop=True)

    return df


def load_data(
    df: pd.DataFrame,
    user: str,
    password: str,
    host: str,
    database: str,
    table: str,
) -> None:
    """Load the data into a MySQL DB.

    Args:
        df: A pandas dataframe containing data to load into MySQL.
        user: A username for MySQL.
        password: User's password.
        host: The host IP address for the database.
        database: The name of the database.
        table: The name of the table.

    Returns:
        None.
    """

    if "--no_db" in sys.argv:
        if not os.path.exists("data.csv"):
            df.to_csv("data.csv", index=False, mode="w", header=True)
            print(f"Succesfully Uploaded {df.shape[0]} Records.")
        else:
            df.to_csv("data.csv", index=False, mode="a", header=False)
            print(f"Succesfully Uploaded {df.shape[0]} Records.")
    else:
        try:
            connection_string = f"mysql+pymysql://{user}:{password}@{host}/{database}"
            engine = create_engine(connection_string)
            df.to_sql(name=table, con=engine, if_exists="append", index=False)

        except Exception as e:
            print("Failed to Load Data.")
            raise (e)
        else:
            print(f"Succesfully Uploaded {df.shape[0]} Records.")


if __name__ == "__main__":
    db_params = {
        "user": "jacob",
        "password": "password",
        "host": "localhost",
        "database": "book_db",
        "table": "BooksToScrape",
    }
    base_url = "http://books.toscrape.com/catalogue"
    science_url = os.path.join(base_url, "category/books/science_22")
    poetry_url = os.path.join(base_url, "category/books/poetry_23")

    science_links = fetch_titles(science_url)
    poetry_links = fetch_titles(poetry_url)

    science_data = [fetch_title_info(link) for link in science_links]
    poetry_data = [fetch_title_info(link) for link in poetry_links]

    science_df = pd.concat(science_data)
    poetry_df = pd.concat(poetry_data)

    clean_science_df = clean_data(science_df)
    clean_poetry_df = clean_data(poetry_df)

    clean_science_df["category"] = "Science"
    clean_poetry_df["category"] = "Poetry"

    load_data(df=clean_science_df, **db_params)
    load_data(df=clean_poetry_df, **db_params)
