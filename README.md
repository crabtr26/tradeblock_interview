# Summary:
This project scrapes data from http://books.toscrape.com/ and loads it into a MySQL database. There are two components: create_db.py to initialize the MySQL database and main.py to scrape and load the data.

# What I did:
1. If you want to replicate what I did, you will need to download MYSQL from here - https://www.mysql.com/downloads/
2. And an Anaconda installation from here -https://www.anaconda.com/products/individual
3. To create an environment with all of the required dependencies, run the following command:`conda env create -f environment.yaml`
4. Activate the conda environment using: `conda activate my_env`
5. Create the db using `python create_db.py`. You will need to match db_params to your MySQL configuration.
6. Load the data using `python main.py`. Again, you will need to match db_params to your MySQL configuration.

# Recommended Testing instructions:
I used MySQL for demo purposes. If you don't want to mess with that, I added a flag to load the data into a csv file instead for convienience. The final output is the same.

1. You will need an Anaconda installation from here -https://www.anaconda.com/products/individual
1. Install the conda environment: `conda env create -f environment.yaml`
2. Activate the conda environment: `conda activate my_env`
3. Load the data into a csv file using `python main.py --no_db`.
