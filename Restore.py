{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "84b034fa",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Import PostgreSQL adapter and libraries\n",
    "import psycopg2\n",
    "import pandas as pd\n",
    "# Write connection parameters\n",
    "hostname = 'localhost'\n",
    "database = 'highlands'\n",
    "username = 'postgres'\n",
    "pwd = 'trust'\n",
    "port_id = 5432 \n",
    "# Initialize connection and cursor\n",
    "# Assign to None to avoid UnboundLocalError in finally block\n",
    "conn = None\n",
    "cur = None\n",
    "# Connect to the PostgreSQL server\n",
    "# Use try-except-finally to ensure proper cleanup\n",
    "try:\n",
    "    # Create connection with PostgreSQL database\n",
    "    conn = psycopg2.connect( \n",
    "        host=hostname,\n",
    "        dbname=database,\n",
    "        user=username,\n",
    "        password=pwd,\n",
    "        port=port_id\n",
    "    )\n",
    "\n",
    "    # Create a cursor object\n",
    "    cur = conn.cursor()\n",
    "\n",
    "    # Table name list\n",
    "    table_names = [\n",
    "        'brand_health',\n",
    "        'brand_image',\n",
    "        'companion',\n",
    "        'competitor_data_for_filter',\n",
    "        'day_of_week',\n",
    "        'day_part',\n",
    "        'need_state',\n",
    "        'segmentation_2017',\n",
    "        'survey_respondents_info'\n",
    "    ]\n",
    "\n",
    "    # Load all tables into DataFrames, assign to variables, and print first 5 rows\n",
    "    for table in table_names:\n",
    "        cur.execute(f'SELECT * FROM public.{table}')\n",
    "        rows = cur.fetchall()\n",
    "        colnames = [desc[0] for desc in cur.description]\n",
    "        df = pd.DataFrame(rows, columns=colnames)\n",
    "        globals()[table + '_df'] = df\n",
    "\n",
    "    # Commit any changes (not strictly needed for SELECT, but kept for completeness)\n",
    "    conn.commit()\n",
    "\n",
    "# Return error if any occurs\n",
    "except Exception as error:\n",
    "    print(error)\n",
    "\n",
    "# Close cursor and connection to free resources\n",
    "finally:\n",
    "    if cur is not None:\n",
    "        cur.close()\n",
    "    if conn is not None:\n",
    "        conn.close()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "85f87497",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "Error importing in API mode: ImportError('On Windows, cffi mode \"ANY\" is only \"ABI\".')\n",
      "Trying to import in ABI mode.\n"
     ]
    }
   ],
   "source": [
    "# Import data preprocessing libraries\n",
    "import numpy as np\n",
    "import matplotlib.pyplot as plt\n",
    "import seaborn as sns\n",
    "import re\n",
    "from sklearn import preprocessing\n",
    "\n",
    "# Load rpy2 extension to integrate R in Jupyter Notebook\n",
    "%load_ext rpy2.ipython"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "ad82b1fc",
   "metadata": {},
   "source": [
    "## Data Screening\n",
    "**Goals:** To understand distribution of data, view distinct observation in each column and determine missingness type of each column\n",
    "\n",
    "1. Screen all rows of all table to standadize columns' names and format\n",
    "    - Convert CamelCase to snake_case\n",
    "    - Replace '#' by '_' for readability with regex\n",
    "    - Convert all to lowercase"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "0531e93d",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "['ID', 'City', 'Group_size', 'Age', 'MPI#Mean', 'TOM', 'BUMO', 'BUMO_Previous', 'MostFavourite', 'Gender', 'MPI#detail', 'Age#group', 'Age#Group#2', 'MPI', 'MPI#2', 'Occupation', 'Occupation#group', 'Year', 'Col', 'MPI_Mean_Use']\n",
      "['ID', 'Year', 'City', 'Brand', 'Spontaneous', 'Awareness', 'Trial', 'P3M', 'P1M', 'Comprehension', 'Brand_Likability', 'Weekly', 'Daily', 'Fre#visit', 'PPA', 'Spending', 'Segmentation', 'NPS#P3M', 'NPS#P3M#Group', 'Spending_use']\n",
      "['ID', 'Year', 'City', 'Awareness', 'Attribute', 'BrandImage']\n",
      "['ID', 'City', 'Companion#group', 'Year']\n",
      "['No#', 'Brand', 'City', 'Year', 'StoreCount']\n",
      "['ID', 'City', 'Dayofweek', 'Visit#Dayofweek', 'Year', 'Weekday#end']\n",
      "['ID', 'City', 'Daypart', 'Visit#Daypart', 'Year']\n",
      "['ID', 'City', 'Year', 'Needstates', 'Day#Daypart', 'NeedstateGroup']\n",
      "['ID', 'Segmentation', 'Visit', 'Spending', 'Brand', 'PPA']\n"
     ]
    }
   ],
   "source": [
    "# Return columns of all dataframes  \n",
    "print([column for column in survey_respondents_info_df.columns])\n",
    "print([column for column in brand_health_df.columns])\n",
    "print([column for column in brand_image_df.columns])\n",
    "print([column for column in companion_df.columns])\n",
    "print([column for column in competitor_data_for_filter_df.columns])\n",
    "print([column for column in day_of_week_df.columns])\n",
    "print([column for column in day_part_df.columns])\n",
    "print([column for column in need_state_df.columns])\n",
    "print([column for column in segmentation_2017_df.columns])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "c3bcc834",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "DataFrame 1 columns: ['ID', 'City', 'Group_size', 'Age', 'MPI_Mean', 'TOM', 'BUMO', 'BUMO_Previous', 'MostFavourite', 'Gender', 'MPI_detail', 'Age_group', 'Age_Group_2', 'MPI', 'MPI_2', 'Occupation', 'Occupation_group', 'Year', 'Col', 'MPI_Mean_Use']\n",
      "DataFrame 2 columns: ['ID', 'Year', 'City', 'Brand', 'Spontaneous', 'Awareness', 'Trial', 'P3M', 'P1M', 'Comprehension', 'Brand_Likability', 'Weekly', 'Daily', 'Fre_visit', 'PPA', 'Spending', 'Segmentation', 'NPS_P3M', 'NPS_P3M_Group', 'Spending_use']\n",
      "DataFrame 3 columns: ['ID', 'Year', 'City', 'Awareness', 'Attribute', 'BrandImage']\n",
      "DataFrame 4 columns: ['ID', 'City', 'Companion_group', 'Year']\n",
      "DataFrame 5 columns: ['No_', 'Brand', 'City', 'Year', 'StoreCount']\n",
      "DataFrame 6 columns: ['ID', 'City', 'Dayofweek', 'Visit_Dayofweek', 'Year', 'Weekday_end']\n",
      "DataFrame 7 columns: ['ID', 'City', 'Daypart', 'Visit_Daypart', 'Year']\n",
      "DataFrame 8 columns: ['ID', 'City', 'Year', 'Needstates', 'Day_Daypart', 'NeedstateGroup']\n",
      "DataFrame 9 columns: ['ID', 'Segmentation', 'Visit', 'Spending', 'Brand', 'PPA']\n"
     ]
    }
   ],
   "source": [
    "# Replace '#' in column names with '_' for all dataframes\n",
    "dfs = [\n",
    "    survey_respondents_info_df,\n",
    "    brand_health_df,\n",
    "    brand_image_df,\n",
    "    companion_df,\n",
    "    competitor_data_for_filter_df,\n",
    "    day_of_week_df,\n",
    "    day_part_df,\n",
    "    need_state_df,\n",
    "    segmentation_2017_df\n",
    "    ]\n",
    "\n",
    "for df in dfs:\n",
    "    df.columns = df.columns.str.replace('#', '_', regex=False)\n",
    "for i, df in enumerate(dfs, start=1):\n",
    "    print(f\"DataFrame {i} columns: {df.columns.tolist()}\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "2a36cbf4",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Function to split CamelCase and keep acronyms\n",
    "import re\n",
    "def split_camel_case_keep_acronyms(name):\n",
    "    \"\"\"\n",
    "    Convert CamelCase to Snake_Case while keeping acronyms intact.\n",
    "    \n",
    "    Examples:\n",
    "    - 'MostFavourite' -> 'Most_Favourite'\n",
    "    - 'CustomerMPIValue' -> 'Customer_MPI_Value'\n",
    "    - 'NPSScore' -> 'NPS_Score'\n",
    "    \"\"\"\n",
    "    # Insert underscore between:\n",
    "    #   1. a lowercase letter and uppercase letter (e.g., tM -> t_M)\n",
    "    #   2. but NOT between consecutive uppercase letters (e.g., MPI stays MPI)\n",
    "    return re.sub(r'(?<=[a-z])(?=[A-Z])', '_', name)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "1ede8a74",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "DataFrame 1 columns: ['ID', 'City', 'Group_size', 'Age', 'MPI_Mean', 'TOM', 'BUMO', 'BUMO_Previous', 'Most_Favourite', 'Gender', 'MPI_detail', 'Age_group', 'Age_Group_2', 'MPI', 'MPI_2', 'Occupation', 'Occupation_group', 'Year', 'Col', 'MPI_Mean_Use']\n",
      "DataFrame 2 columns: ['ID', 'Year', 'City', 'Brand', 'Spontaneous', 'Awareness', 'Trial', 'P3M', 'P1M', 'Comprehension', 'Brand_Likability', 'Weekly', 'Daily', 'Fre_visit', 'PPA', 'Spending', 'Segmentation', 'NPS_P3M', 'NPS_P3M_Group', 'Spending_use']\n",
      "DataFrame 3 columns: ['ID', 'Year', 'City', 'Awareness', 'Attribute', 'Brand_Image']\n",
      "DataFrame 4 columns: ['ID', 'City', 'Companion_group', 'Year']\n",
      "DataFrame 5 columns: ['No_', 'Brand', 'City', 'Year', 'Store_Count']\n",
      "DataFrame 6 columns: ['ID', 'City', 'Dayofweek', 'Visit_Dayofweek', 'Year', 'Weekday_end']\n",
      "DataFrame 7 columns: ['ID', 'City', 'Daypart', 'Visit_Daypart', 'Year']\n",
      "DataFrame 8 columns: ['ID', 'City', 'Year', 'Needstates', 'Day_Daypart', 'Needstate_Group']\n",
      "DataFrame 9 columns: ['ID', 'Segmentation', 'Visit', 'Spending', 'Brand', 'PPA']\n"
     ]
    }
   ],
   "source": [
    "# Apply the function to all DataFrames\n",
    "for df in dfs:\n",
    "    df.columns = [split_camel_case_keep_acronyms(column) for column in df.columns]\n",
    "for i, df in enumerate(dfs, start=1):\n",
    "    print(f\"DataFrame {i} columns: {df.columns.tolist()}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "f1a8ca38",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "DataFrame 1 columns: ['id', 'city', 'group_size', 'age', 'mpi_mean', 'tom', 'bumo', 'bumo_previous', 'most_favourite', 'gender', 'mpi_detail', 'age_group', 'age_group_2', 'mpi', 'mpi_2', 'occupation', 'occupation_group', 'year', 'col', 'mpi_mean_use']\n",
      "DataFrame 2 columns: ['id', 'year', 'city', 'brand', 'spontaneous', 'awareness', 'trial', 'p3m', 'p1m', 'comprehension', 'brand_likability', 'weekly', 'daily', 'fre_visit', 'ppa', 'spending', 'segmentation', 'nps_p3m', 'nps_p3m_group', 'spending_use']\n",
      "DataFrame 3 columns: ['id', 'year', 'city', 'awareness', 'attribute', 'brand_image']\n",
      "DataFrame 4 columns: ['id', 'city', 'companion_group', 'year']\n",
      "DataFrame 5 columns: ['no_', 'brand', 'city', 'year', 'store_count']\n",
      "DataFrame 6 columns: ['id', 'city', 'dayofweek', 'visit_dayofweek', 'year', 'weekday_end']\n",
      "DataFrame 7 columns: ['id', 'city', 'daypart', 'visit_daypart', 'year']\n",
      "DataFrame 8 columns: ['id', 'city', 'year', 'needstates', 'day_daypart', 'needstate_group']\n",
      "DataFrame 9 columns: ['id', 'segmentation', 'visit', 'spending', 'brand', 'ppa']\n"
     ]
    }
   ],
   "source": [
    "# Convert to lowercase\n",
    "for df in dfs:\n",
    "    df.columns = [column.lower() for column in df.columns]\n",
    "for i, df in enumerate(dfs, start=1):\n",
    "    print(f\"DataFrame {i} columns: {df.columns.tolist()}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "f397b5b1",
   "metadata": {},
   "source": [
    "### I. Survey Respondent Info Table\n",
    "**Description:** Table contains the infomation of each respondent including demographics info. \n",
    "\n",
    "**Preprocessing Step:**\n",
    "\n",
    "1. Remove 'col' column - redundant column\n",
    "\n",
    "2. Check missing columns\n",
    "\n",
    "    - Ensure no missing values in important identity columns: year, id\n",
    "\n",
    "3. Ensure respondants' ID are unique\n",
    "\n",
    "4. Standadize observations in categorical columns: 'occupation' and brand-related columns\n",
    "\n",
    "    - Occupations are grouped into different categories for future analysis\n",
    "\n",
    "5. Impute the missing values in age with the median of specified demographics(mpi_detail, occupation, city)\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "4e4abd4a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "       id     city  group_size   age  mpi_mean      tom     bumo  \\\n",
      "0  348226  Cần Thơ         3.0  29.0    5499.0  Other 1  Other 1   \n",
      "1  358485   Hà Nội         3.0  25.0    5499.0  Other 1  Other 1   \n",
      "2  360729  Cần Thơ         3.0  25.0    5499.0  Other 1  Other 1   \n",
      "3  360737  Cần Thơ         3.0  24.0    5499.0  Other 1  Other 1   \n",
      "4  361753  Cần Thơ         3.0  26.0    5499.0  Other 1  Other 1   \n",
      "\n",
      "  bumo_previous most_favourite  gender  \\\n",
      "0          None        Other 1  Female   \n",
      "1          None        Other 1    Male   \n",
      "2          None        Other 1  Female   \n",
      "3          None        Other 1    Male   \n",
      "4          None        Other 1    Male   \n",
      "\n",
      "                               mpi_detail age_group   age_group_2  \\\n",
      "0  From 4.5 millions to 6.49 millions VND   20 - 29  25 - 29 y.o.   \n",
      "1  From 4.5 millions to 6.49 millions VND   20 - 29  25 - 29 y.o.   \n",
      "2  From 4.5 millions to 6.49 millions VND   20 - 29  25 - 29 y.o.   \n",
      "3  From 4.5 millions to 6.49 millions VND   20 - 29  20 - 24 y.o.   \n",
      "4  From 4.5 millions to 6.49 millions VND   20 - 29  25 - 29 y.o.   \n",
      "\n",
      "                   mpi                  mpi_2  \\\n",
      "0  VND 4.5m - VND 8.9m  2.VND 4.5m - VND 8.9m   \n",
      "1  VND 4.5m - VND 8.9m  2.VND 4.5m - VND 8.9m   \n",
      "2  VND 4.5m - VND 8.9m  2.VND 4.5m - VND 8.9m   \n",
      "3  VND 4.5m - VND 8.9m  2.VND 4.5m - VND 8.9m   \n",
      "4  VND 4.5m - VND 8.9m  2.VND 4.5m - VND 8.9m   \n",
      "\n",
      "                                          occupation occupation_group  year  \\\n",
      "0  Unskilled Labor (worker, landry person, driver...      Blue Collar  2018   \n",
      "1  Unskilled Labor (worker, landry person, driver...      Blue Collar  2018   \n",
      "2  Unskilled Labor (worker, landry person, driver...      Blue Collar  2018   \n",
      "3  Skilled Labor (tailor, machinist, carpenter, e...      Blue Collar  2018   \n",
      "4  Semi-skilled labor (salesperson, waiter, photo...      Blue Collar  2018   \n",
      "\n",
      "   col  mpi_mean_use  \n",
      "0    3        5499.0  \n",
      "1    3        5499.0  \n",
      "2    3        5499.0  \n",
      "3    3        5499.0  \n",
      "4    3        5499.0  \n",
      "--------------------------------------------------\n",
      "                  id    group_size           age       mpi_mean          year  \\\n",
      "count   11761.000000  11746.000000  11752.000000    8044.000000  11761.000000   \n",
      "mean   443662.766006      3.287843     35.233237    7335.741671   2017.988436   \n",
      "std    267593.520685      1.332049     10.829025    4667.292681      0.784221   \n",
      "min     89100.000000      1.000000     16.000000    1499.000000   2017.000000   \n",
      "25%    138421.000000      2.000000     27.000000    5499.000000   2017.000000   \n",
      "50%    434078.000000      3.000000     34.000000    6999.000000   2018.000000   \n",
      "75%    767775.000000      4.000000     43.000000    8249.000000   2019.000000   \n",
      "max    863754.000000     35.000000     60.000000  112499.000000   2019.000000   \n",
      "\n",
      "                col   mpi_mean_use  \n",
      "count  11761.000000    8044.000000  \n",
      "mean       3.220900    7335.741671  \n",
      "std        1.105033    4667.292681  \n",
      "min        0.000000    1499.000000  \n",
      "25%        2.000000    5499.000000  \n",
      "50%        3.000000    6999.000000  \n",
      "75%        4.000000    8249.000000  \n",
      "max        5.000000  112499.000000  \n",
      "--------------------------------------------------\n",
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 11761 entries, 0 to 11760\n",
      "Data columns (total 20 columns):\n",
      " #   Column            Non-Null Count  Dtype  \n",
      "---  ------            --------------  -----  \n",
      " 0   id                11761 non-null  int64  \n",
      " 1   city              11761 non-null  object \n",
      " 2   group_size        11746 non-null  float64\n",
      " 3   age               11752 non-null  float64\n",
      " 4   mpi_mean          8044 non-null   float64\n",
      " 5   tom               11761 non-null  object \n",
      " 6   bumo              11761 non-null  object \n",
      " 7   bumo_previous     6096 non-null   object \n",
      " 8   most_favourite    11761 non-null  object \n",
      " 9   gender            11761 non-null  object \n",
      " 10  mpi_detail        8076 non-null   object \n",
      " 11  age_group         11752 non-null  object \n",
      " 12  age_group_2       11752 non-null  object \n",
      " 13  mpi               8044 non-null   object \n",
      " 14  mpi_2             8044 non-null   object \n",
      " 15  occupation        11761 non-null  object \n",
      " 16  occupation_group  11761 non-null  object \n",
      " 17  year              11761 non-null  int64  \n",
      " 18  col               11761 non-null  int64  \n",
      " 19  mpi_mean_use      8044 non-null   float64\n",
      "dtypes: float64(4), int64(3), object(13)\n",
      "memory usage: 1.8+ MB\n",
      "None\n"
     ]
    }
   ],
   "source": [
    "#Describe data distribution and data type\n",
    "print(survey_respondents_info_df.head())\n",
    "print(\"-\" * 50)\n",
    "print(survey_respondents_info_df.describe())\n",
    "print(\"-\" * 50)\n",
    "print(survey_respondents_info_df.info())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "9bcd77d3",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "       id  gender     city  year  group_size      tom     bumo bumo_previous  \\\n",
      "0  348226  Female  Cần Thơ  2018         3.0  Other 1  Other 1          None   \n",
      "1  358485    Male   Hà Nội  2018         3.0  Other 1  Other 1          None   \n",
      "2  360729  Female  Cần Thơ  2018         3.0  Other 1  Other 1          None   \n",
      "3  360737    Male  Cần Thơ  2018         3.0  Other 1  Other 1          None   \n",
      "4  361753    Male  Cần Thơ  2018         3.0  Other 1  Other 1          None   \n",
      "\n",
      "  most_favourite   age age_group   age_group_2                  mpi  \\\n",
      "0        Other 1  29.0   20 - 29  25 - 29 y.o.  VND 4.5m - VND 8.9m   \n",
      "1        Other 1  25.0   20 - 29  25 - 29 y.o.  VND 4.5m - VND 8.9m   \n",
      "2        Other 1  25.0   20 - 29  25 - 29 y.o.  VND 4.5m - VND 8.9m   \n",
      "3        Other 1  24.0   20 - 29  20 - 24 y.o.  VND 4.5m - VND 8.9m   \n",
      "4        Other 1  26.0   20 - 29  25 - 29 y.o.  VND 4.5m - VND 8.9m   \n",
      "\n",
      "                               mpi_detail  mpi_mean  \\\n",
      "0  From 4.5 millions to 6.49 millions VND    5499.0   \n",
      "1  From 4.5 millions to 6.49 millions VND    5499.0   \n",
      "2  From 4.5 millions to 6.49 millions VND    5499.0   \n",
      "3  From 4.5 millions to 6.49 millions VND    5499.0   \n",
      "4  From 4.5 millions to 6.49 millions VND    5499.0   \n",
      "\n",
      "                                          occupation  \n",
      "0  Unskilled Labor (worker, landry person, driver...  \n",
      "1  Unskilled Labor (worker, landry person, driver...  \n",
      "2  Unskilled Labor (worker, landry person, driver...  \n",
      "3  Skilled Labor (tailor, machinist, carpenter, e...  \n",
      "4  Semi-skilled labor (salesperson, waiter, photo...  \n"
     ]
    }
   ],
   "source": [
    "# Rearrange and remove redundant columns\n",
    "\n",
    "# Remove redundant columns\n",
    "redundant_columns = ['col', 'mpi_2', 'occupation_group', 'mpi_mean_use']\n",
    "survey_respondents_info_df = survey_respondents_info_df.drop(columns=redundant_columns, errors='ignore')\n",
    "\n",
    "# Define the desired order of columns\n",
    "first_columns = ['id', 'gender', 'city', 'year', 'group_size', 'tom','bumo','bumo_previous', 'most_favourite']\n",
    "\n",
    "other_columns = [column for column in survey_respondents_info_df.columns if column not in first_columns]\n",
    "\n",
    "\n",
    "# Sort other columns alphabetically\n",
    "sorted_other_columns = sorted(other_columns)\n",
    "\n",
    "# Combine the lists to get the new column order\n",
    "new_column_order = first_columns + sorted_other_columns\n",
    "\n",
    "# Reorder the DataFrame columns\n",
    "survey_respondents_info_df = survey_respondents_info_df[new_column_order]\n",
    "\n",
    "print(survey_respondents_info_df.head())"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "2001a8c9",
   "metadata": {},
   "source": [
    "#### Check ID uniqueness"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "dfe713bd",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create a function to check ID uniqueness\n",
    "\n",
    "def check_id_uniqueness(df, id_column='id'):\n",
    "    \"\"\"\n",
    "    Check if the specified ID column in a DataFrame contains unique values.\n",
    "    Prints duplicate IDs and their counts if any are found.\n",
    "\n",
    "    Steps:\n",
    "    1. Count frequency of each ID.\n",
    "    2. If all frequencies are 1, IDs are unique.\n",
    "    3. If any frequency > 1, there are duplicate IDs.\n",
    "    4. Print duplicate IDs with their counts.\n",
    "    \"\"\"\n",
    "    # Step 1: Count frequency of each ID\n",
    "    id_counts = df[id_column].value_counts()\n",
    "\n",
    "    # Step 2: Check if all IDs are unique\n",
    "    if (id_counts == 1).all():\n",
    "        print(\"All IDs are unique.\")\n",
    "    else:\n",
    "        print(\"Duplicate IDs found!\")\n",
    "\n",
    "        # Step 3: Filter IDs where frequency > 1\n",
    "        duplicates = id_counts[id_counts > 1]\n",
    "\n",
    "        # Step 4: Print duplicate IDs and their counts\n",
    "        print(\"\\nDuplicated IDs and their counts:\")\n",
    "        print(duplicates)\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "60577b4b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "All IDs are unique.\n",
      "--------------------------------------------------\n"
     ]
    }
   ],
   "source": [
    "# Check ID uniqueness in the survey_respondents_info_df DataFrame\n",
    "check_id_uniqueness(survey_respondents_info_df, id_column='id')\n",
    "print(\"-\" * 50)"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "7f5aa83b",
   "metadata": {},
   "source": [
    "#### Standardize occupations"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "29c86f84",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Unique checker class:\n",
    "class uniqueness_checker:\n",
    "    def __init__(self,dataframe = None):\n",
    "        self.dataframe = dataframe\n",
    "    def show_unique(self):\n",
    "        if self.dataframe is None:\n",
    "            print(\"Please select a dataframe.\")\n",
    "            return\n",
    "        for column_name in self.dataframe.columns:\n",
    "            print(f\"Unique values in column '{column_name}':\")\n",
    "            print(f\"Data type: '{self.dataframe[column_name].dtype}'\")\n",
    "            print(self.dataframe[column_name].unique())\n",
    "            print(\"-\" * 50)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "c4719fcd",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Unique values in column 'id':\n",
      "Data type: 'int64'\n",
      "[348226 358485 360729 ... 827936 852627 863754]\n",
      "--------------------------------------------------\n",
      "Unique values in column 'gender':\n",
      "Data type: 'object'\n",
      "['Female' 'Male']\n",
      "--------------------------------------------------\n",
      "Unique values in column 'city':\n",
      "Data type: 'object'\n",
      "['Cần Thơ' 'Hà Nội' 'Hồ Chí Minh' 'Đà Nẵng' 'Hải Phòng' 'Nha Trang']\n",
      "--------------------------------------------------\n",
      "Unique values in column 'year':\n",
      "Data type: 'int64'\n",
      "[2018 2019 2017]\n",
      "--------------------------------------------------\n",
      "Unique values in column 'group_size':\n",
      "Data type: 'float64'\n",
      "[ 3.  4.  5.  2.  1.  6. 10.  8.  7. nan 28. 30. 35. 24.  9. 12. 20.]\n",
      "--------------------------------------------------\n",
      "Unique values in column 'tom':\n",
      "Data type: 'object'\n",
      "['Other 1' 'Highlands Coffee' 'Passio' 'Trung Nguyên'\n",
      " 'Street / Half street coffee (including carts)' 'Aha Cafe' 'Milano'\n",
      " 'Cộng Cà Phê' 'Mê Trang' 'The Coffee House' 'Other 2' 'Runam cafe'\n",
      " 'Long Cafe' 'Urban Station' 'Starbucks' 'Nia cafe' 'Viva Star'\n",
      " 'Phúc Long' 'Other 3' 'Mộc Miên' 'BonPas' 'The Cups Coffee'\n",
      " 'Indepedent Cafe' 'Effoc' 'Cheese Coffee' 'Thức Coffee'\n",
      " 'Other Branded Cafe Chain' 'Coffee Bean & Tea Leaf' 'Maxx Coffee'\n",
      " 'Gong Cha' 'KOI cafe' 'The Coffee Factory']\n",
      "--------------------------------------------------\n",
      "Unique values in column 'bumo':\n",
      "Data type: 'object'\n",
      "['Other 1' 'Highlands Coffee' 'Milano' 'Urban Station' 'Other 2'\n",
      " 'Street / Half street coffee (including carts)' 'Aha Cafe' 'Passio'\n",
      " 'Cộng Cà Phê' 'Viva Star' 'Trung Nguyên' 'Phúc Long' 'Nia cafe'\n",
      " 'The Coffee House' 'Mộc Miên' 'Other 3' 'Long Cafe'\n",
      " 'Coffee Bean & Tea Leaf' 'BonPas' 'Starbucks' 'Mê Trang'\n",
      " 'The Cups Coffee' 'Thức Coffee' 'Effoc' 'Cheese Coffee'\n",
      " 'Other Branded Cafe Chain' 'KOI cafe' 'Maxx Coffee' 'Gong Cha'\n",
      " 'Indepedent Cafe' 'Runam cafe' 'Đen Đá' 'Saigon Café']\n",
      "--------------------------------------------------\n",
      "Unique values in column 'bumo_previous':\n",
      "Data type: 'object'\n",
      "[None \"Don't have any brands\"\n",
      " 'Street / Half street coffee (including carts)' 'Indepedent Cafe'\n",
      " 'Highlands Coffee' 'Trung Nguyên' 'Other Branded Cafe Chain' 'Milano'\n",
      " 'Saigon Café' 'Cộng Cà Phê' 'Thức Coffee' 'The Coffee House'\n",
      " 'Coffee Bean & Tea Leaf' 'Maxx Coffee' 'KOI cafe' 'Starbucks' 'Phúc Long'\n",
      " 'Urban Station' 'Gong Cha' 'Effoc' 'Passio' 'Other 2' 'Aha Cafe'\n",
      " 'Other 3' 'Viva Star' 'Mê Trang' 'Long Cafe' 'Mộc Miên' 'Other 1'\n",
      " 'Nia cafe' 'Đen Đá' 'BonPas' 'Runam cafe']\n",
      "--------------------------------------------------\n",
      "Unique values in column 'most_favourite':\n",
      "Data type: 'object'\n",
      "['Other 1' 'Highlands Coffee' 'Milano' 'Urban Station' 'Other 2'\n",
      " 'Trung Nguyên' 'Viva Star'\n",
      " 'Street / Half street coffee (including carts)' 'Cộng Cà Phê' 'Nia cafe'\n",
      " 'Long Cafe' 'BonPas' 'Other 3' 'The Coffee House' 'Aha Cafe' 'Starbucks'\n",
      " 'Mộc Miên' 'Runam cafe' 'Phúc Long' 'Passio' 'Coffee Bean & Tea Leaf'\n",
      " 'Mê Trang' 'Thức Coffee' 'Đen Đá' 'Cheese Coffee' 'The Cups Coffee'\n",
      " 'Effoc' 'Gong Cha' 'Indepedent Cafe' 'Other Branded Cafe Chain'\n",
      " 'KOI cafe' 'Maxx Coffee' 'The Coffee Factory' 'Saigon Café']\n",
      "--------------------------------------------------\n",
      "Unique values in column 'age':\n",
      "Data type: 'float64'\n",
      "[29. 25. 24. 26. 33. 34. 28. 37. 35. 36. 32. 45. 48. 55. 43. 50. 53. 60.\n",
      " 39. 46. 54. 21. 49. 38. 23. 30. 51. 58. 52. 44. 27. 31. 22. 40. 42. 56.\n",
      " 17. 16. 57. 47. 59. 41. 19. 18. 20. nan]\n",
      "--------------------------------------------------\n",
      "Unique values in column 'age_group':\n",
      "Data type: 'object'\n",
      "['20 - 29' '30 - 39' '40 - 60' '16 - 19' None]\n",
      "--------------------------------------------------\n",
      "Unique values in column 'age_group_2':\n",
      "Data type: 'object'\n",
      "['25 - 29 y.o.' '20 - 24 y.o.' '30 - 34 y.o.' '35 - 39 y.o.' '45+ y.o.'\n",
      " '40 - 44 y.o.' '16 - 19 y.o.' None]\n",
      "--------------------------------------------------\n",
      "Unique values in column 'mpi':\n",
      "Data type: 'object'\n",
      "['VND 4.5m - VND 8.9m' None 'Under VND 4.5m' 'VND 9m - VND 14.9m'\n",
      " 'VND 15m - VND 24.9m' 'VND 25m+']\n",
      "--------------------------------------------------\n",
      "Unique values in column 'mpi_detail':\n",
      "Data type: 'object'\n",
      "['From 4.5 millions to 6.49 millions VND'\n",
      " 'From 6.5 millions to 7.49 millions VND'\n",
      " 'From 7.5 millions to 8.99 millions VND' None\n",
      " 'From 3 millions to 4.49 millions VND' 'Under 3 millions VND' 'Refuse'\n",
      " 'From 9 millions to 11.99 millions VND'\n",
      " 'From 12 millions to 14.99 millions VND'\n",
      " 'From 20 millions to 24.99 millions VND'\n",
      " 'From 15 millions to 19.99 millions VND'\n",
      " 'From 25 millions to 29.99 millions VND'\n",
      " 'From 30 millions to 44.99 millions VND'\n",
      " 'From 45 millions to 74.99 millions VND'\n",
      " 'From 75 million to VND 149.99 million VND']\n",
      "--------------------------------------------------\n",
      "Unique values in column 'mpi_mean':\n",
      "Data type: 'float64'\n",
      "[  5499.   6999.   8249.     nan   3749.   1499.  10499.  13499.  22499.\n",
      "  17499.  27499.  37499.  59999. 112499.]\n",
      "--------------------------------------------------\n",
      "Unique values in column 'occupation':\n",
      "Data type: 'object'\n",
      "['Unskilled Labor (worker, landry person, driver, security guard, cleaner)'\n",
      " 'Skilled Labor (tailor, machinist, carpenter, electrician)'\n",
      " 'Semi-skilled labor (salesperson, waiter, photographer)'\n",
      " 'Officer - Staff level' 'Housewife'\n",
      " 'Small Business (small shop owner, grocery store, etc.)'\n",
      " 'Lecturer / Teacher' 'Broker/ Service provider with no employee'\n",
      " 'Retirement' 'Pupil / Student'\n",
      " 'Professional (doctor, engineer, architect, nursing staff, lawyer, researcher, etc.)'\n",
      " 'Military / Police' 'Freelance'\n",
      " 'Agriculture / Forestry (Fishing, planting, farming)'\n",
      " 'Civil servant - Staff level' 'Officer - Middle Management'\n",
      " 'Civil servant \\xa0- Middle Management'\n",
      " 'Business Owner with less than 10 employees' 'Unemployed'\n",
      " 'Artist (actor/actress, singer, painter, model)'\n",
      " 'Junior Manager / Executive' 'Civil servant \\xa0- Senior Management'\n",
      " 'Self Employed \\xa0- Company owner (under 10 employees)'\n",
      " 'Officer - Senior Management' 'Refuse' 'Job hunting' 'Other'\n",
      " 'Self Employed - Company owner (10 - 20 employees)']\n",
      "--------------------------------------------------\n"
     ]
    }
   ],
   "source": [
    "table = uniqueness_checker(survey_respondents_info_df)\n",
    "table.show_unique()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 14,
   "id": "9bf30ef6",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Count Frequencies of Each Occupation:\n",
      " occupation\n",
      "Unskilled Labor (worker, landry person, driver, security guard, cleaner)               1994\n",
      "Officer - Staff level                                                                  1994\n",
      "Housewife                                                                              1562\n",
      "Pupil / Student                                                                        1235\n",
      "Small Business (small shop owner, grocery store, etc.)                                 1143\n",
      "Skilled Labor (tailor, machinist, carpenter, electrician)                              1103\n",
      "Broker/ Service provider with no employee                                               505\n",
      "Semi-skilled labor (salesperson, waiter, photographer)                                  476\n",
      "Freelance                                                                               431\n",
      "Professional (doctor, engineer, architect, nursing staff, lawyer, researcher, etc.)     323\n",
      "Lecturer / Teacher                                                                      278\n",
      "Retirement                                                                              175\n",
      "Self Employed  - Company owner (under 10 employees)                                     153\n",
      "Civil servant - Staff level                                                              89\n",
      "Officer - Middle Management                                                              88\n",
      "Agriculture / Forestry (Fishing, planting, farming)                                      79\n",
      "Military / Police                                                                        28\n",
      "Unemployed                                                                               19\n",
      "Officer - Senior Management                                                              18\n",
      "Refuse                                                                                   17\n",
      "Artist (actor/actress, singer, painter, model)                                           14\n",
      "Civil servant  - Middle Management                                                       10\n",
      "Job hunting                                                                               9\n",
      "Civil servant  - Senior Management                                                        8\n",
      "Other                                                                                     4\n",
      "Junior Manager / Executive                                                                3\n",
      "Self Employed - Company owner (10 - 20 employees)                                         2\n",
      "Business Owner with less than 10 employees                                                1\n",
      "Name: count, dtype: int64\n"
     ]
    }
   ],
   "source": [
    "# Count frequencies of each occupation\n",
    "count_occupation = survey_respondents_info_df['occupation'].value_counts()\n",
    "print(f\"Count Frequencies of Each Occupation:\\n {count_occupation}\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 15,
   "id": "28ccef58",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Count Frequencies of Each Occupation:\n",
      " standardized_occupation\n",
      "Office Worker                  2103\n",
      "Unskilled Labor                1994\n",
      "Non-working                    1765\n",
      "SME Business Owner             1299\n",
      "Student                        1235\n",
      "Skilled Labor                  1103\n",
      "Artist, Broker & Freelancer     950\n",
      "Professional Roles              601\n",
      "Semi-skilled Labor              555\n",
      "Civil Servants                  135\n",
      "Refuse                           21\n",
      "Name: count, dtype: int64\n"
     ]
    }
   ],
   "source": [
    "# Stadardize job types\n",
    "job_mapping = {\n",
    "    'Unskilled Labor (worker, landry person, driver, security guard, cleaner)': 'Unskilled Labor',\n",
    "    'Skilled Labor (tailor, machinist, carpenter, electrician)': 'Skilled Labor',\n",
    "    'Semi-skilled labor (salesperson, waiter, photographer)': 'Semi-skilled Labor',\n",
    "    'Agriculture / Forestry (Fishing, planting, farming)': 'Semi-skilled Labor',\n",
    "    'Officer - Staff level': 'Office Worker',\n",
    "    'Junior Manager / Executive': 'Office Worker',\n",
    "    'Officer - Middle Management': 'Office Worker',\n",
    "    'Officer - Senior Management': 'Office Worker',\n",
    "    'Civil servant - Staff level': 'Civil Servants',\n",
    "    'Civil servant  - Middle Management': 'Civil Servants',\n",
    "    'Civil servant  - Senior Management': 'Civil Servants',\n",
    "    'Military / Police': 'Civil Servants',\n",
    "    'Lecturer / Teacher': 'Professional Roles',\n",
    "    'Professional (doctor, engineer, architect, nursing staff, lawyer, researcher, etc.)': 'Professional Roles',\n",
    "    'Small Business (small shop owner, grocery store, etc.)': 'SME Business Owner',\n",
    "    'Business Owner with less than 10 employees': 'SME Business Owner',\n",
    "    'Self Employed  - Company owner (under 10 employees)': 'SME Business Owner',\n",
    "    'Self Employed - Company owner (10 - 20 employees)': 'SME Business Owner',\n",
    "    'Broker/ Service provider with no employee': 'Artist, Broker & Freelancer',\n",
    "    'Artist (actor/actress, singer, painter, model)': 'Artist, Broker & Freelancer',\n",
    "    'Freelance': 'Artist, Broker & Freelancer',\n",
    "    'Housewife': 'Non-working',\n",
    "    'Retirement': 'Non-working',\n",
    "    'Pupil / Student': 'Student',\n",
    "    'Unemployed': 'Non-working',\n",
    "    'Job hunting': 'Non-working',\n",
    "    'Refuse': 'Refuse',\n",
    "    'Other': 'Refuse'\n",
    "}\n",
    "\n",
    "survey_respondents_info_df['standardized_occupation'] = survey_respondents_info_df['occupation'].map(job_mapping)\n",
    "\n",
    "count_standardized_occupation = survey_respondents_info_df['standardized_occupation'].value_counts()\n",
    "print(f\"Count Frequencies of Each Occupation:\\n {count_standardized_occupation}\")\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "2fed2dc4",
   "metadata": {},
   "source": [
    "#### Check missing values"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 16,
   "id": "5ff05f61",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Summary Table of Missing Values:\n",
      "                          Number of Missing  Percentage of Missing\n",
      "Column                                                           \n",
      "bumo_previous                         5665                  48.17\n",
      "mpi_mean                              3717                  31.60\n",
      "mpi                                   3717                  31.60\n",
      "mpi_detail                            3685                  31.33\n",
      "group_size                              15                   0.13\n",
      "age_group                                9                   0.08\n",
      "age                                      9                   0.08\n",
      "age_group_2                              9                   0.08\n",
      "id                                       0                   0.00\n",
      "most_favourite                           0                   0.00\n",
      "bumo                                     0                   0.00\n",
      "tom                                      0                   0.00\n",
      "year                                     0                   0.00\n",
      "gender                                   0                   0.00\n",
      "city                                     0                   0.00\n",
      "occupation                               0                   0.00\n",
      "standardized_occupation                  0                   0.00\n"
     ]
    }
   ],
   "source": [
    "# Missingness summary\n",
    "miss_summary = (\n",
    "    survey_respondents_info_df.isna()\n",
    "      .sum()\n",
    "      .rename(\"Number of Missing\")\n",
    "      .to_frame()\n",
    ")\n",
    "miss_summary[\"Percentage of Missing\"] = (miss_summary[\"Number of Missing\"] / len(survey_respondents_info_df) * 100).round(2)\n",
    "miss_summary = miss_summary.rename_axis(\"Column\")\n",
    "print(\"\\nSummary Table of Missing Values:\\n\", miss_summary.sort_values(\"Number of Missing\", ascending=False))\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 17,
   "id": "0695c866",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Unique values after standardization:\n",
      "['4.5m - 6.49m' '6.5m - 7.49m' '7.5m - 8.99m' None '3m - 4.49m' '<3m'\n",
      " 'Refuse' '9m - 11.99m' '12m - 14.99m' '20m - 24.99m' '15m - 19.99m'\n",
      " '25m - 29.99m' '30m - 44.99m' '45m - 74.99m' '75m - 149.99m']\n"
     ]
    }
   ],
   "source": [
    "# Standardize MPI detail\n",
    "replacement = {\"From \": \"\", \" to \": \" - \", \" millions\": \"m\", \" million\": \"m\", \" VND\": \"\", \"Under \": \"<\"}\n",
    "\n",
    "for old_words, new_words in replacement.items():\n",
    "    survey_respondents_info_df['mpi_detail'] = survey_respondents_info_df['mpi_detail'].str.replace(old_words, new_words)\n",
    "\n",
    "print(\"Unique values after standardization:\")\n",
    "print(survey_respondents_info_df['mpi_detail'].unique())\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 18,
   "id": "e133237a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Unique values after standardization:\n",
      "['4.5m - 8.9m' None '<4.5m' '9m - 14.9m' '15m - 24.9m' '>25m']\n"
     ]
    }
   ],
   "source": [
    "# Standardize MPI \n",
    "replacement = {\"VND \": \"\", \"25m+\": \">25m\", \"Under \": \"<\"}\n",
    "\n",
    "for old_words, new_words in replacement.items():\n",
    "    survey_respondents_info_df['mpi'] = survey_respondents_info_df['mpi'].str.replace(old_words, new_words)\n",
    "\n",
    "print(\"Unique values after standardization:\")\n",
    "print(survey_respondents_info_df['mpi'].unique())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 19,
   "id": "375e20cb",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "0\n",
      "0\n",
      "0\n"
     ]
    }
   ],
   "source": [
    "# Replace missing values in MPI with \"Refuse\"\n",
    "survey_respondents_info_df['mpi'] = survey_respondents_info_df['mpi'].fillna('Refuse')\n",
    "survey_respondents_info_df['mpi_detail'] = survey_respondents_info_df['mpi_detail'].fillna('Refuse')\n",
    "survey_respondents_info_df['mpi_mean'] = survey_respondents_info_df['mpi_mean'].fillna('Refuse')\n",
    "\n",
    "# Count missing values after imputation\n",
    "print(survey_respondents_info_df['mpi'].isnull().sum())\n",
    "print(survey_respondents_info_df['mpi_detail'].isnull().sum())\n",
    "print(survey_respondents_info_df['mpi_mean'].isnull().sum())\n"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "b9730592",
   "metadata": {},
   "source": [
    "#### Impute age columns missising values\n",
    "\n",
    "**Ideas:**\n",
    "\n",
    "- People's age would be associated with certain demographics factors such as MPI, Occupation and Living City. The missing values of 'age' will be imputed by the median of respondents belonging to these categories.\n",
    "\n",
    "- Step 1: Impute age column with median based on specified demographics.\n",
    "\n",
    "- Step 2: Categorize the age to age_sub_group and age_group.\n",
    "\n",
    "    - Bins and group labels are divided based on the original group labels.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 20,
   "id": "1f6e960b",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Missing values before imputation: 9\n",
      "Missing values after imputation: 0\n"
     ]
    }
   ],
   "source": [
    "# Step 1: Impute age column with median\n",
    "\n",
    "# Check median of age by different demographics\n",
    "demographics = ['mpi_detail', 'occupation', 'city']\n",
    "median_age_by_demo = (\n",
    "    survey_respondents_info_df\n",
    "    .groupby(demographics)['age']\n",
    "    .median()\n",
    "    .reset_index()\n",
    "    .rename(columns={'age': 'median_age'})\n",
    ")\n",
    "\n",
    "group_median_dict = median_age_by_demo.set_index(demographics)['median_age'].to_dict()\n",
    "\n",
    "# Define function to impute age\n",
    "def impute_age(row):\n",
    "    if pd.isna(row['age']):\n",
    "        key = (row['mpi_detail'], row['occupation'], row['city'])\n",
    "        return group_median_dict.get(key, np.nan)\n",
    "    else:\n",
    "        return row['age']\n",
    "    \n",
    "# Apply the function to impute age\n",
    "survey_respondents_info_df['age_imputed'] = survey_respondents_info_df.apply(impute_age, axis=1)\n",
    "\n",
    "# Check number of missing values before and after imputation\n",
    "print(f\"Missing values before imputation: {survey_respondents_info_df['age'].isna().sum()}\")\n",
    "print(f\"Missing values after imputation: {survey_respondents_info_df['age_imputed'].isna().sum()}\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 21,
   "id": "5d65aec4",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Step 2: Categorize the age to age_sub_group and age_group\n",
    "\n",
    "# Create bins and labels for age_group and age_sub_group\n",
    "age_group_bins = [15, 19, 29, 39, np.inf]\n",
    "age_group_labels = ['16 - 19', '20 - 29', '30 - 39', '40 - 60']\n",
    "age_sub_group_bins = [15, 19, 24, 29, 34, 39, 44, np.inf] \n",
    "age_sub_group_labels = ['16 - 19', '20 - 24', '25 - 29', '30 - 34', '35 - 39', '40 - 44', '45+']\n",
    "\n",
    "# Categorize age_imputed into age_sub_group and age_group\n",
    "survey_respondents_info_df['age_group_new'] = pd.cut(survey_respondents_info_df['age_imputed'], bins=age_group_bins, labels=age_group_labels, right=True, include_lowest=False)\n",
    "survey_respondents_info_df['age_sub_group'] = pd.cut(survey_respondents_info_df['age_imputed'], bins=age_sub_group_bins, labels=age_sub_group_labels, right=True, include_lowest=False)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 22,
   "id": "5ddcabf8",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "       id  gender     city  year  group_size      tom     bumo bumo_previous  \\\n",
      "0  348226  Female  Cần Thơ  2018         3.0  Other 1  Other 1          None   \n",
      "1  358485    Male   Hà Nội  2018         3.0  Other 1  Other 1          None   \n",
      "2  360729  Female  Cần Thơ  2018         3.0  Other 1  Other 1          None   \n",
      "3  360737    Male  Cần Thơ  2018         3.0  Other 1  Other 1          None   \n",
      "4  361753    Male  Cần Thơ  2018         3.0  Other 1  Other 1          None   \n",
      "\n",
      "  most_favourite   age age_group age_sub_group          mpi    mpi_detail  \\\n",
      "0        Other 1  29.0   20 - 29       25 - 29  4.5m - 8.9m  4.5m - 6.49m   \n",
      "1        Other 1  25.0   20 - 29       25 - 29  4.5m - 8.9m  4.5m - 6.49m   \n",
      "2        Other 1  25.0   20 - 29       25 - 29  4.5m - 8.9m  4.5m - 6.49m   \n",
      "3        Other 1  24.0   20 - 29       20 - 24  4.5m - 8.9m  4.5m - 6.49m   \n",
      "4        Other 1  26.0   20 - 29       25 - 29  4.5m - 8.9m  4.5m - 6.49m   \n",
      "\n",
      "  mpi_mean                                         occupation  \\\n",
      "0   5499.0  Unskilled Labor (worker, landry person, driver...   \n",
      "1   5499.0  Unskilled Labor (worker, landry person, driver...   \n",
      "2   5499.0  Unskilled Labor (worker, landry person, driver...   \n",
      "3   5499.0  Skilled Labor (tailor, machinist, carpenter, e...   \n",
      "4   5499.0  Semi-skilled labor (salesperson, waiter, photo...   \n",
      "\n",
      "     occupation_group  \n",
      "0     Unskilled Labor  \n",
      "1     Unskilled Labor  \n",
      "2     Unskilled Labor  \n",
      "3       Skilled Labor  \n",
      "4  Semi-skilled Labor  \n"
     ]
    }
   ],
   "source": [
    "# Reorder columns\n",
    "# Remove redundant columns\n",
    "redundant_columns = ['age_group', 'age_group_2', 'age']\n",
    "survey_respondents_info_df = survey_respondents_info_df.drop(columns=redundant_columns, errors='ignore')\n",
    "\n",
    "# Change column names\n",
    "survey_respondents_info_df = survey_respondents_info_df.rename(columns={'age_group_new': 'age_group', 'age_imputed': 'age', 'standardized_occupation': 'occupation_group'})\n",
    "\n",
    "# Define the desired order of columns\n",
    "first_columns = ['id', 'gender', 'city', 'year', 'group_size', 'tom','bumo','bumo_previous', 'most_favourite']\n",
    "\n",
    "other_columns = [column for column in survey_respondents_info_df.columns if column not in first_columns]\n",
    "\n",
    "\n",
    "# Sort other columns alphabetically\n",
    "sorted_other_columns = sorted(other_columns)\n",
    "\n",
    "# Combine the lists to get the new column order\n",
    "new_column_order = first_columns + sorted_other_columns\n",
    "\n",
    "# Reorder the DataFrame columns\n",
    "survey_respondents_info_df = survey_respondents_info_df[new_column_order]\n",
    "\n",
    "print(survey_respondents_info_df.head())"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "01988c2f",
   "metadata": {},
   "source": [
    "### Brand Image Table"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "id": "bf1391e0",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "       id  year    city         awareness      attribute       brand_image\n",
      "0  725118  2019  Hà Nội  Highlands Coffee  Popular brand  Highlands Coffee\n",
      "1  725466  2019  Hà Nội  Highlands Coffee  Popular brand  Highlands Coffee\n",
      "2  726561  2019  Hà Nội  Highlands Coffee  Popular brand  Highlands Coffee\n",
      "3  726862  2019  Hà Nội  Highlands Coffee  Popular brand  Highlands Coffee\n",
      "4  727219  2019  Hà Nội  Highlands Coffee  Popular brand  Highlands Coffee\n",
      "--------------------------------------------------\n",
      "                  id           year\n",
      "count  643072.000000  643072.000000\n",
      "mean   486938.048413    2018.112589\n",
      "std    272601.961288       0.792818\n",
      "min     89100.000000    2017.000000\n",
      "25%    140350.000000    2017.000000\n",
      "50%    444693.000000    2018.000000\n",
      "75%    795589.000000    2019.000000\n",
      "max    863754.000000    2019.000000\n",
      "--------------------------------------------------\n",
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 643072 entries, 0 to 643071\n",
      "Data columns (total 6 columns):\n",
      " #   Column       Non-Null Count   Dtype \n",
      "---  ------       --------------   ----- \n",
      " 0   id           643072 non-null  int64 \n",
      " 1   year         643072 non-null  int64 \n",
      " 2   city         643072 non-null  object\n",
      " 3   awareness    642675 non-null  object\n",
      " 4   attribute    643072 non-null  object\n",
      " 5   brand_image  643072 non-null  object\n",
      "dtypes: int64(2), object(4)\n",
      "memory usage: 29.4+ MB\n",
      "None\n"
     ]
    }
   ],
   "source": [
    "# Describe data\n",
    "print(brand_image_df.head())\n",
    "print(\"-\" * 50)\n",
    "print(brand_image_df.describe())\n",
    "print(\"-\" * 50)\n",
    "print(brand_image_df.info())"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "c202c914",
   "metadata": {},
   "source": [
    "#### Check missing value\n",
    "\n",
    "- Only 'awareness' column is reported to have missing values\n",
    "- Assumption: the "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 24,
   "id": "2e64d7b6",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n",
      "Summary Table of Missing Values:\n",
      "              Number of Missing  Percentage of Missing\n",
      "Column                                               \n",
      "awareness                  397                   0.06\n",
      "id                           0                   0.00\n",
      "year                         0                   0.00\n",
      "city                         0                   0.00\n",
      "attribute                    0                   0.00\n",
      "brand_image                  0                   0.00\n"
     ]
    }
   ],
   "source": [
    "# Missingness summary\n",
    "miss_summary = (\n",
    "    brand_image_df.isna()\n",
    "      .sum()\n",
    "      .rename(\"Number of Missing\")\n",
    "      .to_frame()\n",
    ")\n",
    "miss_summary[\"Percentage of Missing\"] = (miss_summary[\"Number of Missing\"] / len(brand_image_df) * 100).round(2)\n",
    "miss_summary = miss_summary.rename_axis(\"Column\")\n",
    "print(\"\\nSummary Table of Missing Values:\\n\", miss_summary.sort_values(\"Number of Missing\", ascending=False))\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 25,
   "id": "5a9926ed",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Total number of rows: 643072\n",
      "Total number of non-missing rows: 642675\n",
      "Total number of missing rows: 397\n",
      "Total number of missing: 397\n"
     ]
    }
   ],
   "source": [
    "# Count how many rows having 'awareness' is similar with 'brand_image'\n",
    "missing_rows = brand_image_df[brand_image_df['awareness'].isna()]\n",
    "not_missing_rows = brand_image_df[brand_image_df['awareness'].notna()]\n",
    "\n",
    "print(f\"Total number of rows: {len(brand_image_df)}\")\n",
    "print(f\"Total number of non-missing rows: {len(not_missing_rows)}\")\n",
    "print(f\"Total number of missing rows: {len(missing_rows)}\")\n",
    "\n",
    "# Check if values in 'awareness' are similar to 'brand_image'\n",
    "print(f\"Total number of missing: {len(brand_image_df) - len(not_missing_rows)}\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 28,
   "id": "b5641e5b",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Drop 'awareness' column since it is similar to 'brand_image'\n",
    "brand_image_df = brand_image_df.drop(columns=['awareness'], errors='ignore')"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "05f11a73",
   "metadata": {},
   "source": [
    "### Brand Health Table"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 31,
   "id": "3e588f91",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "       id  year         city      brand spontaneous  awareness trial   p3m  \\\n",
      "0  345853  2018  Hồ Chí Minh  Phúc Long        None  Phúc Long  None  None   \n",
      "1  348403  2018  Hồ Chí Minh  Phúc Long        None  Phúc Long  None  None   \n",
      "2  349552  2018  Hồ Chí Minh  Phúc Long        None  Phúc Long  None  None   \n",
      "3  349764  2018  Hồ Chí Minh  Phúc Long        None  Phúc Long  None  None   \n",
      "4  350072  2018  Hồ Chí Minh  Phúc Long        None  Phúc Long  None  None   \n",
      "\n",
      "    p1m comprehension brand_likability weekly daily  fre_visit  ppa  spending  \\\n",
      "0  None          None             None   None  None        NaN  NaN       NaN   \n",
      "1  None          None             None   None  None        NaN  NaN       NaN   \n",
      "2  None          None             None   None  None        NaN  NaN       NaN   \n",
      "3  None          None             None   None  None        NaN  NaN       NaN   \n",
      "4  None          None             None   None  None        NaN  NaN       NaN   \n",
      "\n",
      "  segmentation  nps_p3m nps_p3m_group  spending_use  \n",
      "0         None      NaN          None           NaN  \n",
      "1         None      NaN          None           NaN  \n",
      "2         None      NaN          None           NaN  \n",
      "3         None      NaN          None           NaN  \n",
      "4         None      NaN          None           NaN  \n",
      "--------------------------------------------------\n",
      "                  id          year     fre_visit           ppa      spending  \\\n",
      "count   74419.000000  74419.000000  19332.000000  14073.000000  14073.000000   \n",
      "mean   478277.867856   2018.091160      7.289468     29.824835    155.014709   \n",
      "std    268141.831294      0.781323      9.222700     19.074486    173.986365   \n",
      "min     89100.000000   2017.000000      1.000000      5.000000      7.000000   \n",
      "25%    140354.000000   2017.000000      2.000000     15.000000     50.000000   \n",
      "50%    443720.000000   2018.000000      4.000000     25.000000    100.000000   \n",
      "75%    791013.000000   2019.000000      8.000000     40.000000    200.000000   \n",
      "max    863754.000000   2019.000000    120.000000    500.000000   3750.000000   \n",
      "\n",
      "           nps_p3m  spending_use  \n",
      "count  21605.00000  14073.000000  \n",
      "mean       7.96723    155.014709  \n",
      "std        1.35239    173.986365  \n",
      "min        0.00000      7.000000  \n",
      "25%        7.00000     50.000000  \n",
      "50%        8.00000    100.000000  \n",
      "75%        9.00000    200.000000  \n",
      "max       10.00000   3750.000000  \n",
      "--------------------------------------------------\n",
      "<class 'pandas.core.frame.DataFrame'>\n",
      "RangeIndex: 74419 entries, 0 to 74418\n",
      "Data columns (total 20 columns):\n",
      " #   Column            Non-Null Count  Dtype  \n",
      "---  ------            --------------  -----  \n",
      " 0   id                74419 non-null  int64  \n",
      " 1   year              74419 non-null  int64  \n",
      " 2   city              74419 non-null  object \n",
      " 3   brand             74419 non-null  object \n",
      " 4   spontaneous       30993 non-null  object \n",
      " 5   awareness         74305 non-null  object \n",
      " 6   trial             47330 non-null  object \n",
      " 7   p3m               28849 non-null  object \n",
      " 8   p1m               19399 non-null  object \n",
      " 9   comprehension     26346 non-null  object \n",
      " 10  brand_likability  10331 non-null  object \n",
      " 11  weekly            13382 non-null  object \n",
      " 12  daily             7621 non-null   object \n",
      " 13  fre_visit         19332 non-null  float64\n",
      " 14  ppa               14073 non-null  float64\n",
      " 15  spending          14073 non-null  float64\n",
      " 16  segmentation      14073 non-null  object \n",
      " 17  nps_p3m           21605 non-null  float64\n",
      " 18  nps_p3m_group     21605 non-null  object \n",
      " 19  spending_use      14073 non-null  float64\n",
      "dtypes: float64(5), int64(2), object(13)\n",
      "memory usage: 11.4+ MB\n",
      "None\n"
     ]
    }
   ],
   "source": [
    "# Describe data\n",
    "print(brand_health_df.head())\n",
    "print(\"-\" * 50)\n",
    "print(brand_health_df.describe())\n",
    "print(\"-\" * 50)\n",
    "print(brand_health_df.info())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "6aa110b0",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "ba492ba7",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Total number of non-missing rows: 30993\n",
      "Number of non-missing rows with 'spontaneous' similar to 'brand': 27626\n",
      "           id  year         city    brand spontaneous awareness trial   p3m  \\\n",
      "35801  733977  2019       Hà Nội  Other 2       Other     Other  None  None   \n",
      "35805  737986  2019    Nha Trang  Other 1       Other     Other  None  None   \n",
      "35823  790855  2019  Hồ Chí Minh  Other 2       Other     Other  None  None   \n",
      "35843  806276  2019      Cần Thơ  Other 1       Other     Other  None  None   \n",
      "35874  821645  2019    Hải Phòng  Other 2       Other     Other  None  None   \n",
      "\n",
      "        p1m  comprehension brand_likability weekly daily  fre_visit  ppa  \\\n",
      "35801  None  Know a little            Other   None  None        NaN  NaN   \n",
      "35805  None  Know a little            Other   None  None        NaN  NaN   \n",
      "35823  None  Know a little            Other   None  None        NaN  NaN   \n",
      "35843  None  Know a little            Other   None  None        NaN  NaN   \n",
      "35874  None  Know a little            Other   None  None        NaN  NaN   \n",
      "\n",
      "       spending segmentation  nps_p3m nps_p3m_group  spending_use  \n",
      "35801       NaN         None      NaN          None           NaN  \n",
      "35805       NaN         None      NaN          None           NaN  \n",
      "35823       NaN         None      NaN          None           NaN  \n",
      "35843       NaN         None      NaN          None           NaN  \n",
      "35874       NaN         None      NaN          None           NaN  \n"
     ]
    }
   ],
   "source": [
    "# Check if 'spontaneous', 'awareness', 'trial' are simialar to 'brand'\n",
    "not_missing_rows = brand_health_df[brand_health_df['spontaneous'].notna()]\n",
    "\n",
    "similar_rows = not_missing_rows[not_missing_rows['spontaneous'] == not_missing_rows['brand']]\n",
    "\n",
    "print(f\"Total number of non-missing rows: {len(not_missing_rows)}\")\n",
    "\n",
    "if len(similar_rows) == len(not_missing_rows):\n",
    "    print(\"All non-missing rows have 'spontaneous' similar to 'brand'.\")\n",
    "else:\n",
    "    unmatched_rows = not_missing_rows[not_missing_rows['spontaneous'] != not_missing_rows['brand']]\n",
    "    print(f\"Number of non-missing rows with 'spontaneous' similar to 'brand': {len(similar_rows)}\")\n",
    "    print(unmatched_rows.head())\n",
    "\n",
    "\n",
    "\n",
    "\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "17ba5dbb",
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "markdown",
   "id": "afda52f0",
   "metadata": {},
   "source": [
    "### Segmentation 2017"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "id": "09383116",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Unique values in column 'id':\n",
      "[ 92316  96307 105678 ... 139183 139313 141432]\n",
      "\n",
      "\n",
      "Unique values in column 'segmentation':\n",
      "['Seg.02 - Mass Asp (VND 25K - VND 59K)' 'Seg.01 - Mass (<VND 25K)'\n",
      " 'Seg.04 - Super Premium (VND 100K+)'\n",
      " 'Seg.03 - Premium (VND 60K - VND 99K)']\n",
      "\n",
      "\n",
      "Unique values in column 'visit':\n",
      "[  4   2   1   8   3  10  16  20   5   6  12   9  40  15  13  14  30   7\n",
      "  25  11  28  18  22  17  60  24  26  27  19  90  45  29  23  38  31  50\n",
      "  21  35  89 120  80 111  55  56 100  32  75  33  88]\n",
      "\n",
      "\n",
      "Unique values in column 'spending':\n",
      "[ 120   80  100  200   60  400   96  140  320   40   48  144  220   68\n",
      "  160  180   72   64   32   28   24  240  280  340  196  236   52  128\n",
      "  112   76  116  300  172  276   36   44   88  156   56  260   20   16\n",
      "  130   90   70   14  110   18   78   58   26   46   98  118  190   12\n",
      "   30   50  480  600  960 1200  360  720  540 2400  900  780  270   75\n",
      " 1120 2100 1080  800  195  390  640  560  490  500 1350   65 1000  650\n",
      "  210  700 2500 1500  255 1280  770  350  840 1600   84   39  440  315\n",
      "  376  330   55  275  450  135   29  312  405  117  145  108   87  165\n",
      "  132  225  177   45  168   59  760  660  245  147   49  420  980  495\n",
      "  290  294  162  297  392  288  216 1770  234  384 1160  590  159  990\n",
      "  295 1540   35  105  385 1050  175  630  875  520  880  680   25  250\n",
      "  125  150  625  750  375  850 1250  950 1800  192  264  228  348  204\n",
      "  336  324  456  372  224    8  208  184    7   63   21  189  217   42\n",
      "  154  182  161  126  623  221  230   17   15   54  207  476  170    6\n",
      "  252    9  352  532 1443  935  325  176  342  104   66   69  504  550\n",
      "  448   27  338   57  198   13   33  286  136   99  345  525  435   10\n",
      "  380]\n",
      "\n",
      "\n",
      "Unique values in column 'brand':\n",
      "['Indepentdent' 'Chain' 'Street']\n",
      "\n",
      "\n",
      "Unique values in column 'ppa':\n",
      "[ 30  20  25  50  15 100  24  35  80  10  12  36  55  17  40  45  18  16\n",
      "   8   7   6  60  70  85  49  59  13  32  28  19  29  75  43  69   9  11\n",
      "  22  39  14  65  56  23  48  95  90  47  26  27  44  38  42  33  58  53\n",
      "   5  21]\n",
      "\n",
      "\n"
     ]
    }
   ],
   "source": [
    "# Check unique values in each column of segmentation_2017_df\n",
    "for column_name in segmentation_2017_df.columns:\n",
    "    print(f\"Unique values in column '{column_name}':\")\n",
    "    print(segmentation_2017_df[column_name].unique())\n",
    "    print(\"\\n\")"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "highlands-py311",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.13"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
