"""Python command-line script for retrieving a set of data files
from an online data repository, combining them, and analyzing the
combined result.

This version finds the data files by querying the API for a CKAN instance.
 
The base URL for the API to use (without the trailing "/api/action" text)
can be specified in an environment variable named 'CKAN_URL' or provided as
a command line argument.

The unique identifier for a dataset to retrieve is a required command line
argument. All of the XLSX files in the specified dataset are retrieved.

No key is used for authentication, so the API will only find data files that are
openly shared for public use.

A command line argument can set the size of the buffer to use for
retrieving data files.
"""
import argparse
import json
import logging
import os
import requests
import sys
import urllib3

import ckanapi
import pandas
import numpy

def nan2none(val):
    if type(val) == int:
        return val
    if type(val) == float:
        return val
    return None

def retrieve_data(url):
    try:
        dataframe = pandas.read_excel(url,header=8,
            converters={1:nan2none,2:nan2none,3:nan2none})
        # The first 8 rows contain descriptive information about
        # the data values. Most importantly, the value in the
        # second row, second column is the school year for which
        # the rest of the data applies. Add that school year
        # to every row so the combined dataset has the school
        # year for reference.
        school_year = dataframe.iloc[1].iloc[1]
        dataframe['School year'] = school_year
        # Get the current list of column names.
        current_columns = dataframe.columns
        print(current_columns)
        # Build a dictionary for explicitly renaming the columns
        # to consistent values.
        cols = {current_columns[0]:'State',current_columns[1]:'Number LEAs',
            current_columns[2]:'Number children who received CEIS',
            current_columns[3]:'Number children who received CEIS and special education services'}
        dataframe.rename(columns=cols)
        print(dataframe.head())
        # Now return everything but the first 8 rows.
        return dataframe[8:]
    except Exception as e:
        logging.error(f'Error retrieving data file at {url}', exc_info=e)
        return None

def get_data_file_list(connection, id):
    try:
        # Construct the ditionary parameter for the API call that identifies the
        # "package" to retrieve.
        dd = {'id': id}
        json_result = connection.call_action(action='package_show', data_dict=dd)
        #logging.info(f'package_show response:\n{json.dumps(json_result, indent=2)}')
        # The JSON results object should contain a list of dictionaries with a
        # key of "resources". Those dictionaries identify the data files. Return
        # that list.
        return json_result.get('resources',None)
    except ckanapi.errors.NotFound:
        logging.error(f'No dataset with identifier {id} found.')
        return None

if __name__ == '__main__':

    logging.basicConfig(level=os.environ.get("LOGLEVEL",logging.ERROR))
    
    ap = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
        description='Retrieve, combine, and analyze data files in a CKAN instance.',
        epilog='''The program uses the following environment variables:
  CKAN_URL: The base URL for the API to use (without the trailing "/api/action" text).
 ''')

    ap.add_argument('identifier', help='The unique identifier for a dataset to use.')
    ap.add_argument('-u','--url',dest='url',default=os.getenv('CKAN_URL', None),
        help='URL for the CKAN API.')
    args = ap.parse_args()

    # Prompt for the API connection details if missing.
    if not args.url:
        args.url = input('Enter CKAN URL:')

    remote = ckanapi.RemoteCKAN(args.url)

    data_files = get_data_file_list(remote,args.identifier)

    # Create an empty dataframe to use in combining the data file contents.
    cdf = pandas.DataFrame()
    for data_file in data_files:
        url = data_file.get('url',None)
        if url is not None:
            # Only attempt to retrieve the data file if it is in Excel format.
            if url[-4:] == 'xlsx':
                df = retrieve_data(url)
                if df is not None:
                    # Append the newly-retrieved dataframe into the combined dataframe.
                    cdf = pandas.concat([cdf,df],ignore_index=True)
    print(cdf.head())
