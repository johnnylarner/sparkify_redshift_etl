import configparser
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import logging

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

def json_bucket_to_dataframe(bucket_name='udacity-dend', prefix='') -> pd.DataFrame:

    """
    Reads json files stored in a bucket and returns them
    as single dataframe.
    """

    # Get s3 buckets
    s3_resource = boto3.resource('s3',
                                region_name='us-west-2'    
                            )
    udacity_bucket = s3_resource.Bucket(bucket_name)


    # Files for iteration
    found_files = udacity_bucket.objects.filter(Prefix=prefix)

    # For teminal output
    read_files = 0

    # Read all JSON files and store them as DFs
    read_dfs = []
    for obj in found_files:

        try:
            # Get bucket attributes and create file path string
            buck_name = obj.bucket_name     
            key = obj.key
            file_location = f's3://{buck_name}/{key}'
            
            # Read jsons and add to list if contains data
            df = pd.read_json(file_location, lines=True)
            if not df.empty:
                read_dfs.append(df)

        except (Exception, ClientError) as e:
            logging.error(e)

        read_files += 1
        print(f'{read_files} files processed.')

    # If JSON files read, concat to one df, else return empty df
    if read_dfs:
        output_df = pd.concat(read_dfs)

    else:
        output_df = pd.DataFrame()
        
    # Output to terminal
    num_rows = output_df.shape[0]
    print(f'{num_rows} rows of JSON data found.')

    return output_df

def write_df_to_s3(df: pd.DataFrame,
                    bucket: str='log_data' or 'song_data') -> bool:
    """
    Writes Dataframe as CSV to S3 bucket. CSV can then
    be moved to staging tables from bucket.

    Returns True if file successfully writes.

    Args:
    df: pandas dataframe containing log or song data
    bucket: str containing s3 url & file name
    """

    bucket_endpoints = {
        'log_data' : config['S3']['LOG_DATA_OUT'],
        'song_data' : config['S3']['SONG_DATA_OUT'],
    }

    try:
        bucket_ep = bucket_endpoints[bucket]

    except KeyError as e:
        logging.error(
            ValueError(f'"{bucket}" is not a udacity bucket name')
        )
        return False

    try:
        df.to_csv(bucket_ep, sep="|", na_rep='null', 
                    quotechar="'", escapechar="|", 
                    header=True, index=False,
                    encoding='utf-8', chunksize=1000,
                    compression='infer'
                    )
        return True
    except Exception as e:
        logging.error(e)
        return False

def collect_udacity_data(bucket: str='log_data' or 'song_data'):
    """
    Wrapper function for collecting Udacity data from S3 and
    storing them as compressed CSVs on user's S3
    """

    bucket_as_df = json_bucket_to_dataframe(
        bucket_name='udacity-dend',
        prefix=bucket
        )

    response = write_df_to_s3(bucket_as_df, bucket=bucket)

    if response:
        print(f'Bucket "{bucket}" was successfully saved in S3.')


def main():

    buckets = {'log_data', 'song_data'}

    for b in buckets:
        collect_udacity_data(b)

if __name__ == '__main__':
    main()