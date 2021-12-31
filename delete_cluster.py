import boto3
import configparser
import logging

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


def delete_cluster():
    """
    Deletes Redshift cluster for project.
    """

    try:
        redshift = boto3.client('redshift',
                                config['REGION']['AWS_REGION']
        )

        redshift.delete_cluster(
            ClusterIdentifier=config['CLUSTER']['CLUSTER_ID'], 
            SkipFinalClusterSnapshot=True
        )

    except Exception as e:
        logging.error(e)

if __name__ == '__main__':
    delete_cluster()