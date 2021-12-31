import boto3
from botocore.exceptions import ClientError
import logging
import configparser
import pandas as pd


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

def create_cluster() -> None:
    """
    Creates Redshift cluster in region 'us-east-1'.
    If cluster exists, error printed to terminal.
    """

    redshift = boto3.client('redshift', region_name=config['REGION']['AWS_REGION'])
    try:
        redshift.create_cluster(
            ClusterType='multi-node',
            NodeType='dc2.large',
            NumberOfNodes=2,
            DBName=config['CLUSTER']['DB_NAME'],
            ClusterIdentifier=config['CLUSTER']['CLUSTER_ID'],
            MasterUsername=config['CLUSTER']['DB_USER'],
            MasterUserPassword=config['CLUSTER']['DB_PASSWORD'],
            IamRoles=[config['IAM_ROLE']['ARN']]
        )

    except ClientError as e:
        logging.error(e)


def get_cluster_properties(cluster_id) -> dict:
    """
    Returns cluster properties of Redshift cluster based on id.
    """
    redshift = boto3.client('redshift', region_name=config['REGION']['AWS_REGION'])
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=cluster_id)['Clusters'][0]
        return cluster_props

    except Exception as e:
        logging.error(e)
        return {}


def enable_cluster_ingress(cluster_properties: dict) -> None:
    """
    Modifies default VPC security group inbound rules.
    """

    ec2 = boto3.resource('ec2', region_name=config['REGION']['AWS_REGION'])

    try:
        vpc = ec2.Vpc(id=cluster_properties['VpcId'])
        print(list(vpc.security_groups.all()))
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        
        defaultSg.authorize_ingress(
            CidrIp=config['SECURITY_GROUP']['INBOUND_IP'],  # TODO: fill out
            IpProtocol=config['SECURITY_GROUP']['IP_PROTOCOL'],  # TODO: fill out
            FromPort=int(config['CLUSTER']['DB_PORT']),
            ToPort=int(config['CLUSTER']['DB_PORT'])
        )

    except Exception as e:
        logging.error(e)


def main() -> None:
    """
    Main function for creating AWS cluster and enabling inbound traffic.

    - Create cluster, log error if creation fails
    - Get proprties of new cluster, log error if fails
    - Use VPC ID to enable inbound traffic for default security group,
    log error if fails
    """


    create_cluster()
    cluster_props = get_cluster_properties(config['CLUSTER']['CLUSTER_ID'])
    enable_cluster_ingress(cluster_props)


if __name__ == '__main__':
    main()
