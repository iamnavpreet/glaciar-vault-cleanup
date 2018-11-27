import boto3
import logging
import argparse
import traceback
import json
import time
from botocore.exceptions import ClientError


def cli_args():
    """
    Setting up the cli arguments and help messages.
    :return: parsed arguments
    :rtype: Dict
    """

    parser = argparse.ArgumentParser(description="Glacier Vault cleanup script")
    parser.add_argument("-listvault", help="List All Glacier", action="store_true")
    parser.add_argument("-listarchives", help="List All Glacier", action="store_true")
    parser.add_argument("-vaultname", help="Provide Vault Name", action="store")
    parser.add_argument("-deleteall", help="Delete All items", action="store_true")
    parser.add_argument("-region", help="List All Glacier", required=True, action="store")

    return parser.parse_args()


def get_if_job_exists_for_vault(glacier_client, vault_name):
    response = glacier_client.list_jobs(vaultName=vault_name)
    for job in response['JobList']:
        if job['Action'] == 'InventoryRetrieval':
            logging.info('Found existing inventory retrieval job...')
            job_id = job['JobId']
            return job_id

    return None


def get_all_vaults(glacier_client):
    response = glacier_client.list_vaults()

    if response['VaultList']:
        logging.info("Found {} Vaults.".format(len(response['VaultList'])))
        for number,vault in enumerate(response['VaultList']):
            logging.info("Vault {} -> {}".format(number + 1, vault['VaultName']))
    else:
        logging.info("No Vault Found")


def validate_yes_no():
    while True:
        user_input = input("Enter Yes/No: ")

        if user_input:
            user_input = str(user_input)
            if user_input.lower() in ["yes", "y"]:
                return True
            elif user_input.lower() in ["no", "n"]:
                return False
            else:
                print("Enter yes/no please..")


def check_job_for_vault(glacier_client, vault):
    job_id = get_if_job_exists_for_vault(glacier_client, vault)
    if job_id:
        logging.info("We have Already Job for same Vault-> {}. ID ->".format(vault, job_id))
        job = glacier_client.describe_job(vaultName=vault, jobId=job_id)
        logging.info("Job status--> {}. Created on -> {}".format(job["StatusCode"], job["CreationDate"]))
        if job["StatusCode"] == "InProgress":
            logging.info("Job are usualy ready within 4hours of request with Amazon")
            logging.info('Inventory not ready, Kindly wait for 4 hrs ..')
            return job["StatusCode"]
        elif job["StatusCode"] == "Succeeded":
            logging.info("Job are usualy ready within 4hours of request with Amazon")
            logging.info('Inventory is ready.')
            return job["JobId"]
    else:
        logging.info("No Job found for the Vault provided")
        logging.info("DO you want to intiate Job?")
        if validate_yes_no():
            job = glacier_client.initiate_job(vaultName=vault,
                                              jobParameters={"Type": "inventory-retrieval"})

            logging.info("Intiated Job with ID -> {}".format(job["jobId"]))
            logging.info("Request you to check after 4 hrs for the same..")
            return "JobOngoing"
        else:
            logging.info("inventory retrieval cancelled...")
            return False


def delete_with_wait(glacier_client, vault, archive):
    count = 0
    while True:
        try:
            glacier_client.delete_archive(vaultName=vault, archiveId=archive)
            break
        except ClientError as e:
            if e.response["Error"]["Code"] == "RequestLimitExceeded":
                count += 1
                print("Oops.. Limits are there... Sleeping for {} seconds...".format(count * 2))
                time.sleep(count * 2)
            else:
                print(" There are issues in the deleting...")
                print(" Error Details - {}".format(e.response['ResponseMetadata']['HTTPStatusCode']))
                print(" Error Details - {}".format(e.response['Error']['Code']))
                break


def clean_archives(glacier_client, vault, archive_list):
    for archive in archive_list:
        logging.info("Deleting Archive - {} - from - Vault-> {}".format(archive, vault))
        delete_with_wait(glacier_client, vault, archive)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s - %(levelname)s : %(message)s', level=logging.INFO, datefmt='%H:%M:%S')
    args = cli_args()
    region = args.region
    logging.info("Connecting to Glacier.. in region {}".format(region))

    glacier_client = boto3.client("glacier")
    glacier_resource = boto3.resource("glacier")

    try:
        if args.listvault:
            logging.info("Listing All Glacier Vaults..")
            get_all_vaults(glacier_client)

        if args.listarchives and args.vaultname:
            vault = args.vaultname
            logging.info("Listing Archives for Vault {}".format(vault))
            status = check_job_for_vault(glacier_client, vault)
            if status:
                exit(0)
            else:
                exit(-1)

        if args.vaultname and args.deleteall:
            logging.info("Request came for deleting all the items in glacier..")
            logging.info("Checking Job for the current Vault")
            job_id = check_job_for_vault(glacier_client, vault)

            job_output = glacier_client.get_job_output(vaultName=vault, jobId=job_id)
            inventory = json.loads(job_output['body'].read().decode('utf-8'))
            archive_list = inventory['ArchiveList']

            clean_archives(glacier_client,vault,archive_list)

    except ClientError as e:
        logging.exception(e)
        logging.info("Issues in Script..")
        logging.info(traceback.format_exc())
