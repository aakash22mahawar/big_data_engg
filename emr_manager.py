import boto3
import time
import sys
import configparser


config = configparser.ConfigParser()
config.read("config.ini")


AWS_ACCESS_KEY_ID = config["MY_AWS"]["ACCESS_KEY"]
AWS_SECRET_ACCESS_KEY = config["MY_AWS"]["SECRET_KEY"]
AWS_REGION = config["MY_AWS"]["REGION"]


emr = boto3.client(
    "emr",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


CLUSTER_NAME = "big-data-engg-emr"

LOG_URI = "s3://aws-logs-732108543790-ap-southeast-2/elasticmapreduce/"


RELEASE_LABEL = "emr-7.13.0"

EC2_KEY_NAME = "aakash-emr-key"

SUBNET_ID = "subnet-04eae1aac75d44e42"

MASTER_SECURITY_GROUP = "sg-04e7fbdc3e628c225"

SLAVE_SECURITY_GROUP = "sg-0faf713a63c3b1797"

JOB_FLOW_ROLE = "emr_big_data_ec2_role"

SERVICE_ROLE = "emr_big_data_role"


def create_cluster():

    response = emr.run_job_flow(

        Name=CLUSTER_NAME,

        LogUri=LOG_URI,

        ReleaseLabel=RELEASE_LABEL,

         Tags=[
            {
            'Key': 'for-use-with-amazon-emr-managed-policies',
            'Value': 'true'
            }
        ],


        Applications=[
            {'Name': 'Hadoop'},
            {'Name': 'Hive'},
            {'Name': 'Spark'}
        ],


        Instances={

            'InstanceGroups': [

                {
                    'Name': 'Primary node',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.large',
                    'InstanceCount': 1
                },

                {
                    'Name': 'Core node',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm4.large',
                    'InstanceCount': 1
                }

            ],

            'KeepJobFlowAliveWhenNoSteps': True,

            'TerminationProtected': False,

            'Ec2KeyName': EC2_KEY_NAME,

            'Ec2SubnetId': SUBNET_ID,

            'EmrManagedMasterSecurityGroup': MASTER_SECURITY_GROUP,

            'EmrManagedSlaveSecurityGroup': SLAVE_SECURITY_GROUP
        },


        JobFlowRole=JOB_FLOW_ROLE,

        ServiceRole=SERVICE_ROLE,

        VisibleToAllUsers=True
    )


    return response['JobFlowId']


def wait_for_cluster(cluster_id):

    print(
        f"Waiting for cluster {cluster_id} "
        f"to be in WAITING state..."
    )


    while True:

        response = emr.describe_cluster(
            ClusterId=cluster_id
        )


        state = response['Cluster']['Status']['State']


        print(f"Cluster state: {state}")


        if state == 'WAITING':

            print("Cluster is ready!")


            dns = response['Cluster'].get(
                'MasterPublicDnsName'
            )


            if dns:

                print(
                    f"Primary node DNS: {dns}"
                )


            break


        elif state in (
            'TERMINATING',
            'TERMINATED',
            'TERMINATED_WITH_ERRORS'
        ):

            reason = (
                response['Cluster']['Status']
                ['StateChangeReason']['Message']
            )


            raise Exception(
                f"Cluster terminated early with state: "
                f"{state}. Reason: {reason}"
            )


        time.sleep(30)


def get_cluster_status(cluster_id):

    response = emr.describe_cluster(
        ClusterId=cluster_id
    )


    state = response['Cluster']['Status']['State']


    print(
        f"Cluster {cluster_id} state: {state}"
    )


def terminate_cluster(cluster_id):

    print(
        f"Terminating cluster {cluster_id}..."
    )


    emr.terminate_job_flows(
        JobFlowIds=[cluster_id]
    )


    print(
        "Termination request submitted."
    )


if __name__ == "__main__":


    if len(sys.argv) < 2:

        print("Please provide an action.")

        print("")

        print("Start cluster:")

        print(
            "python emr_manager.py start"
        )

        print("")

        print("Check status:")

        print(
            "python emr_manager.py status <cluster_id>"
        )

        print("")

        print("Stop cluster:")

        print(
            "python emr_manager.py stop <cluster_id>"
        )

        sys.exit(1)


    action = sys.argv[1].lower()


    if action == "start":

        cluster_id = create_cluster()


        print(
            f"Created cluster {cluster_id}..."
        )


        wait_for_cluster(cluster_id)


        print("")

        print("====================================")

        print(
            "EMR cluster is ready for learning."
        )

        print(
            f"Cluster ID: {cluster_id}"
        )

        print("====================================")


    elif action == "status":


        if len(sys.argv) < 3:

            print(
                "Please provide cluster ID."
            )

            print("Example:")

            print(
                "python emr_manager.py "
                "status j-XXXXXXXXXXXXX"
            )

            sys.exit(1)


        cluster_id = sys.argv[2]


        get_cluster_status(cluster_id)


    elif action == "stop":


        if len(sys.argv) < 3:

            print(
                "Please provide cluster ID."
            )

            print("Example:")

            print(
                "python emr_manager.py "
                "stop j-XXXXXXXXXXXXX"
            )

            sys.exit(1)


        cluster_id = sys.argv[2]


        terminate_cluster(cluster_id)


    else:

        print("Invalid action.")

        print("Use: start, status or stop")