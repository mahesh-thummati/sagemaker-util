# pylint: disable=trailing-whitespace
import json
import boto3


def _describe_notebook_instance(resource_name):
    """
    describes notebook instance
    """
    client = boto3.client("sagemaker")
    try:
        response = client.describe_notebook_instance(
            NotebookInstanceName = resource_name
        )
        return response
    except Exception as exc:
        raise "Exception {} occurred while describing notebook".format(exc)

def _list_tags(resource_arn):
    """
    list sagemaker notebook tags
    """
    client = boto3.client("sagemaker")
    try:
        response = client.list_tags(
            ResourceArn = resource_arn
        )
        return response
    except Exception as error:
        raise "Exception {} occurred while listing tags of the notebook".format(error)

def _get_s3_bucket():
    """
    get sagemaker notebook in the account and region
    """
    region = boto3.session.Session().region_name
    sts_connection = boto3.client("sts",
        region_name = region,
        endpoint_url = "https://sts.{}.amazonaws.com".format(region)
    )
    account = sts_connection.get_caller_identity().get("Account")
    return "{}-{}-sagemaker-work-area".format(account, region)

def _get_principal_tag_value(nb_tags):
    """
    get principal tag value
    """
    for key_value in nb_tags:
        if key_value["Key"] == "mufg:principal":
            return key_value["Value"]
    return ""

class SageMakerUtil:
    """SageMaker Util class"""

    def __init__(self):
        with open("/opt/ml/metadata/resource-metadata.json") as f:
            resource_meta = json.load(f)
        nb_attr = _describe_notebook_instance(resource_meta["ResourceName"])
        nb_tags = _list_tags(resource_meta["ResourceArn"])
        self._props = {}
        self._props["subnet_ids"] = self._subnet_ids = [nb_attr["SubnetId"]]
        self._props["kms_key"] = self._kms_key = nb_attr["KmsKeyId"]
        self._props["role"] = self._role = nb_attr["RoleArn"].replace("NotebookExecution", 
                                                                      "Training")
        self._props["security_group"] = self._security_group = nb_attr["SecurityGroups"]
        self._props["s3_bucket"] = self._s3_bucket = _get_s3_bucket()
        self._props["s3_prefix"] = self._s3_prefix = _get_principal_tag_value(nb_tags["Tags"])
        self._props["principal_tag_value"] = self._principal_tag_value = self._s3_prefix
    
    def __str__(self):
        return str(self._props)
    
    # read only properties
    @property
    def subnet_ids(self):
        return self._subnet_ids
    
    @property
    def kms_key(self):
        return self._kms_key
    
    @property
    def role(self):
        return self._role
    
    @property
    def security_group(self):
        return self._security_group
    
    @property
    def s3_bucket(self):
        return self._s3_bucket
    
    @property
    def s3_prefix(self):
        return self._s3_prefix
    
    @property
    def principal_tag_value(self):
        return self._principal_tag_value

    @property
    def props(self):
        return self._props
