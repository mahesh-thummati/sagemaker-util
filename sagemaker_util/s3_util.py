import re
import threading
import os
import boto3
from boto3.s3.transfer import TransferConfig


class Error(Exception):
    """Base class for exceptions in this module."""
    pass

def _is_s3(path:str)->bool:
    """
    Determines if the path is an s3 path
    """
    return path.startswith("s3://")

def _trim_path(path):
    """
    trims "/" at the end of path
    """
    if path.endswith("/"):
        path = path[:-1] # remove / at the end
    
    return path

def _is_dir(path: str)->bool:
    """
    Determines if a path is a directory
    """
    if _is_s3(path):
        return path.endswith("/")
    else:
        return os.path.isdir(os.path.abspath(path))

def _append_object(base_path, obj)->str:
    """
    Appends object to basepath
    """
    base_path = _trim_path(base_path)
    return f"{base_path}/{obj}"

def _extract_bucket_key(s3_uri: str)->tuple:
    """
    This extracts bucket name and key given s3 uri. 
    :param s3_uri: s3 uri of form s3://bucket_name/prefix1/prefix2/file.ext
    :return bucket, key tuple
    """
    s3_regex="^s3://([a-z0-9.-]+)/(.*)$"
    search =re.search(s3_regex, s3_uri)
    if search is None:
        raise Error("Invalid s3 uri: {}".format(s3_uri))
    return search.groups()

def _extract_immediate_prefix(obj_key:str)->str:
    """
    Extracts immediate prefix from source object
    """
    immed_prefix = ""
    if len(obj_key.split("/")) > 1:
        immed_prefix = obj_key.split("/")[-2]
    
    return immed_prefix

def _get_dest_obj_name(initial_src, obj):
    """
    Determines destination object name based on initial source from use input and listed object passed
    """
    immed_prefix = ""
    if _is_s3(initial_src):
        immed_prefix = _extract_immediate_prefix(_extract_bucket_key(initial_src)[1])
    else:
        if os.path.isdir(os.path.abspath(initial_src)):
            immed_prefix = os.path.basename(os.path.abspath(initial_src))
        else:
            immed_prefix = _extract_immediate_prefix(initial_src)
    
    if immed_prefix == "":
        return obj
    else:
        return obj.split("{}/".format(immed_prefix))[-1]

def _list_dir(dir_name:str)->list:
    """
    Lists files in a dir and it's sub directories recurssively
    """
    files_and_dirs = os.listdir(dir_name)
    list_of_files = []
    for file in files_and_dirs:
        completePath = os.path.join(dir_name, file)
        if os.path.isdir(completePath):
            list_of_files = list_of_files + _list_dir(completePath)
        else:
            list_of_files.append(completePath)

    return list_of_files

def _create_local_dir(file_path:str)->bool:
    """
    creates local directory if does not exist
    """
    try:
        directory = os.path.dirname(file_path)
        if directory == "":
            return True # nothing to create
        if not os.path.exists(directory):
            os.makedirs(directory)
    except Exception as exc:
        raise Error("Error {} occurred while creating local directory".format(exc))
    
    return True

def aws_s3_ls(s3_uri: str, list_extended=False)->list:
    """
    list s3 objects in a bucket/prefix
    :param s3_uri: s3 path to list
    :param list_extended: to list extedned details - owner, size, last modified and object name
    :return list of s3 objects
    """
    client = boto3.client("s3")
    bucket, prefix = _extract_bucket_key(s3_uri)
    s3_objects = []
    cont_token = None
    while (True):
        if cont_token is None:
            kwargs = {
                "Bucket": bucket,
                "MaxKeys": 100,
                "Prefix": prefix
            }
        else:
            kwargs = {
                "Bucket": bucket,
                "MaxKeys": 100,
                "Prefix": prefix,
                "ContinuationToken": cont_token
            }   
        try:
            response = client.list_objects_v2(**kwargs)
            if response["KeyCount"] == 0:
                print ("Requested s3 object doesn't exist.")
                break
            for record in response["Contents"]:
                if record["Size"] > 0: # ignore just prefix names
                    if list_extended:
                        s3_objects.append((record["Size"], 
                                           record["LastModified"].strftime("%Y%m%d %H:%M:%S.%s"), 
                                           record["Key"]))
                    else:
                        s3_objects.append(record["Key"])
            if response["IsTruncated"]:
                cont_token = response["NextContinuationToken"]
            else:
                break
        except Exception as exc:
            raise Error("Error {} occurred while listing objects.".format(exc))
    return s3_objects

def _list_objects(src: str)->list:
    """
    List objects based on file system
    """
    if _is_s3(src):
        return aws_s3_ls(src)
    else:
        if _is_dir(src):
            return _list_dir(src)
        else:
            return [src]

def _copy_s3_to_s3(src_bucket: str, src_key: str, dest_bucket: str, dest_key: str)->bool:
    """
    copies an s3 file to another s3 bucket
    """
    s3_resource = boto3.resource('s3')
    copy_source = {
        'Bucket': src_bucket,
        'Key': src_key
        }
    bucket = s3_resource.Bucket(dest_bucket)
    try:
        bucket.copy(copy_source, dest_key)
    except Exception as exc:
        raise Error("Error {} occurred while working with s3 object to s3 object.".format(exc))
    
    return True

def _copy_s3_to_local(src_bucket: str, src_key: str, dest: str)->bool:
    """
    copies an s3 file to local
    """
    s3_resource = boto3.resource('s3')
    try:
        s3_resource.Bucket(src_bucket).download_file(src_key, dest)
    except Exception as exc:
        raise Error("Error {} occurred while working on s3 object to local.".format(exc))
    
    return True

def _copy_local_to_s3(src: str, dest_bucket: str, dest_key: str)->bool:
    """
    copies local file to s3
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(src, dest_bucket, dest_key)
    except Exception as exc:
        raise Error("Error {} occurred while working on local object to s3.".format(exc))
    
    return True

def _process_file_movement(src:str, dest:str, is_move=False)->bool:
    """
    copies/moves s3/local folder/file to s3/local
    """
    debug_str = "move" if (is_move) else "copy"
    
    objects = _list_objects(src) # list objects
    for obj in objects:
        if _is_dir(dest) or _is_dir(src):
            temp_dest = _append_object(dest, _get_dest_obj_name(src, obj))
        else:
            temp_dest = dest
        
        if _is_s3(src) and _is_s3(dest): #s3 to s3
            src_bucket, _ = _extract_bucket_key(src)
            dest_bucket, dest_key = _extract_bucket_key(temp_dest)
            print(f"{debug_str} file s3://{src_bucket}/{obj} to {temp_dest}")
            status = _copy_s3_to_s3(src_bucket, obj, dest_bucket, dest_key)
            if status and is_move:
                aws_s3_rm(f"s3://{src_bucket}/{obj}")
        elif _is_s3(src): # s3 to local
            src_bucket, _ = _extract_bucket_key(src)
            _create_local_dir(temp_dest) # create dir if doesn't exist
            print(f"{debug_str} file s3://{src_bucket}/{obj} to {temp_dest}")
            status = _copy_s3_to_local(src_bucket, obj, temp_dest)
            if status and is_move:
                aws_s3_rm(f"s3://{src_bucket}/{obj}")
        elif _is_s3(dest): # local to s3
            dest_bucket, dest_key = _extract_bucket_key(temp_dest)
            print(f"{debug_str} file {obj} to {temp_dest}")
            status = _copy_local_to_s3(obj, dest_bucket, dest_key)
            if status and is_move:
                os.remove(obj)       
        
        if not status:
            raise Error(f"S3 {debug_str} failed.")
    return True

def aws_s3_cp(src:str, dest:str)->bool:
    """
    Copies a local file or S3 object to another location locally or in S3

    :param src: source s3 object/prefix, local file/directory
    :param dest: dest s3 object/prefix, local file/directory
    :return True if copy is successful else False
    """
    if _is_s3(src) or _is_s3(dest):
        status = _process_file_movement(src, dest)
    else:
        raise Error("None of the src/dest is an s3 filesystem. Use local file utils to copy.")
    if not status:
            raise Error("S3 copy failed.")
    return True

def aws_s3_mv(src:str, dest:str)->bool:
    """
    Moves a local file or S3 object to another location locally or in S3

    :param src: source s3 object/prefix, local file/directory
    :param dest: dest s3 object/prefix, local file/directory
    :return True if copy is successful else False
    """
    if _is_s3(src) or _is_s3(dest):
        status = _process_file_movement(src, dest, True)
    else:
        raise Error("None of the src/dest is an s3 filesystem. Use local file utils to move.")
    if not status:
            raise Error("S3 move failed.")
    return True

def aws_s3_rm(s3_key: str)->bool:
    """
    Delete s3 object or multiple objects from an s3 bucket.
    :param s3_key: s3 key in the form "s3://bucket_name/file_name.txt", if the s3 path ends with "/" entire prefix will be deleted
    :return True if delete is successful else False
    """
    client = boto3.client("s3")
    #list objects
    objs_to_delete = aws_s3_ls(s3_key)
    if len(objs_to_delete) ==0: # no files to delete
        return True
    s3_bucket, _ = _extract_bucket_key(s3_key)
    #prepare s3 objects to delete
    objs_list=[]
    for obj in objs_to_delete:
        objs_list.append({"Key": obj})
    
    try:
        response = client.delete_objects(
            Bucket=s3_bucket,
            Delete={
                'Objects': objs_list,
                'Quiet': False
            }
        )
    except Exception as exc:
        raise Error("Cannot delete, exception {} occurred".format(exc))
    
    return True