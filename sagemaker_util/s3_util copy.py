import re
import threading
import os
import boto3
from boto3.s3.transfer import TransferConfig


class Error(Exception):
    """Base class for exceptions in this module."""
    pass

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

def _extract_immediate_prefix(obj_key:str)->str:
    """
    extracts immediate prefix from source object
    """
    immed_prefix = ""
    if len(obj_key.split("/")) > 1:
        immed_prefix = obj_key.split("/")[-2]
    
    return immed_prefix

def _trim_path(path):
    """
    trims "/" at the end of path
    """
    if path.endswith("/"):
        path = path[:-1] # remove / at the end
    
    return path
    
def _get_dest_obj_name(immed_prefix:str, obj:str)->str:
    """
    returns dest object name by trimming everything before immed_prefix in obj str
    """
    if immed_prefix == "":
        dest_obj_name = obj
    else:
        dest_obj_name = obj.split("{}/".format(immed_prefix))[-1]
        # dest_obj_name = "{}/{}".format(immed_prefix, obj.split("{}/".format(immed_prefix))[-1])
    
    return dest_obj_name

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


def _multithread(thread_func, iter):
    """
    executes given function spawning multiple threads
    :parm thread_func function to call for each thread
    :parm iter iterator of objects that thread works on
    :return status True/False
    """
    t = []
    # creating a lock
    lock = threading.Lock()
    # spawn threads
    for i in range(8):
        t[i] = threading.Tread(target=thread_func, args=(lock,))
    # start threads
    for i in range(8):
        t[i].start()
    # wait for threads to complete
    for i in range(8):
        t[i].join()


def _copy_s3_file_to_s3(src: str, dest: str, is_move=False)->bool:
    """
    copies/moves s3 file to s3
    """
    debug_str = "moving" if (is_move) else "copying"
    src_bucket, src_key = _extract_bucket_key(src)
    dest_bucket, dest_key = _extract_bucket_key(dest)
    s3_resource = boto3.resource('s3')
    print(f"{debug_str} file {src} to {dest}")
    copy_source = {
        'Bucket': src_bucket,
        'Key': src_key
        }
    bucket = s3_resource.Bucket(dest_bucket)
    try:
        bucket.copy(copy_source, dest_key)
        if is_move:
            aws_s3_rm(src)
    except Exception as exc:
        raise Error("Error {} occurred while {} objects.".format(exc, debug_str))
    
    return True


def _copy_s3_folder_to_s3(src:str, dest:str, is_move=False)->bool:
    """
    copies/moves s3 folder to s3
    """
    debug_str = "move" if (is_move) else "copy"
    src_bucket, src_key = _extract_bucket_key(src)
    dest = _trim_path(dest)
    src_last_prefix = _extract_immediate_prefix(src_key)
    # list objects
    objects = aws_s3_ls(src)
    for obj in objects:
        dest_obj_name = _get_dest_obj_name(src_last_prefix, obj)
        status = _copy_s3_file_to_s3(
            "s3://{}/{}".format(src_bucket, obj), 
            "{}/{}".format(dest, dest_obj_name),
            is_move
        )
        if not status:
            raise Error(f"S3 {debug_str} failed.")
    return True

def _copy_s3_file_to_local(src:str, dest:str, is_move=False)->bool:
    """
    copies s3 file to local
    """
    debug_str = "moving" if (is_move) else "copying"
    s3 = boto3.resource('s3')
    src_bucket, src_key = _extract_bucket_key(src)
    if dest.endswith("/"):
        file_name = src_key.split("/")[-1]
        dest = "{}{}".format(dest, file_name) # if dest is folder append file name to it
        _create_local_dir(dest) # create dir if doesn't exist
    print("{} file {} to {}".format(debug_str, src, dest))
    try:
        s3.Bucket(src_bucket).download_file(src_key, dest)
        if is_move:
            aws_s3_rm(src)
    except Exception as exc:
        raise Error("Error {} occurred while {} objects.".format(exc, debug_str))
    
    return True

def _copy_s3_folder_to_local(src:str, dest:str, is_move=False)->bool:
    """
    copies s3 folder to local 
    """
    debug_str = "move" if (is_move) else "copy"
    src_bucket, src_key = _extract_bucket_key(src)
    dest = _trim_path(dest)
    src_last_prefix = _extract_immediate_prefix(src_key)
    # list objects
    objects = aws_s3_ls(src)
    for obj in objects:
        dest_obj_name = _get_dest_obj_name(src_last_prefix, obj)
        #src_prefix
        status = _copy_s3_file_to_local(
            "s3://{}/{}".format(src_bucket, obj),
            "{}/{}".format(dest, dest_obj_name),
            is_move
        )
        if not status:
            raise Error(f"S3 {debug_str} failed.")
    return True

def _copy_local_folder_to_s3(src:str, dest:str, is_move=False)->bool:
    """
    copies local folder to s3
    """
    debug_str = "move" if (is_move) else "copy"
    src_last_prefix = os.path.basename(src)
    dest = _trim_path(dest)
    # list objects
    local_files = _list_dir(src)
    for file in local_files:
        dest_obj_name = _get_dest_obj_name(src_last_prefix, file)
        #src_prefix
        status = _copy_local_file_to_s3(
            file,
            "{}/{}".format(dest, dest_obj_name),
            is_move
        )
        if not status:
            raise Error(f"S3 {debug_str} failed.")
    
    return True

def _copy_local_file_to_s3(src:str, dest:str, is_move=False)->bool:
    """
    copies local file to s3
    """
    debug_str = "moving" if (is_move) else "copying"
    s3_client = boto3.client('s3')
    dest_bucket, dest_key = _extract_bucket_key(dest)
    
    if dest.endswith("/"):
        src_file_name = os.path.basename(src)
        dest_key = "{}{}".format(dest_key, src_file_name) # if dest is folder append file name to it
    print("{} file {} to {}".format(debug_str, src, dest))
    try:
        response = s3_client.upload_file(src, dest_bucket, dest_key)
        if is_move:
            os.remove(src)
    except Exception as exc:
        raise Error("Error {} occurred while {} objects.".format(exc, debug_str))
    
    return True

def aws_s3_cp(src:str, dest:str)->bool:
    """
    Copies a local file or S3 object to another location locally or in S3

    :param src: source s3 object/prefix, local file/directory
    :param dest: dest s3 object/prefix, local file/directory
    :return True if copy is successful else False
    """
    if src.startswith("s3://") and dest.startswith("s3://"):
        if src.endswith("/"): #copying entire folder
            status = _copy_s3_folder_to_s3(src, dest)
        else:
            status = _copy_s3_file_to_s3(src, dest)
        if not status:
            raise Error("S3 copy failed.")
    elif src.startswith("s3://"):
        if src.endswith("/"): #copying entire folder
            status = _copy_s3_folder_to_local(src, dest)
        else:
            status = _copy_s3_file_to_local(src, dest)
        if not status:
            raise Error("S3 copy failed.")
    elif dest.startswith("s3://"):
        if os.path.isdir(src): #copying entire folder
            status = _copy_local_folder_to_s3(os.path.abspath(src), dest)
        else:
            status = _copy_local_file_to_s3(os.path.abspath(src), dest)
        if not status:
            raise Error("S3 copy failed.")
    else:
        raise Error("None of the src/dest is an s3 filesystem. Use local file utils to copy.")
    
    return status

def aws_s3_mv(src:str, dest:str)->bool:
    """
    Moves a local file or S3 object to another location locally or in S3

    :param src: source s3 object/prefix, local file/directory
    :param dest: dest s3 object/prefix, local file/directory
    :return True if copy is successful else False
    """
    if src.startswith("s3://") and dest.startswith("s3://"):
        if src.endswith("/"): #moving entire folder
            status = _copy_s3_folder_to_s3(src, dest, True)
        else:
            status = _copy_s3_file_to_s3(src, dest, True)
        if not status:
            raise Error("S3 move failed.")
    elif src.startswith("s3://"):
        if src.endswith("/"): #moving entire folder
            status = _copy_s3_folder_to_local(src, dest, True)
        else:
            status = _copy_s3_file_to_local(src, dest, True)
        if not status:
            raise Error("S3 move failed.")
    elif dest.startswith("s3://"):
        if os.path.isdir(src): #moving entire folder
            status = _copy_local_folder_to_s3(os.path.abspath(src), dest, True)
        else:
            status = _copy_local_file_to_s3(os.path.abspath(src), dest, True)
        if not status:
            raise Error("S3 move failed.")
    else:
        raise Error("None of the src/dest is an s3 filesystem. Use local file utils to copy.")
    
    return status

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