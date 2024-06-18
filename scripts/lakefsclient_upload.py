import argparse
import os

from avalon.mainoperations import put_files
from avalon.operations.LakeFsWrapper import LakeFsWrapper
from lakefs_sdk import Configuration


def get_lfs(lakefs_host, lakefs_password, lakefs_username, local_temp_path):
    c = Configuration(host=lakefs_host,
                      password=lakefs_password,
                      username=lakefs_username)
    c.temp_folder_path = local_temp_path
    lfs = LakeFsWrapper(configuration=c)
    return lfs


def put_files_to_lakefs(lfs, local_path: str, repo_name: str, remote_path: str, branch: str) -> None:

    try:
        put_files(lake_fs_client=lfs,
                  local_path=local_path,
                  remote_path=remote_path,
                  repo=repo_name,
                  branch=branch,
                  pipeline_id="dug-data-ingest",
                  task_docker_image="none",
                  task_args=[],
                  s3storage=False,
                  commit_id="none",
                  task_name="bdc-ingest")
    except Exception as e:
        raise Exception("Error uploading files to lakefs: {}".format(e))

def main():
    parser = argparse.ArgumentParser(description='Util that uploads directory to lakefs')
    parser.add_argument('-l', '--localpath', help='<Required> Local path', required=True)
    parser.add_argument('-r', '--remotepath', help='<Required> Remote path', required=True)
    parser.add_argument('-e', '--repository', help='<Required> Repository', required=True)
    parser.add_argument('-b', '--branch', help='<Required> Branch', required=True)

    args = parser.parse_args()

    host = os.environ.get('LAKEFS_HOST')
    username = os.environ.get('LAKEFS_USERNAME')
    password = os.environ.get('LAKEFS_PASSWORD')
    temp_path = os.environ.get('LAKEFS_TEMPPATH')
    lfs = get_lfs(lakefs_host=host,
                  lakefs_username=username,
                  lakefs_password=password,
                  local_temp_path=temp_path)
    put_files_to_lakefs(lfs, local_path=args.localpath,
                        remote_path=args.remotepath,
                        branch=args.branch,
                        repo_name=args.repository)


if __name__ == '__main__':
    main()
