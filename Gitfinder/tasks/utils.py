from __future__ import annotations

import json
import os
import shutil
from typing import Any, Dict, List, Union
from uuid import UUID

import jsonschema
import loguru
import requests
from jsonschema import validate
from loguru import logger


####################################
# START: Centralised logging setup #
####################################
def setup(log_level: str, output: str) -> loguru.Logger:
    """
    Centralized Loguru logging configuration.
    Args:
        log_level: Logging level. Defaults to INFO.
        output: Determines where the logs should be directed.
                    Accepts "stdout", "file", or "both".

    Returns:
        Logger: The logger object
    """

    # Clear previous handlers
    logger.remove()

    log_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{"
        "name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level> "
    )

    if output in ["stdout", "both"]:
        import sys

        logger.add(
            sink=sys.stdout,
            level=log_level,
            format=log_format,
            enqueue=True,
            backtrace=True,
        )

    if output in ["file", "both"]:
        logger.add(
            sink=os.path.join(mapper_log_dest, "app.log"),
            rotation="10 MB",
            level=log_level,
            format=log_format,
            enqueue=True,
            backtrace=True,
        )

    # This allows for easy import and use throughout the application
    return logger


# By default, set up the logger to INFO level and both outputs
mapper_log_level: str = os.getenv("MAPPER_LOG_LEVEL", "INFO")
mapper_log_type: str = os.getenv("MAPPER_LOG_TYPE", "both")
mapper_log_dest: str = os.getenv("MAPPER_LOG_DEST", "logs/")
logger = setup(mapper_log_level, mapper_log_type)

##################################
# END: Centralised logging setup #
##################################


###########################
# START: Github API setup #
###########################
GH_API_BASE = os.environ.get("GITHUB_API_BASE", "https://api.github.com")
GH_PAT = os.environ["GITHUB_TOKEN"]
GH_HEADERS = {
    "Authorization": f"token {GH_PAT}",
    "Accept": "application/vnd.github+json",
}
CACHE_PATH: str = os.getenv("MAPPER_CACHE_PATH", ".mapper")


def empty_cache(dir_path=CACHE_PATH):
    """
    Will empty the cache (i.e. delete all files in the cache)
    Args:
        dir_path: the path to delete, defaults to the value of CACHE_PATH
    """
    if not os.path.exists(dir_path):
        logger.info(f"Cache path {dir_path} does not exist, won't empty cache...")
        return

    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        if os.path.isdir(item_path):
            shutil.rmtree(item_path)
        else:
            os.remove(item_path)
    logger.info(f"Cache path {dir_path} emptied...")


def get_github_orgs() -> list[Any]:
    """
    Retrieve a list of organizations the authenticated user has access to using the GitHub API.
    Returns:
        list[Any]: A list of organization names the authenticated user has access to.
    """
    url = f"{GH_API_BASE}/user/orgs"
    response = requests.get(url, headers=GH_HEADERS)
    if response.status_code == 200:
        orgs = response.json()
        org_names = [org["login"] for org in orgs]
        return org_names
    else:
        logger.error("Error getting list of orgs! ")
        return []


###########################
# END: Github API setup   #
###########################


###########################
# START: Snyk API helpers #
###########################
SNYK_V1_BASE_URL = "https://api.snyk.io/v1"
SNYK_REST_BASE_URL = "https://api.snyk.io"


def snyk_list_orgs(
    snyk_token: str, group_id: str, current_page: int = 1, current_list: List[Any] = []
) -> list[Any]:
    """
    Will recursively list Snyk orgs present in a given group
    Args:
        snyk_token(str): The Snyk token to use for the request
        group_id(str): The group ID that we'll list orgs under
        current_page(int): Used for recursion: keeps track of the page we're currently on
        current_list(List): Used for recursion: keeps track of the current list of orgs, we append to it on each recurse

    Returns:
        A list of Snyk organisations
    """
    response = requests.get(
        f"{SNYK_V1_BASE_URL}/group/{group_id}/orgs?perPage=100&page={current_page}",
        headers={"Authorization": f"token {snyk_token}"},
    )
    response_body = response.json()
    orgs_in_page = response_body.get("orgs", [])
    if len(orgs_in_page) > 0:
        logger.debug(f"Found {len(orgs_in_page)} Snyk orgs on page {current_page}...")
        current_list.extend(orgs_in_page)
        return snyk_list_orgs(snyk_token, group_id, (current_page + 1), current_list)
    else:
        logger.debug(
            f"Didn't find any Snyk orgs on page {current_page}, returning {len(current_list)} orgs..."
        )
        return current_list


def snyk_projects_in_org(
    snyk_token: str, org_id: str, next_url: str = None, projects: List[Any] = []
) -> list[Any]:
    """
    Will recursively get a list of Snyk projects in a specified org
    Args:
        snyk_token(str): The Snyk token to use for the request
        org_id(str): The group ID that we'll list orgs under
        next_url(str): Used for recursion: we set this if there's another page we need to request
        projects(List): Used for recursion: keeps track of the current list of projects, we append to it on each recurse

    Returns:

    """
    if next_url:
        url = next_url
    else:
        url = f"/rest/orgs/{org_id}/projects?version=2024-01-04&limit=100"

    response = requests.get(
        f"{SNYK_REST_BASE_URL}{url}",
        headers={
            "Authorization": f"token {snyk_token}",
            "Accept": "application/vnd.api+json",
        },
    )
    response_body = response.json()
    projects_response = response_body.get("data", [])
    logger.debug(f"[{url}] got {len(projects_response)} projects")
    projects.extend(projects_response)
    next_page_url = response_body.get("links", {}).get("next")
    if next_page_url:
        logger.debug("next page detected, recursing...")
        return snyk_projects_in_org(snyk_token, org_id, next_page_url, projects)
    return projects


def snyk_tag_project(
    snyk_token: str, org_id: str, project_id: str, key: str, value: str
) -> bool:
    """
    Tags a Snyk project
    Args:
        snyk_token(str): The Snyk token we'll use for the request
        org_id(str): The org where the project we'll tag resides
        project_id(str): The project ID that we'll tag
        key(str): The tag key
        value(str): The tag value

    Returns:
        True if the return status code is 200, otherwise False. We may return false due to API errors, or the tag may
        have already been added.
    """
    response = requests.post(
        f"{SNYK_V1_BASE_URL}/org/{org_id}/project/{project_id}/tags",
        headers={"Authorization": f"token {snyk_token}"},
        json={"key": key, "value": value},
    )
    return response.status_code == 200


###########################
# END: Snyk API helpers   #
###########################


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Union[str, Any]:
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


IMPORT_DATA_SCHEMA = {
    "type": "object",
    "properties": {
        "schema": {"type": "integer"},
        "orgName": {"type": "string", "pattern": "^[a-zA-Z0-9-]+$"},
        "integrationName": {"type": "string", "pattern": "^[a-zA-Z0-9-]+$"},
        "tags": {"type": "object", "additionalProperties": {"type": "string"}},
        "branches": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1
        },
    },
    "required": ["schema", "orgName", "integrationName"],
    "additionalProperties": False,
}




def validate_import_data(import_data: Dict) -> bool:
    """
    Will attempt to validate the import data against the defined import schema
    Args:
        import_data(Dict): The import data that we're trying to validate

    Returns:
        True if the import data matches the schema, False if it does not
    """
    try:
        validate(instance=import_data, schema=IMPORT_DATA_SCHEMA)
        return True
    except jsonschema.exceptions.ValidationError as err:
        return False


def update_nested_dicts(*dicts: Dict) -> bool:
    """
    Helper function which will merge a number of nested dictionaries in to a single flat dictionary
    Args:
        *dicts: Any number of Dicts

    Returns:
        A single flat Dict based on the contents of the input dictionaries
    """

    def merge_dictionaries(d1, d2):
        for key in d2:
            if key in d1 and isinstance(d1[key], dict) and isinstance(d2[key], dict):
                d1[key] = merge_dictionaries(d1[key], d2[key])
            else:
                d1[key] = d2[key]
        return d1

    # Start with an empty dictionary
    result_dict = {}

    # Iterate over each dictionary and merge it into the result_dict
    for d in dicts:
        result_dict = merge_dictionaries(result_dict, d)

    return result_dict