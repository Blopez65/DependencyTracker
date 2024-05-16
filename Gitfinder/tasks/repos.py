import json
import os
import shutil
import time
from typing import Any, Dict, List, Optional

import git
import requests
import utils
import yaml
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from utils import CACHE_PATH, GH_API_BASE, GH_HEADERS

logger = utils.logger


@flow(task_runner=ConcurrentTaskRunner)
def get_repo_import_report(org_name: str) -> dict[Any, Any]:
    """
    Gets the import report for a particular org.
    Args:
        org_name (str): The name of the GitHub organisation to get the report of.

    Returns:
        dict[Any, Any]: A dictionary where the key is the repo name and the value is the import.yml contents
    """
    # Get the list of repos that we need to process

    repos: List[dict[Any, Any]] = get_repos(org_name)
    logger.debug(
        f"[{org_name}] found {len(repos)} repositories in GitHub org to process"
    )

    # First we need to submit a task to clone all the repositories
    clone_futures = {}
    repo_cache = {
        item["name"]: {k: v for k, v in item.items() if k != "name"} for item in repos
    }
    for i, repo in enumerate(repos):
        if i >= 40:
            break
        repo_output_path: str = os.path.join(
            CACHE_PATH, "repos", org_name, repo["name"]
        )
        logger.debug(
            f"[{repo['name']}] sparse cloning repository to {repo_output_path}"
        )
        clone_future = sparse_clone_repo.submit(repo, repo_output_path)
        clone_futures[repo["name"]] = clone_future.result()
        time.sleep(2)  # Note: Sleep avoids database lockup

    # Initialize a set to keep track of repositories that are still being processed
    processing_repositories = set(clone_futures.keys())

    # Now, as cloning completes, we need to process the importfile
    import_futures = {}
    clone_errs = {}

    while processing_repositories:
        for repo_name in list(
            processing_repositories
        ):  # Iterate over a copy of the set
            repo_output_path: str = os.path.join(
                CACHE_PATH, "repos", org_name, repo_name
            )
            future = clone_futures[repo_name]
            status, msg = future
            if status == "SUCCEEDED":
                logger.debug(f"[{repo_name}] finished cloning to {repo_output_path}")
                logger.debug(f"[{repo_name}] reading import file")
                import_future = read_import_file.submit(repo_output_path)
                import_futures[repo_name] = import_future
                time.sleep(0.8)  # Note: Sleep avoids database lockup
            else:
                logger.error(f"[{repo_name}] Error cloning repository")
                clone_errs[repo_name] = {
                    "repo_name": repo_name,
                    "repo_data": repo_cache[repo_name],
                    "gh_org": org_name,
                    "status": "FAILED_CLONE",
                    "import_data": None,
                }

            # Remove the repository from the processing set
            processing_repositories.remove(repo_name)

        time.sleep(
            0.5
        )  # Add a small delay to prevent the loop from consuming too much CPU

    # Finally, we wait for all imports to complete
    import_data = {}
    import_errs = {}
    for repo_name, import_future in import_futures.items():
        status, result = import_future.result(30)
        if status == "SUCCEEDED" and result:
            logger.debug(
                f"[{repo_name}] repository has an import file and file was successfully read"
            )
            if utils.validate_import_data(result):
                import_data[repo_name] = {
                    "repo_name": repo_name,
                    "repo_data": repo_cache[repo_name],
                    "gh_org": org_name,
                    "status": "SUCCEEDED",
                    "import_data": result,
                }
            else:
                logger.error(
                    f"[{repo_name}] Import data in {repo_name} is in an invalid format - skipping"
                )
                import_errs[repo_name] = {
                    "repo_name": repo_name,
                    "repo_data": repo_cache[repo_name],
                    "gh_org": org_name,
                    "import_data": None,
                    "status": "FAILED_INVALID_IMPORTDATA",
                }
        else:
            import_errs[repo_name] = {
                "repo_name": repo_name,
                "repo_data": repo_cache[repo_name],
                "gh_org": org_name,
                "import_data": None,
                "status": status,
            }
            logger.warning(f"[{repo_name}] {result}")

    # Now let's generate the return data, a report of succeeded & failed for consumption
    return_data = {
        "successful": import_data,
        "failed_clone": clone_errs,
        "failed_import": import_errs,
    }
    return return_data


@task()
def get_repos(org_name: str) -> list[dict[Any, Any]]:
    """
    Gets the list of repositories for a particular GitHub organisation
    Args:
        org_name (str): The name of the GitHub organisation to grab the repos of.

    Returns:
        list[dict[Any, Any]]: A list of repositories (as dicts)
    """
    logger.debug(f"[{org_name}] getting list of repositories in org")
    repos: List[dict[Any, Any]] = []
    url: str = f"{GH_API_BASE}/orgs/{org_name}/repos?per_page=100"
    page: int = 1
    while url:
        logger.debug(f"[{org_name}] getting page {page} of repos...")
        response: requests.Response = requests.get(url, headers=GH_HEADERS)
        response.raise_for_status()
        repos.extend([x for x in response.json() if not x["archived"]])
        url = response.links.get("next", {}).get("url")
        page += 1
    return repos


@task(retries=3, retry_delay_seconds=5*60, log_prints=True)
def sparse_clone_repo(repo: Dict[Any, Any], output_path: str) -> None:
    """
    Will perform a sparse clone, which means we'll only checkout the import files that we need. This is important
    because we don't want to clone down the whole repo structure (which could be huge for monorepos). If there is no
    import file, we'll just be left with an empty directory
    Args:
        repo(Dict): The repo object
        output_path: The path that we'll output the repo to

    Returns:

    """
    try:
        logger.debug(
            f"Sparse cloning {repo['name']} using branch {repo['default_branch']} to {output_path}"
        )
        cloned_repo = git.Repo.clone_from(
            repo["ssh_url"], output_path, no_checkout=True, depth=1
        )
        cloned_repo.git.config("core.sparseCheckout", "true")
        with open(
            os.path.join(output_path, ".git", "info", "sparse-checkout"), "w"
        ) as sc_file:
            for path in [".snyk.d/import.yml", ".snyk.d/import.yaml"]:
                sc_file.write(path + "\n")
        cloned_repo.git.checkout(repo["default_branch"])
        return "SUCCEEDED", None
    except Exception as e:
        err_msg = f"Failed to clone {repo['name']} from {repo['ssh_url']} to {output_path} -  [err={str(e)}]"
        logger.error(err_msg)
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        return "FAILED_CLONE", err_msg

@task()
def read_import_file(input_path: str, max_attempts: int = 5) -> tuple[str, Any]:
    """
    Reads, from disk, a particular import file that we've cloned from a repo.

    Args:
        input_path: The input path (on disk) that we'll read from.
        max_attempts: Maximum number of attempts to try reading the file.

    Returns:
        tuple[str, Any]: A tuple where the first element is the status (e.g., 'SUCCEEDED' or 'FAILED'),
                         and the second element is the parsed contents or error message.
    """
    attempt_count = 0
    import_yaml_path: str = os.path.join(input_path, ".snyk.d/import.yaml")
    import_yml_path: str = os.path.join(input_path, ".snyk.d/import.yml")
    while attempt_count < max_attempts:
        file_to_read: Optional[str] = None
        if os.path.exists(import_yaml_path):
            file_to_read = import_yaml_path
        elif os.path.exists(import_yml_path):
            file_to_read = import_yml_path

        if file_to_read and os.path.exists(file_to_read):
            try:
                with open(file_to_read, "r") as f:
                    return "SUCCEEDED", yaml.safe_load(f)
            except Exception as e:
                return "FAILED_TO_READ", f"Failed to read import file: {str(e)}"
        else:
            # Wait for a bit before retrying
            time.sleep(2.5)
            attempt_count += 1

    return "FAILED_NO_IMPORT", "Import file was not found in repository after retries"