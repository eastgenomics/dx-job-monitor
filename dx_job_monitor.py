#!/bin/python3

""" 
Finds failed jobs in 002 projects and sends messages to alert the team
Requires:
    - DNANEXUS_TOKEN
    - SLACK_TOKEN
"""

import collections
import os
import dxpy as dx
import requests

from helper import get_logger
from prometheus import PromClass

log = get_logger("main log")

DNANEXUS_URL = "https://platform.dnanexus.com/panx/projects/"


def _check_dx_login(token: str):
    dx.set_security_context({"auth_token_type": "Bearer", "auth_token": token})

    try:
        dx.api.system_whoami()

    except Exception as e:
        log.error(e)

        message = "dx-job-monitoring: Error with dxpy token! Error code: \n" f"`{e}`"
        _post_message_to_slack("#egg-alerts", message)

        raise Exception("dx-job-monitoring: Error with dxpy token!")


def _post_message_to_slack(
    channel: str,
    message: str,
) -> None:
    """
    Request function for slack web api

    Returns: None
    """

    log.info(f"Sending POST request to channel: #{channel}")

    try:
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            {
                "token": os.environ.get("SLACK_TOKEN"),
                "channel": channel,
                "text": message,
            },
        ).json()

        if response["ok"]:
            log.info(f"POST request to channel #{channel} successful")
            return
        else:
            # slack api request failed
            log.error(response["error"])

    except Exception as e:
        log.error(e)


def _get_projects(prefix: str) -> list:
    """
    Return list of <prefix> projects

    Returns:
        Iterable: List of projects
    """

    return list(
        dx.find_projects(name=f"{prefix}_*", name_mode="glob", describe={"name": True})
    )


def _get_jobs_in_project(project_id: str, created="-24h") -> list:
    """
    Return job description

    Args:
        job_id (str): Job id

    Returns:
        str: Job description
    """

    return list(
        dx.find_jobs(
            project=project_id,
            created_after=created,
            describe={"name": True, "state": True},
        )
    )


def get_jobs_per_project(projects: list):
    """
    Return dict of project2state2jobs

    Args:
        projects (list): List of project ids

    Returns:
        dict: Dict of project to state to jobs
    """

    project_id_to_name = {}
    project_id_with_no_failed_jobs = []
    data = collections.defaultdict(list)

    for project in projects:
        project_id = project["id"]
        project_name = project["describe"]["name"]

        jobs = _get_jobs_in_project(project_id)

        if jobs:
            project_id_to_name[project_id] = project_name

            log.info(f"Processing {len(jobs)} job(s) in project {project_id}")

            failed = False

            for job in jobs:
                name, state = job["describe"]["name"], job["describe"]["state"]

                if state.lower() == "failed":
                    data[project_id].append(name)
                    failed = True

            if not failed:  # there is no failed jobs in this project
                project_id_with_no_failed_jobs.append(
                    f"<{DNANEXUS_URL}{project_id.lstrip('project-')}|{project_name}>\n"
                )

    # No pb projects
    if project_id_with_no_failed_jobs:
        message = (
            "Jobs have been run in the last 24h and "
            "none have failed for:\n"
            "{}".format("\n".join(project_id_with_no_failed_jobs))
        )
        _post_message_to_slack("#egg-logs", message)

    return project_id_to_name, data


def send_message_to_slack(data: dict, project_id_to_name: dict) -> None:
    """
    Function to sort through data and send messages to Slack if job failed

    Args:
        project2jobs (dict): Dict of project to failed jobs
    """

    for project_id, names in data.items():
        count = collections.Counter(names)
        ls = "\n".join([f"- {v} {k}" for k, v in count.items()])

        message = (
            f":x: The following jobs failed in "
            f"<{DNANEXUS_URL}{project_id.lstrip('project-')}|{project_id_to_name[project_id]}>\n"
            f"{ls}"
            "\n\nLink: "
            f"{DNANEXUS_URL}{project_id.lstrip('project-')}/"
            "monitor?state.values=failed"
        )

        _post_message_to_slack(
            "#egg-alerts",
            message,
        )


def main():
    _check_dx_login(os.environ.get("DNANEXUS_TOKEN"))

    projects = _get_projects("002")
    project_id_to_name, data = get_jobs_per_project(projects)
    send_message_to_slack(data, project_id_to_name)

    # Nothing run in the last 24h
    _post_message_to_slack(
        "#egg-logs",
        ":information_source: dx-job-monitoring: daily check!",
    )

    # Tell Prometheus that the job has run
    prom = PromClass("dxjobmonitor_cron")
    prom.format_metrics
    prom.emit_metrics


if __name__ == "__main__":
    main()
