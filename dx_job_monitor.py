#!/bin/python3

""" dx_job_monitor
Finds failed jobs in 002 projects and sends messages to alert the team
"""

from collections import defaultdict, Counter
import os
import dxpy as dx
import requests
import sys

from helper import get_logger

log = get_logger("main log")


def check_dx_login():
    try:
        dx.api.system_whoami()

    except Exception as e:
        log.error('Error with dxpy token')
        log.error(e)

        message = (
            "dx-job-monitoring: Error with dxpy token! Error code: \n"
            f"`{e}`"
            )
        post_message_to_slack('egg-alerts', message)

        log.info('Programme will stop. Alert message sent!')
        sys.exit()


def post_message_to_slack(channel, message):
    """
    Request function for slack web api

    Returns:
        dict: slack api response
    """

    log.info(f'Sending POST request to channel: #{channel}')

    try:
        response = requests.post('https://slack.com/api/chat.postMessage', {
            'token': os.environ['SLACK_TOKEN'],
            'channel': f'#{channel}',
            'text': message
        }).json()

        if response['ok']:
            log.info(f'POST request to channel #{channel} successful')
            return
        else:
            # slack api request failed
            error_code = response['error']
            log.error(f'Slack API error to #{channel}')
            log.error(f'Error Code From Slack: {error_code}')

    except Exception as e:
        # endpoint request fail from server
        log.error(f'Error sending POST request to channel #{channel}')
        log.error(e)


def get_002_projects():
    """
    Return list of 002 projects

    Returns:
        list: List of project ids
    """

    project_objects = []

    projects = dx.find_projects(name="002_*", name_mode="glob")

    for project in projects:
        project_objects.append(dx.DXProject(project["id"]))

    return project_objects


def get_jobs_per_project(projects):
    """
    Return dict of project2state2jobs

    Args:
        projects (list): List of project ids

    Returns:
        dict: Dict of project to state to jobs
    """

    project2jobs = defaultdict(lambda: defaultdict(list))
    project_no_run = []

    for project in projects:
        project_id = project.describe()["id"]
        project_name = project.describe()["name"]

        log.info(f'Get job per {project_name} started')
        jobs = list(dx.find_jobs(project=project_id, created_after="-24h"))

        if jobs:
            for job in jobs:
                job = dx.DXJob(job["id"])
                job_name = job.describe()["name"]
                job_state = job.describe()["state"]
                project2jobs[
                    (project_name, project_id)][job_state].append(job_name)
        else:
            project_no_run.append(project_name)

    return project2jobs, project_no_run


def send_msg_using_hermes(project2jobs, project_no_run):
    """
    Sends msg using Hermes

    Args:
        project2jobs (dict): Dict of project to failed jobs
    """

    log.info('Send message function started')
    project_no_pb = []

    for project, project_id in project2jobs:
        states = project2jobs[(project, project_id)]

        if "failed" in states:
            for state in states:
                count = Counter(project2jobs[(project, project_id)][state])
                ls = [f'- {v} {k}' for k, v in count.items()]

                jobs = "\n".join(ls)
                id = project_id.split('-')[1]

                if state == "failed":
                    message = (
                        f':x: The following jobs failed in'
                        f' {project} with project ID: {project_id}.\n\n'
                        f'{jobs}'
                        '\n\nLink: '
                        f'https://platform.dnanexus.com/projects/{id}/'
                        'monitor?state.values=failed'
                    )
                    post_message_to_slack('egg-alerts', message)

        else:
            project_no_pb.append(project)

    # No pb projects
    if project_no_pb:
        job_run_but_no_pb_projects = ", ".join(project_no_pb)
        message = (
            ":heavy_check_mark: Jobs have been run in the last 24h and "
            "none have failed for: "
            f"{job_run_but_no_pb_projects}"
        )
        post_message_to_slack('egg-logs', message)

    # Nothing run in the last 24h
    if project_no_run:
        nb_projects_no_jobs = len(project_no_run)
        message = (
            ":heavy_check_mark: No jobs have been ran in the last 24h "
            f"for {nb_projects_no_jobs} projects"
        )
        post_message_to_slack('egg-logs', message)


def main():

    dnanexus_token = os.environ['DNANEXUS_TOKEN']

    # env variable for dx authentication
    dx_security_context = {
            "auth_token_type": "Bearer",
            "auth_token": dnanexus_token
        }

    # set token to env
    dx.set_security_context(dx_security_context)
    check_dx_login()

    projects = get_002_projects()
    project2jobs, project_no_run = get_jobs_per_project(projects)
    send_msg_using_hermes(project2jobs, project_no_run)


if __name__ == "__main__":
    main()
