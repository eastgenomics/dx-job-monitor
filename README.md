## What does this app do?

Checks for jobs run in any 002 project in the last 24hr and sends notifications to Slack.  
If any jobs have failed in a project, a message will be sent to `egg-alerts`, and those projects where all jobs completed successfully a message will be sent to `egg-logs`.

## What are typical use cases for this app?

Daily check to see if any run issues are present.

## What data are required for this app to run?

A config file (txt) containing three env variables: `DNANEXUS_TOKEN`, `SLACK_TOKEN` and `PROM_PATH`. 

To be able to run on the server, another two variables are needed `HTTP_PROXY` and `HTTPS_PROXY`

## Logging

The main logging script is `helper.py`

The script will generate a log file `dx-job-monitor.log` in `/var/log/monitoring`

## Docker

A Dockerfile is included to recreate the docker image.

To run the file, current command on server: 

```docker run --env-file <config.txt> -v /var/log/monitoring:/var/log/monitoring:z <image name>```

## Automation

A cron job has been set up to run the script daily at 9am

#### This was made by EMEE GLH
