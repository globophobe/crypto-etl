import os

from invoke import task

from cryptotick.constants import BIGQUERY_LOCATION
from cryptotick.utils import get_container_name, get_deploy_env_vars, set_environment

set_environment()


@task
def deploy_function(c, topic, memory=2048):
    entry_point = topic.replace("-", "_")
    region = os.environ[BIGQUERY_LOCATION]
    timeout = 540
    env_vars = get_deploy_env_vars()
    cmd = f"""
        gcloud functions deploy {topic} \
            --region={region} \
            --memory={memory}MB \
            --timeout={timeout}s \
            --runtime=python37 \
            --entry-point={entry_point} \
            --set-env-vars={env_vars} \
            --trigger-topic={topic}
    """
    c.run(cmd)


@task
def deploy_scheduler(
    c,
    topic,
    name=None,
    payload="{}",
    schedule="*/10 * * * *",
    timezone="Etc/UTC",
    memory=256,
    scheduler_only=False,
):
    if not scheduler_only:
        deploy_function(c, topic, memory=memory)
    cmd = f"""
        gcloud scheduler jobs create pubsub {name or topic} \
            --schedule='{schedule}' \
            --time-zone='{timezone}' \
            --topic={topic} \
            --message-body='{payload}'
    """
    c.run(cmd)


@task
def build_container(c, hostname="asia.gcr.io", image="cryptotick"):
    build_args = get_deploy_env_vars(pre="--build-arg ", sep=" ")
    name = get_container_name(hostname, image)
    # Build
    cmd = f"""
        docker build \
            {build_args} \
            --file=Dockerfile \
            --tag={name} .
    """
    c.run(cmd)


@task
def push_container(c, hostname="asia.gcr.io", image="cryptotick"):
    name = get_container_name(hostname, image)
    # Push
    cmd = f"docker push {name}"
    c.run(cmd)
