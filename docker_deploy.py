from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from ingest_data import main_flow

docker_block = DockerContainer.load("docker-container")

deploy = Deployment(name="docker-flow")
deployment_parameters = {
    "output_dir": "",
    "taxi_color": "",
    "year": "",
    "month": "",
}

docker_deploy = deploy.build_from_flow(
    flow=main_flow,
    name="docker-flow",
    infrastructure=docker_block,
    parameters=deployment_parameters,
)


if __name__ == "__main__":
    docker_deploy.apply()
