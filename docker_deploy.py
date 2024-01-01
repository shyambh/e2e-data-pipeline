from prefect.deployments import Deployment
from prefect.infrastructure.container import DockerContainer
from ingest_data import main_flow

docker_block = DockerContainer.load("docker-container")

deploy = Deployment(name="docker-flow")
deploy.parameters = {
    "output_dir": ".sample_data",
    "taxi_color": "yellow",
    "year": "2020",
    "month": "08",
}

docker_deploy = deploy.build_from_flow(
    flow=main_flow, name="docker-flow", infrastructure=docker_block, load_existing=True
)


if __name__ == "__main__":
    docker_deploy.apply()
