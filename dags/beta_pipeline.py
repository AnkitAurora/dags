from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


@dag(
    dag_id="aem_pipeline",
    start_date=datetime(2025,1,1),
    schedule_interval="0 13 * * *",
    catchup=False,
    default_args=default_args,
    tags=["aem", "adobe"]
)

def aem_pipeline():

    @task
    def get_objects():
        return [
            {"object_name": "public"},
            {"object_name": "partner"},
        ]

    @task_group(group_id="process_object")
    def process_object(object_name):

        @task
        def get_assets(object_name):
            print(f"Fetching assets for {object_name}")
            return {"object_name": object_name, "count": 100}

        @task
        def load_assets(asset_info):
            print(f"loading {asset_info['count']} for assets for {asset_info['object_name']}")
            return asset_info

        @task
        def transform_assets(asset_info):
            print(f"Transforing assets for {asset_info['object_name']}")

        info = get_assets(object_name)
        loaded = load_assets(info)
        transform_assets(loaded)

    @task
    def export_metadata():
        print("Exporting metata to Data Cloud")

    objects = get_objects()
    processed = process_object.expand_kwargs(objects)
    export = export_metadata()

    processed >> export

    aem_pipeline()