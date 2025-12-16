import time
import yaml
import json
import logging
import oracledb
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.client import V1PersistentVolumeClaim, V1ObjectMeta
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
from pytz import timezone


## Default JOB Parameters
DAG_NAME = 'YonoB_Daily_k8'
CONFIG_FILE = 'db2_to_uds_config.yml'
CONFIGURATION_NAME = 'db2_to_uds'
MAIN_METHOD = 'extract_adapter.MainAdapter'
RUN_FILE = 'task_executor.py'
## Fetch the system parameters
HOME_PATH = Variable.get('HOME_PATH', '/opt/airflow')
CONFIG_FOLDER = Variable.get('CONFIG_FOLDER', '/opt/airflow')
SCRIPT_PATH = Variable.get('SCRIPT_PATH')
DAGS_PVC = Variable.get('DAGS_PVC')
LOGS_PVC = Variable.get('LOGS_PVC')
STG_PVC = Variable.get('STG_PVC', deserialize_json=True)
IMAGE = Variable.get('IMAGE')
NAMESPACE = Variable.get('NAMESPACE')
AIRFLOW_USER = Variable.get('AIRFLOW_USER')
AIRFLOW_EMAIL_GROUP = Variable.get('ETL_developer_group')
## Initialize Logger
logger = logging.getLogger(__name__)
 
# Default args of dags 
default_args = {
    'owner': "HCL MARTECH",
    'email': [AIRFLOW_EMAIL_GROUP],
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': 300,
    'retry_exponential_backoff': True,
    'start_date': datetime(2025, 2, 17),
    'schedule_interval': '30 23 * * *',
    'catchup': False,
    'description': 'Execute everyday at 9.15 a.m. IST - To Load data to Oracle',
    'timezone': timezone('Asia/Kolkata')
}
 
config_file = f'{HOME_PATH}/{CONFIG_FOLDER}/{CONFIG_FILE}'

def create_pvc(pvc_dict, namespace=NAMESPACE):
    '''
    The Method create the PVC is doesn't exist and retries after 5 sec till the stua becomes
    to Bound for 3 minutes otherwise after 3 minutes will result in an error and manual 
    intervention would be required.
    '''
    try:
        pvc_name = pvc_dict['name']
        counter = 0
        config.load_incluster_config()
        v1_pvc = client.CoreV1Api()
        pvc = v1_pvc.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
        if pvc is not None:
            logger.info(f'PVC {pvc_name} already exist. Status: {pvc.status.phase}')

            while pvc.status.phase != 'Bound':
                logger.info(f'Waiting for PVC {pvc_name} to be Bound. Current Status: {pvc.status.phase}')
                time.sleep(5)
                counter += 5
                if counter > 180:
                    raise Exception('Issue Bounding the PVC Manual intervention required')
                pvc = v1_pvc.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
            logger.info(f'PVC {pvc_name} now status: {pvc.status.phase}')
    except ApiException as e:
        counter = 0
        if e.status == 404:
            logger.info(f'PVC {pvc_name} not found. Creating!')
            ## Define stars of PVC
            pvc = V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name=pvc_name),
                spec={
                    'accessModes': pvc_dict['accessModes'],
                    'resources': pvc_dict['resources'],
                    'storageClassName': pvc_dict['storageClassName'] 
                }
            )
            ## Create PVC
            v1_pvc.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)

            #Wait for PVC Status to be changed to Bound
            while pvc.status is None or pvc.status.phase != 'Bound':
                logger.info(f'Waiting for PVC {pvc_name} to be Bound. Current Status: {pvc.status.phase}')
                time.sleep(5)
                counter += 5
                if counter > 180:
                    raise Exception('Issue Bounding the PVC Manual intervention required')
                pvc = v1_pvc.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
            logger.info(f'PVC {pvc_name} now status: {pvc.status.phase}')
        else:
            raise e

def get_job_config(configfile):
    '''
    The Method read the configuration file
    '''
    logger.info('Loading job configuration file: %s', configfile)
    with open(configfile, 'r') as file:
        conf = yaml.safe_load(file)
    return conf[CONFIGURATION_NAME]
 
def create_worker_tasks(table, obj, dag, method):
    '''
    The Method Mounts the PVC volume and create the K8PodOperations
    downstream for airflow task
    '''
    ## Initialize parameters
    dag_volume = 'airflow-dags-volume'
    logs_volume = 'airflow-logs-volume'
    stg_volume = 'airflow-worker-tmp-volume'
    vol = []
    
    ## This should run for the very first run otherwise the check
    ## will become overhead
    if STG_PVC['status'] or STG_PVC['status'].upper() == 'TRUE':
        create_pvc(STG_PVC)

    ## create the list of volumemounts
    def create_volumesmounts(path, volume_name):
        var = ''
        if path == 'etl_inward_files' or path == 'logs':
            var = k8s.V1VolumeMount(
               name=volume_name, mount_path=f'{HOME_PATH}/{path}'
            )
        else:
            var = k8s.V1VolumeMount(
                name=volume_name, mount_path=f'{HOME_PATH}/{path}',
                sub_path=path.split('_')[-1]
            )
        return var
    ## Iterate over the folder/mounts to be created
    for path in ['dags', 'etl_scripts', 'etl_configs', 'etl_metadata']:
        vol.append(create_volumesmounts(path, dag_volume))
    
    vol.append(create_volumesmounts('logs', logs_volume))
    vol.append(create_volumesmounts('etl_inward_files', stg_volume))
    
    volume_dag = k8s.V1Volume(
        name=dag_volume,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=DAGS_PVC),
    )
    volume_log = k8s.V1Volume(
        name=logs_volume,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=LOGS_PVC),
    )
    volume_worker_stg = k8s.V1Volume(
        name=stg_volume,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=STG_PVC['name']),
    )

    ## Check for temp storage PVC and its bound condition if it exist then ignore otherwise createone

    return KubernetesPodOperator(
        task_id=f'load_{table}_toUDS' if method == 'load_oracle' else f'extract_SFG_{table}_toUDSFS',
        namespace=NAMESPACE,
        image=IMAGE,
        cmds=[
            "python",  # Run Python
            f"{SCRIPT_PATH}{RUN_FILE}",  # Execute task_executor.py with args
            f"{config_file}",  # Path to the config file
            f"{CONFIGURATION_NAME}",  # Config tag
            f"{json.dumps(obj)}",  # Convert object_params to string
            f"{MAIN_METHOD}",  # Class import path as string
            f"{method}"
        ],
        volumes=[volume_dag, volume_log, volume_worker_stg],
        volume_mounts=vol,
        
        on_finish_action='delete_pod',
        termination_message_policy='FallbackToLogsOnError',
        reattach_on_restart=True,
    log_pod_spec_on_failure=False,
        # Run the container as root to allow elevated permissions
        #security_context=k8s.V1SecurityContext(run_as_user=AIRFLOW_USER, privileged=True),
        dag=dag
    )

def refresh_views_tasks(obj):
   '''
   Is used to refresh the views
   '''
   view = obj['refresh_views']['views']
   return PythonOperator(
        task_id=f'refresh_{view}', 
        python_callable=execute_query_oracle, 
        provide_context=True,
        op_kwargs={'object':obj},
        trigger_rule='all_done'
    )

def execute_query_oracle(**kwargs):
    """
    Execute query on Oracle, retrieving credentials from Airflow Connections.

    Args:
        Object (str): Contains details
        connection_id (str): The Airflow connection ID for Oracle (default: 'oracle_default').

    Returns:
        None
    """
    # Fetch the connection details from Airflow
    try:
        connection_id='oracle_default'
        object = kwargs['object']
        view = object['refresh_views']['views']
        connection = BaseHook.get_connection(connection_id)

        # Retrieve the credentials and DSN
        username = connection.login
        password = connection.password
        dsn = connection.host  # Typically the host or DSN is stored here

        # Create a connection to the Oracle database using these details
        connection = oracledb.connect(user=username, password=password, dsn=dsn)

        # Create a cursor
        cursor = connection.cursor()

        # Query to fetch the last load date for the given table (adjust query as needed)
        query = f"BEGIN DBMS_SNAPSHOT.REFRESH('{view}'); END;"
        cursor.execute(query)
        connection.commit()
        logger.info(f'Materialised view refreshed - {query}')

        # Query the materialized view's last refresh time
        sql_query = f"""
        SELECT LAST_REFRESH_DATE 
        FROM ALL_MVIEWS 
        WHERE MVIEW_NAME = '{view}'
        """
        cursor.execute(sql_query)
        result = cursor.fetchone()
        if result:
            logger.info(f'Last refresh date - {result[0]}')
        else:
            logger.error(f'Failed to fetch materialized view')
            raise
        return None
    except oracledb.DatabaseError as e:
        logger.info(f"Database error occurred: {e}")
        raise
    except Exception as e:
        logger.info(f"An error occurred: {e}")
        raise
    finally:
        # Close cursor and connection
        if cursor:
            cursor.close()
        if connection:
            connection.close()   

def trigger_task(task_name):
    return TriggerDagRunOperator(
        task_id=f"trigger_{task_name}",
        trigger_dag_id=task_name,
        wait_for_completion=False,        
    )

# Airflow DAG definition
with DAG(dag_id=DAG_NAME, default_args=default_args, schedule_interval=default_args['schedule_interval'], catchup=default_args['catchup']) as dag:
    start_task = DummyOperator(
        task_id='start',
        dag=dag,
    )
    end_task = DummyOperator(
        task_id='end',
        dag=dag,
    )
    dynamic_dependency = {}
    dependent_list = []
    for obj in get_job_config(config_file)["objects"]:
        if obj["isactive"] and obj['schedule'].lower() == 'daily':
            table = obj['db_table']
            dynamic_dependency[table] = {
                'extract': create_worker_tasks(table, obj, dag, 'copy_to_staging'),
                'load': create_worker_tasks(table, obj, dag, 'load_oracle'),
            }
            start_task.set_downstream(dynamic_dependency[table]['extract'])
            dynamic_dependency[table]['extract'].set_downstream(dynamic_dependency[table]['load'])
            if obj.get('refresh_views', None) and obj['refresh_views']['dependencies'] == []:
                if obj.get("trigger", None):
                    dynamic_dependency[table]['load'] >> refresh_views_tasks(obj) >> trigger_task(obj.get("trigger", None)) >> end_task
                else:
                    dynamic_dependency[table]['load'] >> refresh_views_tasks(obj) >> end_task
            elif obj.get('refresh_views', None):
                dependent_list.extend(obj['refresh_views']['dependencies'])

    for obj in get_job_config(config_file)["objects"]:
        if obj["isactive"] and obj['schedule'].lower() == 'daily':
            table = obj['db_table']
            if obj.get('refresh_views', None) and obj['refresh_views']['dependencies'] == []:
                continue
            if obj.get('refresh_views', None) and obj['refresh_views']['dependencies'] != []:
                refresh_views = refresh_views_tasks(obj)
                for dep in obj['refresh_views']['dependencies']:
                    refresh_views.set_upstream(dynamic_dependency[dep]['load'])
                refresh_views.set_downstream(end_task)
            elif table not in dependent_list:
                dynamic_dependency[table]['load'].set_downstream(end_task)



can you just follow this approach. not the complete code but the way it is creating dag?

I am not getting any error but I want you to rewrite earlier code like this and use loop technique to generate dag and also change the source code if it requires

Yes—below is your **earlier MinIO DAG rewritten in the exact same “loop technique” style** as your working `YonoB_Daily_k8` DAG (create tasks in a `for obj in config["objects"]` loop; no `.partial()` / no mapping). This will create **one KubernetesPodOperator pod per table**, and Airflow will run them **in parallel** (subject to concurrency/pools) [1].  

Also included is the required **source-code entrypoint change** so each pod processes only that table/object, but **keeps running until it finishes historic requirements** (it will still loop through all required dates inside `process_oracle_to_minio_with_dependencies`).

***

## DAG (loop-based task generation)

```python
import time
import json
import yaml
import logging
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.client import V1PersistentVolumeClaim, V1ObjectMeta
from kubernetes.client import models as k8s

from pytz import timezone


# ----------------------------
# Job Params
# ----------------------------
DAG_NAME = "CDP_DB_TO_Minio"
CONFIG_FILE = Variable.get("CONFIG_YAML_PATH", "etl_configs/db2_to_uds_config.yml")
CONFIGURATION_NAME = Variable.get("CONFIG_SECTION_KEY", "uds_to_minio")
RUN_FILE = "CDP_minio_ingestion.py"

HOME_PATH = Variable.get("HOME_PATH", "/opt/airflow")
CONFIG_FOLDER = Variable.get("CONFIG_FOLDER", "/opt/airflow")  # keep same pattern as your working DAG
SCRIPT_PATH = Variable.get("SCRIPT_PATH")
DAGS_PVC = Variable.get("DAGS_PVC")
LOGS_PVC = Variable.get("LOGS_PVC")
STG_PVC = Variable.get("STG_PVC", deserialize_json=True)
IMAGE = Variable.get("IMAGE")
NAMESPACE = Variable.get("NAMESPACE")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "HCL MARTECH",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": 300,
    "start_date": datetime(2025, 6, 10),
    "schedule_interval": None,
    "catchup": False,
    "timezone": timezone("Asia/Kolkata"),
}

config_file = f"{HOME_PATH}/{CONFIG_FOLDER}/{CONFIG_FILE}"


def create_pvc(pvc_dict, namespace=NAMESPACE):
    try:
        pvc_name = pvc_dict["name"]
        counter = 0
        config.load_incluster_config()
        v1_pvc = client.CoreV1Api()
        pvc = v1_pvc.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
        if pvc is not None:
            logger.info("PVC %s exists. Status: %s", pvc_name, pvc.status.phase)
            while pvc.status.phase != "Bound":
                time.sleep(5)
                counter += 5
                if counter > 180:
                    raise Exception("Issue Bounding the PVC. Manual intervention required")
                pvc = v1_pvc.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
    except ApiException as e:
        if e.status == 404:
            counter = 0
            logger.info("PVC %s not found. Creating!", pvc_name)
            pvc = V1PersistentVolumeClaim(
                metadata=V1ObjectMeta(name=pvc_name),
                spec={
                    "accessModes": pvc_dict["accessModes"],
                    "resources": pvc_dict["resources"],
                    "storageClassName": pvc_dict["storageClassName"],
                },
            )
            v1_pvc.create_namespaced_persistent_volume_claim(namespace=namespace, body=pvc)

            while True:
                pvc = v1_pvc.read_namespaced_persistent_volume_claim(name=pvc_name, namespace=namespace)
                phase = getattr(getattr(pvc, "status", None), "phase", None)
                if phase == "Bound":
                    break
                time.sleep(5)
                counter += 5
                if counter > 180:
                    raise Exception("Issue Bounding the PVC. Manual intervention required")
        else:
            raise


def get_job_config(configfile):
    logger.info("Loading job configuration file: %s", configfile)
    with open(configfile, "r") as file:
        conf = yaml.safe_load(file)
    return conf[CONFIGURATION_NAME]


def create_worker_task(table: str, obj: dict, dag: DAG):
    # Volumes + mounts (same approach as your code)
    dag_volume = "airflow-dags-volume"
    logs_volume = "airflow-logs-volume"
    stg_volume = "airflow-worker-tmp-volume"
    vol_mounts = []

    # Fix your original boolean check (otherwise 'FALSE' still triggers creation)
    stg_enabled = str(STG_PVC.get("status", "FALSE")).upper() == "TRUE"
    if stg_enabled:
        create_pvc(STG_PVC)

    def vm(path, volume_name):
        if path in ("etl_inward_files", "logs"):
            return k8s.V1VolumeMount(name=volume_name, mount_path=f"{HOME_PATH}/{path}")
        return k8s.V1VolumeMount(
            name=volume_name,
            mount_path=f"{HOME_PATH}/{path}",
            sub_path=path.split("_")[-1],
        )

    for path in ["dags", "etl_scripts", "etl_configs", "etl_metadata"]:
        vol_mounts.append(vm(path, dag_volume))
    vol_mounts.append(vm("logs", logs_volume))
    vol_mounts.append(vm("etl_inward_files", stg_volume))

    volume_dag = k8s.V1Volume(
        name=dag_volume,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=DAGS_PVC),
    )
    volume_log = k8s.V1Volume(
        name=logs_volume,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=LOGS_PVC),
    )
    volume_worker_stg = k8s.V1Volume(
        name=stg_volume,
        persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=STG_PVC["name"]),
    )

    # Make safe task id
    safe_table = "".join([c.lower() if c.isalnum() else "_" for c in table])

    # Pass exactly ONE object to the pod (JSON string)
    obj_json = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)

    # This pod will run the script entrypoint that processes *only this object*
    # but will continue looping dates inside (historic) until completed.
    return KubernetesPodOperator(
        task_id=f"cdp_minio_{safe_table}",
        name=f"cdp-minio-{safe_table}".replace("_", "-"),
        namespace=NAMESPACE,
        image=IMAGE,
        cmds=["bash", "-c"],
        arguments=[
            f"python {SCRIPT_PATH}{RUN_FILE} "
            f"--config-yaml '{config_file}' "
            f"--section '{CONFIGURATION_NAME}' "
            f"--object-json '{obj_json}'"
        ],
        volumes=[volume_dag, volume_log, volume_worker_stg],
        volume_mounts=vol_mounts,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        dag=dag,
    )


with DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=default_args["schedule_interval"],
    catchup=default_args["catchup"],
) as dag:
    start_task = DummyOperator(task_id="start")
    end_task = DummyOperator(task_id="end")

    dynamic_tasks = {}

    cfg = get_job_config(config_file)

    for obj in cfg.get("objects", []):
        if obj.get("isactive", True):
            table = obj["db_table"]
            dynamic_tasks[table] = create_worker_task(table, obj, dag)

            # parallel fan-out + fan-in
            start_task >> dynamic_tasks[table] >> end_task
```

This matches your preferred “loop-generated DAG” style and will run all table pods in parallel (Airflow decides how many at a time) [1].

***

## Source code change (required entrypoint)
You *must* change the script so it can run **one object** passed by `--object-json`.

In your `CDP_minio_ingestion.py`, add a CLI entrypoint that:
1) loads `conn_config` and YAML defaults
2) runs `process_oracle_to_minio_with_dependencies(...)` **for that single object**
3) does **not** loop all objects

Use this minimal add-on at the bottom (keeping your existing functions unchanged):

```python
import argparse
import json
from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

def run_single_object_from_payload(obj_config: dict, *, config_yaml: str, section: str, current_business_loaddt: str):
    conn_config = get_system_config()
    cfg = get_job_config(config_yaml)[section]

    oracle_config = conn_config.get("target", {})
    minio_config = conn_config.get("minio", {})

    effective_schema = obj_config.get("schema", cfg.get("schema", "appuser"))
    if effective_schema.lower() == "campaign":
        oracle_config = conn_config.get("campaign", {})

    audit_config = {
        "target": conn_config.get("target", {}),
        "schema": effective_schema,
        "audit_table": cfg.get("audit_config", {}).get("audit_table", "AIRFLOW_CDP_DIAPI_RUN_LOG"),
    }

    load_type = obj_config.get("load_type", "delta")
    where_clause = obj_config.get("where_clause", "")

    table_name = f"{effective_schema}.{obj_config['db_table']}"
    output_path = obj_config.get("output_path", cfg.get("output_path"))
    if not output_path:
        raise ValueError(f"output_path not found for object {obj_config}")

    chunk_size = obj_config.get("chunksize", cfg.get("chunksize", 100_000))
    order_by = obj_config.get("ORDER_BY")

    delta_column_type = (obj_config.get("delta_column_type", cfg.get("delta_column_type", "date")) or "date").lower()
    delta_column_format = obj_config.get("delta_column_format", cfg.get("delta_column_format", ""))

    delta_column = obj_config.get("delta_column") if load_type == "historic" else None
    sub_folder = obj_config.get("sub_folder") if load_type == "historic" else None

    # IMPORTANT: this function is the one that will keep running until historic requirements are met
    process_oracle_to_minio_with_dependencies(
        oracle_config=oracle_config,
        minio_config=minio_config,
        config_audit=audit_config,
        table_name=table_name,
        base_object_path=output_path,
        current_business_loaddt=current_business_loaddt,
        chunk_size=chunk_size,
        order_by=order_by,
        load_type=load_type,
        delta_column=delta_column,
        sub_folder=sub_folder,
        delta_column_type=("timestamp" if delta_column_type == "timestamp" else "date"),
        delta_column_format=delta_column_format,
        where_clause=where_clause,
    )

def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config-yaml", required=True)
    p.add_argument("--section", required=True)
    p.add_argument("--object-json", required=True)
    p.add_argument("--business-date", default=None)
    return p.parse_args()

if __name__ == "__main__":
    args = _parse_args()
    obj = json.loads(args.object_json)
    biz_date = args.business_date or datetime.now(IST).strftime("%Y-%m-%d")

    run_single_object_from_payload(
        obj_config=obj,
        config_yaml=args.config_yaml,
        section=args.section,
        current_business_loaddt=biz_date,
    )
```

Now each pod runs **only its table**, and if `load_type=historic`, it will continue until all pending historic dates are processed (because that logic stays inside `process_oracle_to_minio_with_dependencies`) [1].

***

If you share your exact YAML object structure for the MinIO config (`objects` fields, keys for `output_path`, `load_type`, etc.), the `run_single_object_from_payload` mapping can be aligned 1:1 with your config keys to avoid any “missing key” runtime failures.

Citations:
[1] KubernetesPodOperator — apache-airflow-providers-cncf ... https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html
[2] selected_image_6903981366468649783.jpg https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/images/90742002/9f1a92a9-bfbe-48fa-8d3c-6c09d9050dfd/selected_image_6903981366468649783.jpg
[3] Airflow kubernetes executor: Run 2 jobs on the same pod https://stackoverflow.com/questions/56547388/airflow-kubernetes-executor-run-2-jobs-on-the-same-pod
[4] Apache Airflow KubernetesPodOperator https://www.sparkcodehub.com/airflow/operators/kubernetes-pod-operator
[5] KubernetesPodOperator¶ https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/5.2.1/operators.html
[6] KubernetesPodOperator: exit as success when another task is completed. · apache airflow · Discussion #34444 https://github.com/apache/airflow/discussions/34444
[7] BigQueryInsertJobOperator fails for task IDs with 64 characters · Issue #39567 · apache/airflow https://github.com/apache/airflow/issues/39567
[8] Optimizing DAG Parsing in Airflow: A Comprehensive Guide https://www.sparkcodehub.com/airflow/performance/dag-parsing
[9] 3 Ways to run Airflow on Kubernetes https://www.truefullstaq.com/en/blog/airflow-kubernetes
[10] In airflow, can we set jinja2 strings into `task_id`? https://stackoverflow.com/questions/55104264/in-airflow-can-we-set-jinja2-strings-into-task-id
[11] How to Optimize Airflow Scheduler for Faster DAG Parsing https://www.linkedin.com/posts/marclamberti_airflow-apacheairflow-dataengineering-activity-7392206192799895552-Wndj
[12] Airflow Parallelism 101: A Comprehensive Guide - Learn - Hevo Data https://hevodata.com/learn/airflow-parallelism/
[13] Airflow scheduler with statsd enabled crashes when ... https://github.com/apache/airflow/issues/18010
[14] What Resources Might Limit... https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dagfile-processing.html
[15] Use the KubernetesPodOperator | Astronomer Documentation https://www.astronomer.io/docs/learn/kubepod-operator/
[16] Airflow 任务命名规范 | 代码酷 https://www.echo.cool/docs/data/airflow/airflow-best-practices/airflow-task-naming-convention/
[17] Best Practices — Airflow 3.1.5 Documentation https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
[18] Running Kubernetes Job using Kubernetes Pod Operator in Airflow https://stackoverflow.com/questions/74795792/running-kubernetes-job-using-kubernetes-pod-operator-in-airflow
[19] airflow.sdk API Reference https://airflow.apache.org/docs/task-sdk/stable/api.html
[20] Should you avoid top-level imports in a file defining Airflow operators https://stackoverflow.com/questions/70622772/should-you-avoid-top-level-imports-in-a-file-defining-airflow-operators
[21] Multiprocessing/multithreading in an Airflow DAG https://www.reddit.com/r/dataengineering/comments/12pc16r/multiprocessingmultithreading_in_an_airflow_dag/
