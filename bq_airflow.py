import airflow
import datetime
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'BQ Export Example',
    'start_date': days_ago(2),
}


dag = DAG(
    dag_id= 'example_bq_export',
    default_args=args,
    schedule_interval= None
    #schedule_interval=timedelta(minutes=1)
)


def get_metrics(project_id, next_page_token):
    """ Call the https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.metricDescriptors/list
        using the googleapiclient to get all the metricDescriptors for the project
    """
#GET https://monitoring.googleapis.com/v3/{name}/metricDescriptors

    service = build('monitoring', 'v3', cache_discovery=True)
    project_name = 'projects/{project_id}'.format(
        project_id=project_id
    )

    metrics = service.projects().metricDescriptors().list(
         name=project_name,
         pageSize=config.PAGE_SIZE,
         pageToken=next_page_token
    ).execute()

    logging.debug("project_id: {}, size: {}".format(
        project_id,
        len(metrics["metricDescriptors"])
        )
    )
    return metrics

#contributor operator, gcs hook operator



    
with airflow.DAG(
        'composer_sample_dag',
        'catchup=False',
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:

    # Print the dag_run id from the Airflow logs
    print_dag_run_conf = bash_operator.BashOperator(
        task_id='print_dag_run_conf', bash_command='echo {{ dag_run.id }}')


t1 = PythonOperator(
    task_id='list_stackdriver_metrics',
    python_callable= get_metrics,
    op_projectid= {'project_id':""},
    op_nextpagetoken={'next_page_token':""},
    dag=dag
)

#Create BigQuery output dataset.
make_bq_dataset = bash_operator.BashOperator(
    task_id='make_bq_dataset',
    # Executing 'bq' command requires Google Cloud SDK which comes
    # preinstalled in Cloud Composer.
    bash_command='bq ls {} || bq mk {}'.format(
    bq_dataset_name, bq_dataset_name))
# Load the event file to Bigquery
load_events_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id='load_events_data_to_bigquery',
    bucket=os.environ.get('GCS_BUCKET').split("/")[-1],
    source_objects=[
        'data/output_events_{date}.txt'.format(date="{{ ds }}")
    ],
    destination_project_dataset_table='meetup.event',
    source_format='NEWLINE_DELIMITED_JSON',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND',
    schema_object='dags/schemas/event_schema.json'
)

t2 = BashOperator(
    task_id='echo', 
    bash_command='echo test', 
    dag=dag, 
    depends_on_past=False)

#excoms
#from one operator to another operator

t1 >> t2
            

#YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
# These args will get passed on to each operator
default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

from google.cloud import monitoring_v3
client = monitoring_v3.MetricServiceClient()
import datetime
target_project = '[Your project]'
start = datetime.datetime(2019,3,5, 0, 0, 0, 0)
end = datetime.datetime(2019,3,6, 0, 0, 0, 0)
topic_bytes = 'pubsub.googleapis.com/topic/byte_cost'
def list_metric_descriptors(client, project_resource, metric):
  resource_descriptors = client.list_metric_descriptors(
      project_resource,
      'metric.type="{}"'.format(metric))
  for descriptor in resource_descriptors:
    print(descriptor)