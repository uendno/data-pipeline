from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 11, 13),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG("study_pn_campaign",
          default_args=default_args,
          schedule_interval=timedelta(1))

extract_recent_questions = MySqlToGoogleCloudStorageOperator(
    task_id="extract_recent_questions",
    dag=dag,
    sql="""
        SELECT qid, author_uid as uid, created
        FROM `qa_question`
        WHERE created >= 1605606503
        AND created < 1605692903
    """,
    bucket="pipeline-pn-campaign",
    filename="{{ ds }}/recent_questions.csv",
    mysql_conn_id='gotit_study_mysql',
    google_cloud_storage_conn_id='gotit_analytics_gc',
    export_format='csv'
)

extract_recently_active_users = MySqlToGoogleCloudStorageOperator(
    task_id="extract_recently_active_users",
    dag=dag,
    sql="""
        SELECT DISTINCT vc_account.uid as uid, vc_account.balance as balance
        FROM qa_question JOIN vc_account
        ON qa_question.author_uid = vc_account.uid
        WHERE qa_question.created >= 1605606503
        AND qa_question.created < 1605692903
    """,
    bucket="pipeline-pn-campaign",
    filename="{{ ds }}/recently_active_users.csv",
    mysql_conn_id='gotit_study_mysql',
    google_cloud_storage_conn_id='gotit_analytics_gc',
    export_format='csv'
)

process_engagements = BashOperator(
    task_id="process_engagements",
    dag=dag,
    bash_command="""
    cd /dataflow
    export GOOGLE_APPLICATION_CREDENTIALS=/credentials/gotit-analytics.json
    export PROJECT=gotit-analytics
    export REGION=asia-northeast1
    python -m get_recent_engaged_askers \
        --output=./output \
        --questions=gs://pipeline-pn-campaign/{{ ds }}/recent_questions.csv \
        --users=gs://pipeline-pn-campaign/{{ ds }}/recently_active_users.csv \
        --giap-es-index={{ var.value.giap_es_index }} \
        --giap-es-username={{ var.value.giap_es_username }} \
        --giap-es-password={{ var.value.giap_es_password }} \
        --from-ts=1605606503 \
        --to-ts=1605692903 \
        --engagement-range=10 \
        --setup_file=./setup.py \
        --project=$PROJECT \
        --region=$REGION \
        --temp_location=gs://pipeline-pn-campaign/{{ ds }} \
        --runner DataflowRunner
    """
)

process_engagements << extract_recent_questions
process_engagements << extract_recently_active_users