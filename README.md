# Data pipeline POC

## Development guide:

- Store Google Cloud Platform key file under the path `credentials/gotit-analytics.json`

- Start airflow
```
docker-compose up
```
Airflow UI is now located at http://localhost:8080

- Open Airflow UI and add the following variables:

```
giap_es_index: Elasticsearch index which stores GIAP events
giap_es_username: Elasticsearch username 
giap_es_password: Elasticsearch password 
```

- Add the following connections:

```
- gotit_study_mysql: MySQL credentials to connect to Got It Study DB
- gotit_analytics_gc: Google Cloud Platform credentials to connect to Google Cloud Platform. With this connection, please add the following values:
+ Project Id: gotit-analytics
+ Keyfile Path: /credentials/gotit-analytics.json
```

- The DAG named `study_pn_campaign` is hardcoded to query data from `1605606503` to `1605692903`. By triggering this dag, we will get engagements which happened in this time range.