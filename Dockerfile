FROM puckel/docker-airflow:1.10.9

ADD ./requirements.txt /additonal-requirements.txt

RUN pip install --upgrade -r /additonal-requirements.txt