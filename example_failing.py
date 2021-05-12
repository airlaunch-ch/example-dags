#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the PythonOperator."""
import time
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime

from airflow.providers.google.cloud.hooks.gcs import GCSHook

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 1, 30),
    'email': ['benjamin.staubli@airlaunch.ch']
}


with DAG(
    dag_id='example_failing',
    default_args=default_args,
    schedule_interval=None,
    tags=['example'],
    start_date=days_ago(2),
) as dag:

    def download_json():
        hook = GCSHook()
        file = hook.download(bucket_name='demo-bucket.airlaunch.ch', object_name='data/covid_pipeline_scheduled__2020-02-05T00:00:00+00:00.json')
        print(file)
        return

    run_this = PythonOperator(
        task_id='print_the_context',
        python_callable=download_json,
    )