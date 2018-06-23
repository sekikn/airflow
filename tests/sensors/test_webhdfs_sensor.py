# -*- coding: utf-8 -*-
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
import os
import unittest

from airflow import configuration, DAG
from airflow.hooks.webhdfs_hook import WebHDFSHook
from airflow.models import Connection
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor
from airflow.utils import db
from airflow.utils.timezone import datetime

configuration.load_test_config()

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'


class WebHdfsSensorTests(unittest.TestCase):

    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=args)

    def test_webhdfs_sensor(self):
        db.merge_conn(Connection(conn_id="webhdfs",
                                 conn_type="hdfs",
                                 host="0.0.0.0",
                                 port=50070))
        filepath = "/tmp/foo"
        WebHDFSHook('webhdfs').get_conn().makedirs(os.path.join(filepath, "bar"))
        t = WebHdfsSensor(
            task_id='webhdfs_sensor_check',
            filepath=filepath,
            webhdfs_conn_id="webhdfs",
            timeout=120,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
              ignore_ti_state=True)
