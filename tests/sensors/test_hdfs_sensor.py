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
import six
import unittest

from datetime import timedelta

from airflow import configuration, DAG
from airflow.exceptions import AirflowSensorTimeout
from airflow.hooks.hdfs_hook import HDFSHook
from airflow.models import Connection
from airflow.sensors.hdfs_sensor import HdfsSensor
from airflow.utils import db
from airflow.utils.timezone import datetime
from tests.core import FakeHDFSHook

configuration.load_test_config()

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'unit_test_dag'


class HdfsSensorTests(unittest.TestCase):

    def setUp(self):
        self.hook = FakeHDFSHook
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=args)

    def test_legacy_file_exist(self):
        """
        Test the legacy behaviour
        :return:
        """
        # When
        task = HdfsSensor(task_id='Should_be_file_legacy',
                          filepath='/datadirectory/datafile',
                          timeout=1,
                          retry_delay=timedelta(seconds=1),
                          poke_interval=1,
                          hook=self.hook)
        task.execute(None)

        # Then
        # Nothing happens, nothing is raised exec is ok

    def test_legacy_file_exist_but_filesize(self):
        """
        Test the legacy behaviour with the filesize
        :return:
        """
        # When
        task = HdfsSensor(task_id='Should_be_file_legacy',
                          filepath='/datadirectory/datafile',
                          timeout=1,
                          file_size=20,
                          retry_delay=timedelta(seconds=1),
                          poke_interval=1,
                          hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)

    def test_legacy_file_does_not_exists(self):
        """
        Test the legacy behaviour
        :return:
        """
        task = HdfsSensor(task_id='Should_not_be_file_legacy',
                          filepath='/datadirectory/not_existing_file_or_directory',
                          timeout=1,
                          retry_delay=timedelta(seconds=1),
                          poke_interval=1,
                          hook=self.hook)

        # When
        # Then
        with self.assertRaises(AirflowSensorTimeout):
            task.execute(None)

    @unittest.skipIf(six.PY3, "Snakebite is not compatible with Python 3 for now")
    def test_hdfs_sensor(self):
        db.merge_conn(Connection(conn_id="hdfs_default",
                                 conn_type="hdfs",
                                 host="localhost",
                                 port=8020))
        filepath = "/tmp/foo"
        list(HDFSHook().get_conn().mkdir(paths=[os.path.join(filepath, "bar")],
                                         create_parent=True))
        t = HdfsSensor(
            task_id='hdfs_sensor_check',
            filepath=filepath,
            timeout=120,
            dag=self.dag)
        t.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
              ignore_ti_state=True)
