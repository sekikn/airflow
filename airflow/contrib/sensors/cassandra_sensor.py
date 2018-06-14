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
from airflow.contrib.hooks.cassandra_hook import CassandraHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class CassandraSensor(BaseSensorOperator):
    """
    Checks for the existence of a record in a Cassandra cluster
    """
    template_fields = ('table', 'keys')

    @apply_defaults
    def __init__(self, table, keys, cassandra_conn_id, *args, **kwargs):
        """
        Create a new CassandraSensor

        :param table: Target Cassandra table.
                      Use dot notation to target a specific keyspace.
        :type table: string
        :param keys: The keys and their values to be monitored
        :type keys: dict
        :param cassandra_conn_id: The connection ID to use
                                  when connecting to Cassandra cluster
        :type cassandra_conn_id: string
        """
        super(CassandraSensor, self).__init__(*args, **kwargs)
        self.cassandra_conn_id = cassandra_conn_id
        self.table = table
        self.keys = keys

    def poke(self, context):
        self.log.info('Sensor check existence of record: %s', self.keys)
        try:
            hook = CassandraHook(self.cassandra_conn_id)
            return hook.record_exists(self.table, self.keys)
        except Exception as e:
            self.log.exception(e)
        return False
