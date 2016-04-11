# -*- coding: utf-8 -*-

# Copyright (C) 2015 Baifendian Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import THBaseService
from hbase.ttypes import *

class DataProcess(object):

    def __init__(self, conf):
        print "init dataprocess"
        self._section_name = "DataProcess"
        self.thrift_host = conf.get(self._section_name, "HBASE_THRIFT_HOST", required=True)
        self.port = conf.getint(self._section_name, "HBASE_THRIFT_PORT", required=True)
         
        self.transport = TSocket.TSocket(self.thrift_host, self.port)
        self.transport = TTransport.TBufferedTransport(self.transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = THBaseService.Client(self.protocol)

        self.table_name = conf.get(self._section_name, "HBASE_TABLE_ID", required=True)
        self.id_family = conf.get(self._section_name, "HBASE_FAMILY_ID")
        self.id_qualifier = conf.get(self._section_name, "HBASE_QUALIFIER_ID")
        try:
            self.transport.open()
        except TTransport.TTransportException:
            raise Exception('Error to open thrift service on {host} at port {port}'.format(host=self.thrift_host, port=self.port))

    def get(self, row_key, table_name, id_family, id_qualifier):
        try:
            print "request hbase"
            column = TColumn(family = id_family, qualifier=id_qualifier)
            get = TGet( row = row_key, columns = [column])
            id_result = self.client.get(table_name, get)
            return id_result 
        except Exception, e:
            print "get error:%s" % e
            return None


    def get_default(self, row_key):
        return self.get(row_key, self.table_name, self.id_family, self.id_qualifier)

    def __del__(self):
        self.transport.close()
        self.client = None

    



if __name__ == "__main__":
    pass
