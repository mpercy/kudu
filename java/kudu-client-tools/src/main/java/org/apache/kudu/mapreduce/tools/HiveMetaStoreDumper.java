// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.kudu.mapreduce.tools;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.HiveMetastoreConfig;

import java.util.List;

public class HiveMetaStoreDumper {
  public static long TIMEOUT_MS = 10000;

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: HiveMetaStoreDumper kudu_master_addresses full_table_name");
      System.exit(1);
    }
    String kuduMasterAddresses = args[0];
    String fullTableName = args[1];

    List<String> parts = Splitter.on('.').splitToList(fullTableName);
    Preconditions.checkState(parts.size() == 2);
    String dbName = parts.get(0);
    String tableName = parts.get(1);

    AsyncKuduClient.AsyncKuduClientBuilder builder =
        new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasterAddresses);
    AsyncKuduClient kuduClient = builder.build();

    HiveMetastoreConfig hmsConfig = kuduClient.getHiveMetastoreConfig().join(TIMEOUT_MS);
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hmsConfig.getHiveMetastoreUris());
    hiveConf.setBoolVar(
        HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL,
        hmsConfig.getHiveMetastoreSaslEnabled());

    // Check that the owner of the table in the HMS matches.
    IMetaStoreClient hmsClient = new HiveMetaStoreClient(hiveConf);

    Table table = hmsClient.getTable(dbName, tableName);
    String owner = table.getOwner();
    String type = table.getTableType();
    String storageHandler = table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);

    StringBuilder sb = new StringBuilder();
    sb.append("owner: ").append(owner).append(", ")
      .append("type: ").append(type).append(", ")
      .append("handler: ").append(storageHandler);
    System.out.println(sb.toString());
  }
}
