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

package org.apache.kudu.hive.tools;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.HiveMetastoreConfig;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Prototype of a class to read HMS metadata.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HiveMetaStoreDumper {
  public static long TIMEOUT_MS = 10000;
  public static Logger LOGGER = LoggerFactory.getLogger(HiveMetaStoreDumper.class);

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      LOGGER.error("Usage: {} kudu_master_addresses full_table_name",
                   HiveMetaStoreDumper.class.getSimpleName());
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

    IMetaStoreClient hmsClient = new HiveMetaStoreClient(hiveConf);

    Table table = hmsClient.getTable(dbName, tableName);
    String owner = table.getOwner();
    String type = table.getTableType();
    String storageHandler = table.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);

    LOGGER.info("owner: {}, type: {}, handler: {}", owner, type, storageHandler);
  }
}
