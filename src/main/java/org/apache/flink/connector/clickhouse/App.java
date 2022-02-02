package org.apache.flink.connector.clickhouse;

import org.apache.flink.connector.clickhouse.catalog.ClickHouseCatalog;
import org.apache.flink.connector.clickhouse.config.ClickHouseConfig;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;

import java.util.HashMap;
import java.util.Map;

/** test. */
public class App {
    public static void main(String[] args) {
        try {
            TableEnvironment tEnv =
                    TableEnvironment.create(
                            EnvironmentSettings.newInstance()
                                    .inBatchMode()
                                    .useBlinkPlanner()
                                    .build());

            Map<String, String> props = new HashMap<>();
            props.put(ClickHouseConfig.DATABASE_NAME, "my_db_name");
            props.put(ClickHouseConfig.URL, "clickhouse://localhost:8123");
            props.put(ClickHouseConfig.USERNAME, "clickhouse-user");
            props.put(ClickHouseConfig.PASSWORD, "secret");
            props.put(ClickHouseConfig.SINK_FLUSH_INTERVAL, "30s");
            Catalog cHcatalog = new ClickHouseCatalog("clickhouse", props);
            tEnv.registerCatalog("clickhouse", cHcatalog);
            tEnv.useCatalog("clickhouse");
            tEnv.executeSql(
                    "insert into `my_db_name`.`users` select 1 as `user_id`, 2 as `UserType`, 'qwe' as `language`, 'qwe' as `country`, 'qw' as `gender`, 123 as score, array['qwe'] as list");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
