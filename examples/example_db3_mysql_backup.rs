use mysql_cdc::binlog_client::BinlogClient;
use mysql_cdc::binlog_options::BinlogOptions;
use mysql_cdc::errors::Error;
use mysql_cdc::providers::mariadb::gtid::gtid_list::GtidList;
use mysql_cdc::providers::mysql::gtid::gtid_set::GtidSet;
use mysql_cdc::replica_options::ReplicaOptions;
use mysql_cdc::ssl_mode::SslMode;
use std::env;
use mysql_cdc::events::event_header::EventHeader;
use mysql_cdc::events::binlog_event::BinlogEvent;
use std::collections::HashMap;
use mysql_cdc::events::table_map_event::TableMapEvent;
use mysql_cdc::events::row_events::row_data::RowData;
use mysql_cdc::events::row_events::mysql_value::MySqlValue;

fn main() -> Result<(), Error> {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    if args.len() <= 3 {
        log::error!("Try passing mysql_username, mysql_password, binlog_file");
        return Err(Error::String(String::from("Invalid Arguments")));
    }

    let username = &args[1];
    let password = &args[2];
    let binlog = &args[3];

    // Start replication from MariaDB GTID
    let _options = BinlogOptions::from_mariadb_gtid(GtidList::parse("0-1-270")?);

    // Start replication from MySQL GTID
    let gtid_set =
        "d4c17f0c-4f11-11ea-93e3-325d3e1cd1c8:1-107, f442510a-2881-11ea-b1dd-27916133dbb2:1-7";
    let _options = BinlogOptions::from_mysql_gtid(GtidSet::parse(gtid_set)?);

    // Start replication from the position
    let _options = BinlogOptions::from_position(binlog.clone(), 0);

    // Start replication from last master position.
    // Useful when you are only interested in new changes.
    let _options = BinlogOptions::from_end();

    // Start replication from first event of first available master binlog.
    // Note that binlog files by default have expiration time and deleted.
    let options = BinlogOptions::from_start();

    let options = ReplicaOptions {
        username: username.clone(),
        password: password.clone(),
        blocking: true,
        ssl_mode: SslMode::Disabled,
        binlog: options,
        ..Default::default()
    };

    let mut client = BinlogClient::new(options);

    let mut tid_2_table_map_event = HashMap::<u64, TableMapEvent>::new();
    for result in client.replicate()? {
        let (header, event) : (EventHeader, BinlogEvent) = result?;
        // println!("Header: {:#?}", header);
        // println!("Event: {:#?}", event);

        log::debug!("Replication position before event processed");
        print_position(&client);

        sync_to_db3(&header, &event, &mut tid_2_table_map_event);
        // After you processed the event, you need to update replication position
        client.commit(&header, &event);

        log::debug!("Replication position after event processed");
        print_position(&client);
    }
    Ok(())
}
fn sync_to_db3(header: &EventHeader, event: &BinlogEvent, tid_map:  &mut HashMap<u64, TableMapEvent>) {
    log::info!("{:?}", event);

    match event {
        BinlogEvent::QueryEvent(e) => {
            log::info!("Handle QueryEvent >>>");
            log::warn!("{:?}", e);
            if e.error_code != 0 {
                log::error!("Skip running sql query with query code: {}", e.error_code);
                return;
            }
            log::warn!("sql {}", e.sql_statement);
        },
        BinlogEvent::RowsQueryEvent(e) => {
            log::error!("Can't Handle RowsQueryEvent currently >>>");
            return;
        }
        BinlogEvent::WriteRowsEvent(e) => {
            log::info!("Handle WriteRowsEvent >>>");
            match tid_map.get(&e.table_id) {
                Some(table_map_event) => {

                    if !all_colume_present(&e.columns_present) {
                        log::error!(" Can't handle write rows with column not present. To be supported.");
                        return;
                    }

                    let rows = rows_to_string(&e.rows);
                    let sql = format!("INSERT INTO `{}`.`{}` VALUES ({});",
                                      table_map_event.database_name,
                                      table_map_event.table_name, rows);

                    log::info!("insert sql statement: {}", sql);
                    return;
                }
                None => {
                    log::error!("Skip WriteRowsEvent. There is no tableMapEvent correspond to tid {}.", e.table_id);
                    return;
                }
            }
            log::debug!("next event position: {}", header.next_event_position);

        },
        BinlogEvent::UpdateRowsEvent(e) => {
            log::info!("Handle UpdateRowsEvent >>>");
            match tid_map.get(&e.table_id) {
                Some(table_map_event) => {

                    if table_map_event.table_metadata.is_none() || table_map_event.table_metadata.as_ref().unwrap().column_names.is_none() {
                        log::error!("Can't handle update rows with column_names is none. To be supported.");
                        return;
                    }
                    if !all_colume_present(&e.columns_before_update) || !all_colume_present(&e.columns_after_update) {
                        log::error!("Can't handle update rows when before/after column not all present. To be supported.");
                        return;
                    }
                    if e.rows.is_empty() {
                        log::error!(" Can't handle update rows with empty rows.");
                        return;
                    }

                    let column_names = &table_map_event.table_metadata.as_ref().unwrap().column_names.as_ref().unwrap();

                    let sql = format!("UPDATE `{}`.`{}` SET {} WHERE {} LIMIT 1;",
                                      table_map_event.database_name,
                                      table_map_event.table_name,
                                      generate_eq_condition(column_names, &e.rows[0].after_update.cells),
                                      generate_eq_condition(column_names, &e.rows[0].before_update.cells));

                    log::info!("update sql statement: {}", sql);
                    return;
                }
                None => {
                    log::error!("Fail to handle DeleteRowsEvent when there is no tableMapEvent correspond to tid {}.", e.table_id);
                    return;
                }
            }
            return;
        },
        BinlogEvent::DeleteRowsEvent(e) => {
            log::info!("Handle DeleteRowsEvent >>>");
            match tid_map.get(&e.table_id) {
                Some(table_map_event) => {

                    if table_map_event.table_metadata.is_none() || table_map_event.table_metadata.as_ref().unwrap().column_names.is_none() {
                        log::error!(" Can't handle delete rows with column_names is none. To be supported.");
                        return;
                    }
                    if !all_colume_present(&e.columns_present) {
                        log::error!(" Can't handle delete rows with column not present. To be supported.");
                        return;
                    }
                    if e.rows.is_empty() {
                        log::error!(" Can't handle delete rows with empty rows.");
                        return;
                    }

                    let column_names = &table_map_event.table_metadata.as_ref().unwrap().column_names.as_ref().unwrap();

                    let sql = format!("DELETE FROM `{}`.`{}` WHERE {} LIMIT 1;",
                                      table_map_event.database_name,
                                      table_map_event.table_name,
                                      generate_eq_condition(column_names, &e.rows[0].cells));

                    log::info!("delete sql statement: {}", sql);
                    return;
                }
                None => {
                    log::error!("Fail to handle DeleteRowsEvent when there is no tableMapEvent correspond to tid {}.", e.table_id);
                    return;
                }
            }

        },
        BinlogEvent::TableMapEvent(e) => {
            tid_map.insert(e.table_id, e.clone());
        }
        BinlogEvent::UnknownEvent => {
            log::debug!("Skip UnknownEvent!");
        },
        _ => {
            log::debug!("Skip Event Type {} ", header.event_type);
        }
    }
}
fn generate_eq_condition(column_names: &Vec<String>, row_data_vec: &Vec<Option<MySqlValue>>) -> String {
    let mut conditions = Vec::<String>::new();

    for i in 0..row_data_vec.len() {
        match &row_data_vec[i] {
            Some(value) =>
                conditions.push(format!("{} = {}", column_names.get(i).unwrap(),
                                        mysql_value_to_string(&value))),
            None => conditions.push(format!("{} is NULL", column_names.get(i).unwrap())),
        }
    }
    conditions.join(" AND ")
}
/// convert MySqlValue to string
fn mysql_value_to_string(mysql_value: &MySqlValue) -> String {
    match mysql_value {
        MySqlValue::TinyInt(value) => value.to_string(),
        MySqlValue::SmallInt(value) => value.to_string(),
        MySqlValue::MediumInt(value) => value.to_string(),
        MySqlValue::Int(value) => value.to_string(),
        MySqlValue::BigInt(value) => value.to_string(),
        MySqlValue::Float(value) => value.to_string(),
        MySqlValue::Double(value) => value.to_string(),
        MySqlValue::Decimal(value) => value.to_string(),
        MySqlValue::String(value) => format!("'{}'", value),
        MySqlValue::Enum(value) => value.to_string(),
        MySqlValue::Year(year) => year.to_string(),
        MySqlValue::Date(date) => format!("'{}-{}-{}'", date.year, date.month, date.day),
        MySqlValue::Time(time) => format!("'{}:{}:{}.{:06}'", time.hour, time.minute, time.minute, time.millis),
        MySqlValue::DateTime(dt) => format!("'{}-{}-{} {}:{}:{}.{:06}'", dt.year, dt.month, dt.day,
        dt.hour, dt.minute, dt.second, dt.millis),
        MySqlValue::Timestamp(ts) => ts.to_string(),
        MySqlValue::Bit(b) => {
            panic!("Do not support Bit type value currently, Value {:?}", mysql_value);
        }
        MySqlValue::Set(set) => {
            panic!("Do not support Set type value currently, Value {:?}", mysql_value);
        }
        MySqlValue::Blob(_) => {
            panic!("Do not support Blob type value currently, Value {:?}", mysql_value);
        }
    }
}
fn rows_to_string(row_data_vec: &Vec<RowData>) -> String {
    let mut rows = Vec::<String>::new();

    for row in row_data_vec {
        let mut items = Vec::<String>::new();
        for item in &row.cells {
            match item {
                Some(value) => {
                    items.push(mysql_value_to_string(value));
                }
                None => {
                    items.push("NULL".to_string());
                }
            }
        }
        rows.push(format!("({})",items.join(",")));
    }
    return rows.join(",");
}
fn all_colume_present(present: &Vec<bool>) -> bool {
    for flag in present {
        if !flag {
            return false;
        }
    }
    return true;
}
fn print_position(client: &BinlogClient) {
    log::debug!("Binlog Filename: {:#?}", client.options.binlog.filename);
    log::debug!("Binlog Position: {:#?}", client.options.binlog.position);

    if let Some(x) = &client.options.binlog.gtid_list {
        log::debug!("MariaDB Gtid Position: {:#?}", x.to_string());
    }
    if let Some(x) = &client.options.binlog.gtid_set {
        log::debug!("MySQL Gtid Position: {:#?}", x.to_string());
    }
}
