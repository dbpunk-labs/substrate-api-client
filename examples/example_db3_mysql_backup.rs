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


use std::sync::mpsc::{channel, Receiver};
use std::str;
use clap::{load_yaml, App};
use codec::Decode;
// use node_template_runtime::Event;
use ac_primitives::{AssetTipExtrinsicParamsBuilder, BaseExtrinsicParams};
use db3_runtime::{Call};
use sp_core::H256 as Hash;
use substrate_api_client::utils::FromHexString;
use node_runtime::{Header};
use sp_keyring::AccountKeyring;
use sp_runtime::generic::Era;
use substrate_api_client::rpc::WsRpcClient;
use substrate_api_client::{compose_extrinsic_offline, Api, AssetTipExtrinsicParams, UncheckedExtrinsicV4, XtStatus, AssetTip, MultiAddress};
use serde::{Deserialize};
#[derive(Clone, Debug, PartialEq, Deserialize)]
struct ResponseBody<'a> {
    status: u8,
    msg: &'a str,
    req_id: &'a str,
}
fn main() -> Result<(), Error> {
    env_logger::init();
    let yml = load_yaml!("db3_backup.yml");
    let matches = App::from_yaml(yml).get_matches();

    let node_ip = matches.value_of("node-server").unwrap_or("ws://127.0.0.1");
    let node_port = matches.value_of("node-port").unwrap_or("9944");
    let ns = matches.value_of("node-ns").unwrap_or("demo_ns");
    let username = matches.value_of("mysql-username").unwrap_or("root");
    let password = matches.value_of("mysql-password").unwrap();
    let database = matches.value_of("mysql-database").unwrap();
    let url = format!("{}:{}", node_ip, node_port);
    // Start replication from MariaDB GTID
    let _options = BinlogOptions::from_mariadb_gtid(GtidList::parse("0-1-270")?);

    // Start replication from MySQL GTID
    let gtid_set =
        "d4c17f0c-4f11-11ea-93e3-325d3e1cd1c8:1-107, f442510a-2881-11ea-b1dd-27916133dbb2:1-7";
    let _options = BinlogOptions::from_mysql_gtid(GtidSet::parse(gtid_set)?);

    // Start replication from the position
    // let _options = BinlogOptions::from_position(binlog.clone(), 0);
    let _options = BinlogOptions::from_start();

    // Start replication from last master position.
    // Useful when you are only interested in new changes.
    let _options = BinlogOptions::from_end();

    // Start replication from first event of first available master binlog.
    // Note that binlog files by default have expiration time and deleted.
    let options = BinlogOptions::from_start();

    let options = ReplicaOptions {
        username: String::from(username),
        password: String::from(password),
        blocking: true,
        ssl_mode: SslMode::Disabled,
        binlog: options,
        ..Default::default()
    };

    let mut client = BinlogClient::new(options);
    let mut tid_2_table_map_event = HashMap::<u64, TableMapEvent>::new();



    let delegate = AccountKeyring::Bob;
    let owner = AccountKeyring::Alice;

    let mut api = Api::<_, _, AssetTipExtrinsicParams>::new(WsRpcClient::new(&url))
        .map(|api| api.set_signer(delegate.pair()))
        .unwrap();

    println!("[+] Subscribe to events ... ");
    let (events_in, events_out) = channel();
    api.subscribe_events(events_in).unwrap();

    let delegate_address = db3_runtime::Address::Id(
        db3_runtime::AccountId::new(AccountKeyring::Bob.to_account_id().into()));
    let owner_address = db3_runtime::Address::Id(
        db3_runtime::AccountId::new(AccountKeyring::Alice.to_account_id().into()));

    let mut req_id = 1;

    create_ns_with_delegate(url.as_ref(), &owner, &delegate_address, ns, req_id);


    for result in client.replicate()? {
        let (header, event) : (EventHeader, BinlogEvent) = result?;
        log::debug!("Replication position before event processed");
        print_position(&client);

        match sync_to_db3(&header, &event, database, &mut tid_2_table_map_event) {
            Some(sql) => {
                if sql.is_empty() {
                    continue;
                }
                let tx_param = generate_tx_param(&api);
                let api = api.clone().set_extrinsic_params_builder(tx_param);
                println!(
                    "[+] Delegate Account Nonce is {}\n",
                    api.get_nonce().unwrap()
                );
                println!("[+] req_id: {}, runSqlByDelegate: insert table >>>>>>>>", req_id);
                #[allow(clippy::redundant_clone)]
                let xt: UncheckedExtrinsicV4<_, _> = compose_extrinsic_offline!(
                api.clone().signer.unwrap(),
                Call::SQLDB(pallet_sql_db::Call::run_sql_by_delegate {
                    owner: owner_address.clone(),
                    data: sql.as_bytes().to_vec(),
                    req_id: req_id.to_string().as_bytes().to_vec(),
                    ns: ns.as_bytes().to_vec()
                }),
                api.extrinsic_params(api.get_nonce().unwrap())
                );

                log::debug!("[+] Composed Extrinsic:\n {:?}\n", xt);
                // send and watch extrinsic until in block
                let blockh = api
                    .send_extrinsic(xt.hex_encode(), XtStatus::InBlock)
                    .unwrap();
                println!("[+] Transaction got included in block {:?}", blockh);

                println!("[+] GeneralResultEvent:\n {}", receive_sqldb_event(&events_out, req_id));
                req_id += 1;
            }
            None => {}
        }

        // After you processed the event, you need to update replication position
        client.commit(&header, &event);

        log::debug!("Replication position after event processed");
        print_position(&client);
    }
    Ok(())
}
fn create_ns_with_delegate(url: &str, owner: &AccountKeyring,
                           delegate_address: &db3_runtime::Address,
                           ns_name: &str, req_id: i32) {

    let client = WsRpcClient::new(url);

    // initialize api and set the signer (sender) that is used to sign the extrinsics
    let api = Api::<_, _, AssetTipExtrinsicParams>::new(client)
        .map(|api| api.set_signer(owner.pair()))
        .unwrap();

    // Information for Era for mortal transactions
    println!("[+] Subscribe to events ... ");
    let (events_in, events_out) = channel();
    api.subscribe_events(events_in).unwrap();

    let tx_param = generate_tx_param(&api);
    let api = api.set_extrinsic_params_builder(tx_param);
    println!(
        "[+] Account Nonce is {}\n",
        api.get_nonce().unwrap()
    );

    println!("[+] req_id: {}, create ns >>>>>>>>", req_id);
    // create_ns("test_ns", "1234");
    #[allow(clippy::redundant_clone)]
        let xt: UncheckedExtrinsicV4<_, _> = compose_extrinsic_offline!(
        api.clone().signer.unwrap(),
        Call::SQLDB(pallet_sql_db::Call::create_ns_and_add_delegate {
            ns: ns_name.as_bytes().to_vec(),
            delegate: delegate_address.clone(),
            delegate_type: 3,
            req_id: req_id.to_string().as_bytes().to_vec(),
        }),
        api.extrinsic_params(api.get_nonce().unwrap())
    );
    log::debug!("[+] Composed Extrinsic:\n {:?}\n", xt);
    // send and watch extrinsic until in block
    let blockh = api
        .send_extrinsic(xt.hex_encode(), XtStatus::InBlock)
        .unwrap();
    println!("[+] Transaction got included in block {:?}", blockh);

    println!("[+] GeneralResultEvent:\n {}", receive_sqldb_event(&events_out, req_id));
}


fn generate_tx_param<P>(api: &Api<P, WsRpcClient, AssetTipExtrinsicParams>) -> AssetTipExtrinsicParamsBuilder {
    let head = api.get_finalized_head().unwrap().unwrap();
    let h: Header = api.get_header(Some(head)).unwrap().unwrap();
    let period = 5;
    AssetTipExtrinsicParamsBuilder::new()
        .era(Era::mortal(period, h.number.into()), head)
        .tip(AssetTip::new(0))
}
/***
Try to receive one GeneralResultEvent
 */
fn receive_sqldb_event(events_out: &Receiver<String>, req_id: i32) -> String {
    for _ in 0..5 {
        let event_str = events_out.recv().unwrap();
        let _unhex = Vec::from_hex(event_str).unwrap();
        let mut _er_enc = _unhex.as_slice();
        let _events = Vec::<system::EventRecord<db3_runtime::Event, Hash>>::decode(&mut _er_enc);
        match _events {
            Ok(evts) => {
                for evr in &evts {
                    log::debug!("decoded: {:?} {:?}", evr.phase, evr.event);
                    match &evr.event {
                        db3_runtime::Event::SQLDB(be) => {
                            log::debug!(">>>>>>>>>> db3 SQLDB event: {:?}", be);
                            match &be {
                                pallet_sql_db::Event::GeneralResultEvent(event_data) => {
                                    let json_str = match str::from_utf8(event_data) {
                                        Ok(v) => v,
                                        Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
                                    };
                                    let data: ResponseBody = serde_json::from_str(json_str).unwrap();
                                    if req_id.to_string().eq(data.req_id) {
                                        return String::from(json_str);
                                    } else {
                                        log::info!("ignoring event with unexpected req_id {}", data.req_id);
                                    }
                                }
                                _ => {
                                    log::debug!("ignoring unsupported SQLDB event");
                                }
                            }
                        }
                        _ => log::info!("ignoring unsupported module event: {:?}", evr.event),
                    }
                }
            }
            Err(_) => log::error!("couldn't decode event record list"),
        }
    }
    String::from("")
}
fn sync_to_db3(header: &EventHeader, event: &BinlogEvent, database: &str,
               tid_map:  &mut HashMap<u64, TableMapEvent>) -> Option<String> {

    match event {
        BinlogEvent::QueryEvent(e) => {
            log::info!("Handle QueryEvent >>>");
            log::debug!("{:?}", e);
            if e.error_code != 0 {
                log::error!("Skip running sql query with query code: {}", e.error_code);
                return None;
            }
            if e.database_name.ne(database) {
                log::info!("Skip handling database {}", e.database_name);
                return None;
            }
            log::info!("sql {}", e.sql_statement);
            return Some(e.sql_statement.clone());
        },
        BinlogEvent::RowsQueryEvent(e) => {
            log::error!("Can't Handle RowsQueryEvent currently >>>");
            return None;
        }
        BinlogEvent::WriteRowsEvent(e) => {
            log::info!("Handle WriteRowsEvent >>>");
            match tid_map.get(&e.table_id) {
                Some(table_map_event) => {
                    if table_map_event.database_name.ne(database) {
                        log::info!("Skip handling database {}", table_map_event.database_name.as_str());
                        return None;
                    }
                    if !all_colume_present(&e.columns_present) {
                        log::error!(" Can't handle write rows with column not present. To be supported.");
                        return None;
                    }

                    let rows = rows_to_string(&e.rows);
                    let sql = format!("INSERT INTO `{}`.`{}` VALUES ({});",
                                      table_map_event.database_name,
                                      table_map_event.table_name, rows);

                    log::info!("insert sql statement: {}", sql);
                    return Some(sql);
                }
                None => {
                    log::error!("Skip WriteRowsEvent. There is no tableMapEvent correspond to tid {}.", e.table_id);
                    return None;
                }
            }
            log::debug!("next event position: {}", header.next_event_position);

        },
        BinlogEvent::UpdateRowsEvent(e) => {
            log::info!("Handle UpdateRowsEvent >>>");
            match tid_map.get(&e.table_id) {
                Some(table_map_event) => {

                    if table_map_event.database_name.ne(database) {
                        log::info!("Skip handling database {}", table_map_event.database_name.as_str());
                        return None;
                    }
                    if table_map_event.table_metadata.is_none() || table_map_event.table_metadata.as_ref().unwrap().column_names.is_none() {
                        log::error!("Can't handle update rows with column_names is none. To be supported.");
                        return None;
                    }
                    if !all_colume_present(&e.columns_before_update) || !all_colume_present(&e.columns_after_update) {
                        log::error!("Can't handle update rows when before/after column not all present. To be supported.");
                        return None;
                    }
                    if e.rows.is_empty() {
                        log::error!(" Can't handle update rows with empty rows.");
                        return None;
                    }

                    let column_names = &table_map_event.table_metadata.as_ref().unwrap().column_names.as_ref().unwrap();

                    let sql = format!("UPDATE `{}`.`{}` SET {} WHERE {} LIMIT 1;",
                                      table_map_event.database_name,
                                      table_map_event.table_name,
                                      generate_eq_condition(column_names, &e.rows[0].after_update.cells),
                                      generate_eq_condition(column_names, &e.rows[0].before_update.cells));

                    log::info!("update sql statement: {}", sql);
                    return Some(sql);
                }
                None => {
                    log::error!("Fail to handle DeleteRowsEvent when there is no tableMapEvent correspond to tid {}.", e.table_id);
                    return None;
                }
            }
        },
        BinlogEvent::DeleteRowsEvent(e) => {
            log::info!("Handle DeleteRowsEvent >>>");
            match tid_map.get(&e.table_id) {
                Some(table_map_event) => {

                    if table_map_event.database_name.ne(database) {
                        log::info!("Skip handling database {}", table_map_event.database_name.as_str());
                        return None;
                    }
                    if table_map_event.table_metadata.is_none() || table_map_event.table_metadata.as_ref().unwrap().column_names.is_none() {
                        log::error!(" Can't handle delete rows with column_names is none. To be supported.");
                        return None;
                    }
                    if !all_colume_present(&e.columns_present) {
                        log::error!(" Can't handle delete rows with column not present. To be supported.");
                        return None;
                    }
                    if e.rows.is_empty() {
                        log::error!(" Can't handle delete rows with empty rows.");
                        return None;
                    }

                    let column_names = &table_map_event.table_metadata.as_ref().unwrap().column_names.as_ref().unwrap();

                    let sql = format!("DELETE FROM `{}`.`{}` WHERE {} LIMIT 1;",
                                      table_map_event.database_name,
                                      table_map_event.table_name,
                                      generate_eq_condition(column_names, &e.rows[0].cells));

                    log::info!("delete sql statement: {}", sql);
                    return Some(sql);
                }
                None => {
                    log::error!("Fail to handle DeleteRowsEvent when there is no tableMapEvent correspond to tid {}.", e.table_id);
                    return None;
                }
            }

        },
        BinlogEvent::TableMapEvent(e) => {
            tid_map.insert(e.table_id, e.clone());
            return None;
        }
        BinlogEvent::UnknownEvent => {
            log::debug!("Skip UnknownEvent!");
            return None;
        },
        _ => {
            log::debug!("Skip Event Type {} ", header.event_type);
            return None
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
