/*
    Copyright 2019 Supercomputing Systems AG
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
//! This examples shows how to use the compose_extrinsic_offline macro which generates an extrinsic
//! without asking the node for nonce and does not need to know the metadata
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

fn run_sql_by_owner_and_add_delegate(owner: &AccountKeyring, delegate_address: &db3_runtime::Address) {

    let url = get_node_url_from_cli();
    let client = WsRpcClient::new(&url);

    // initialize api and set the signer (sender) that is used to sign the extrinsics
    let api = Api::<_, _, AssetTipExtrinsicParams>::new(client)
        .map(|api| api.set_signer(owner.pair()))
        .unwrap();

    // Information for Era for mortal transactions
    println!("[+] Subscribe to events ... ");
    let (events_in, events_out) = channel();
    api.subscribe_events(events_in).unwrap();

    let mut req_id = 1234;

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
        Call::SQLDB(pallet_sql_db::Call::create_ns {
            ns: "test_ns".as_bytes().to_vec(),
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


    req_id += 1;
    let tx_param = generate_tx_param(&api);
    let api = api.set_extrinsic_params_builder(tx_param);
    println!(
        "[+] Account Nonce is {}\n",
        api.get_nonce().unwrap()
    );
    println!("[+] req_id: {}, runSqlByOwner: create table >>>>>>>>", req_id);
    #[allow(clippy::redundant_clone)]
        let xt: UncheckedExtrinsicV4<_, _> = compose_extrinsic_offline!(
        api.clone().signer.unwrap(),
        Call::SQLDB(pallet_sql_db::Call::run_sql_by_owner {
            data: "create table location(id INT NOT NULL PRIMARY KEY, coordinates VARCHAR(50));".as_bytes().to_vec(),
            req_id: req_id.to_string().as_bytes().to_vec(),
            ns: "test_ns".as_bytes().to_vec()
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
    let tx_param = generate_tx_param(&api);
    let api = api.set_extrinsic_params_builder(tx_param);
    println!(
        "[+] Account Nonce is {}\n",
        api.get_nonce().unwrap()
    );
    println!("[+] req_id: {}, runSqlByOwner: insert table >>>>>>>>", req_id);
    #[allow(clippy::redundant_clone)]
        let xt: UncheckedExtrinsicV4<_, _> = compose_extrinsic_offline!(
        api.clone().signer.unwrap(),
        Call::SQLDB(pallet_sql_db::Call::run_sql_by_owner {
            data: "insert into location values
(1, '37.772,-122.214'),
(2, '21.291,-157.821');".as_bytes().to_vec(),
            req_id: req_id.to_string().as_bytes().to_vec(),
            ns: "test_ns".as_bytes().to_vec()
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
    let tx_param = generate_tx_param(&api);
    let api = api.set_extrinsic_params_builder(tx_param);
    println!(
        "[+] Account Nonce is {}\n",
        api.get_nonce().unwrap()
    );
    println!("[+] req_id: {}, runSqlByOwner: select * from table >>>>>>>>", req_id);
    #[allow(clippy::redundant_clone)]
        let xt: UncheckedExtrinsicV4<_, _> = compose_extrinsic_offline!(
        api.clone().signer.unwrap(),
        Call::SQLDB(pallet_sql_db::Call::run_sql_by_owner {
            data: "select * from location;".as_bytes().to_vec(),
            req_id: req_id.to_string().as_bytes().to_vec(),
            ns: "test_ns".as_bytes().to_vec()
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

    let tx_param = generate_tx_param(&api);
    let api = api.set_extrinsic_params_builder(tx_param);
    println!(
        "[+] Account Nonce is {}\n",
        api.get_nonce().unwrap()
    );
    println!("[+] req_id: {}, add delegate to {} for test_ns >>>>>>>>", req_id, delegate_address);
    #[allow(clippy::redundant_clone)]
        let xt: UncheckedExtrinsicV4<_, _> = compose_extrinsic_offline!(
        api.clone().signer.unwrap(),
        Call::SQLDB(pallet_sql_db::Call::add_delegate {
            delegate: delegate_address.clone(),
            ns: "test_ns".as_bytes().to_vec(),
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
fn run_sql_by_delegate(delegate : &AccountKeyring, owner_address: &db3_runtime::Address) {
    let url = get_node_url_from_cli();
    let client = WsRpcClient::new(&url);

    // initialize api and set the signer (sender) that is used to sign the extrinsics

    let api = Api::<_, _, AssetTipExtrinsicParams>::new(client)
        .map(|api| api.set_signer(delegate.pair()))
        .unwrap();

    println!("[+] Subscribe to events ... ");
    let (events_in, events_out) = channel();
    api.subscribe_events(events_in).unwrap();

    let mut req_id = 3234;
    req_id += 1;
    let tx_param = generate_tx_param(&api);
    let api = api.set_extrinsic_params_builder(tx_param);
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
            data: "insert into location values
(3, '-18.142,178.431'),
(4, '-27.467,153.027');".as_bytes().to_vec(),
            req_id: req_id.to_string().as_bytes().to_vec(),
            ns: "test_ns".as_bytes().to_vec()
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
    let tx_param = generate_tx_param(&api);
    let api = api.set_extrinsic_params_builder(tx_param);
    println!(
        "[+] Delegate Account Nonce is {}\n",
        api.get_nonce().unwrap()
    );
    println!("[+] req_id: {}, runSqlByDelegate: select * from table >>>>>>>>", req_id);
    #[allow(clippy::redundant_clone)]
        let xt: UncheckedExtrinsicV4<_, _> = compose_extrinsic_offline!(
        api.clone().signer.unwrap(),
        Call::SQLDB(pallet_sql_db::Call::run_sql_by_delegate {
            owner: owner_address.clone(),
            data: "select * from location;".as_bytes().to_vec(),
            req_id: req_id.to_string().as_bytes().to_vec(),
            ns: "test_ns".as_bytes().to_vec()
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

fn main() {
    env_logger::init();
    let bob_address = db3_runtime::Address::Id(
        db3_runtime::AccountId::new(AccountKeyring::Bob.to_account_id().into()));
    let alice_address = db3_runtime::Address::Id(
        db3_runtime::AccountId::new(AccountKeyring::Alice.to_account_id().into()));

    run_sql_by_owner_and_add_delegate(&AccountKeyring::Alice, &bob_address);
    run_sql_by_delegate(&AccountKeyring::Bob, &alice_address);
}
#[derive(Clone, Debug, PartialEq, Deserialize)]
struct ResponseBody<'a> {
    status: u8,
    msg: &'a str,
    req_id: &'a str,
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

pub fn get_node_url_from_cli() -> String {
    let yml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yml).get_matches();

    let node_ip = matches.value_of("node-server").unwrap_or("ws://127.0.0.1");
    let node_port = matches.value_of("node-port").unwrap_or("9944");
    let url = format!("{}:{}", node_ip, node_port);
    println!("Interacting with node on {}\n", url);
    url
}

fn generate_tx_param<P>(api: &Api<P, WsRpcClient, AssetTipExtrinsicParams>) -> AssetTipExtrinsicParamsBuilder {
    let head = api.get_finalized_head().unwrap().unwrap();
    let h: Header = api.get_header(Some(head)).unwrap().unwrap();
    let period = 5;
    AssetTipExtrinsicParamsBuilder::new()
        .era(Era::mortal(period, h.number.into()), head)
        .tip(AssetTip::new(0))
}
