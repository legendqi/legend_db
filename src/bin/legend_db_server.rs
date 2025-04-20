use futures::SinkExt;
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use std::{env, fs, io};
use std::fs::File;
use std::io::{BufRead, Read};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use legend_db::custom_error::LegendDBResult;
use legend_db::sql::engine::engine::{Engine, Session};
use legend_db::sql::engine::kv::KVEngine;
use legend_db::storage::disk::DiskEngine;

const DB_PATH: &str = "/tmp/legend_db-test/legend_db-log";
const RESPONSE_END: &str = "!!!end!!!";

const  DEFAULT_DB_FOLDER:  &str = "/var/lib/legend_db/";
const CURRENT_DB_FILE:  &str = "/var/lib/legend_db/current";

const DB_CONFIG: &str = "/etc/legend_db/legend_db.conf";

/// Possible requests our clients can send us
enum SqlRequest {
    SQL(String),
    ListTables,
    TableInfo(String),
    NoDatabase
}

impl SqlRequest {
    pub fn parse(cmd: &str) -> Self {
        let upper_cmd = cmd.to_uppercase();
        // 判断是否选择数据库，判断
        if fs::metadata(CURRENT_DB_FILE).is_err() {
            return SqlRequest::NoDatabase;
        }
        let current_db = fs::read_to_string(CURRENT_DB_FILE);
        if current_db.is_err() {
            return SqlRequest::NoDatabase;
        }
        if upper_cmd.starts_with("USE") {
            let args = upper_cmd.split_ascii_whitespace().collect::<Vec<_>>();
            if args.len() == 2 {
                return SqlRequest::SQL(cmd.into());
            }
        }
        if upper_cmd == "SHOW TABLES" {
            return SqlRequest::ListTables;
        }
        if upper_cmd.starts_with("SHOW TABLE") {
            let args = upper_cmd.split_ascii_whitespace().collect::<Vec<_>>();
            if args.len() == 3 {
                return SqlRequest::TableInfo(args[2].to_lowercase());
            }
        }
        SqlRequest::SQL(cmd.into())
    }
}


pub struct ServerSession<E: Engine> {
    session: Session<E>,
}

impl<E: Engine + 'static> ServerSession<E> {
    pub fn new(eng: MutexGuard<E>) -> LegendDBResult<Self> {
        Ok(Self {
            session: eng.session()?,
        })
    }

    pub async fn handle_request(&mut self, socket: TcpStream) -> LegendDBResult<()> {
        let mut lines = Framed::new(socket, LinesCodec::new());
        while let Some(result) = lines.next().await {
            match result {
                Ok(line) => {
                    // 解析并得到 SqlRequest
                    let req = SqlRequest::parse(&line);
                    
                    // 执行请求
                    let response = match req {
                        SqlRequest::NoDatabase => todo!("No database selected"),
                        SqlRequest::SQL(sql) => match self.session.execute(&sql) {
                            Ok(rs) => rs.to_string(),
                            Err(e) => e.to_string(),
                        },
                        SqlRequest::ListTables => self.session.get_table_names().unwrap_or_else(|e| e.to_string()),
                        SqlRequest::TableInfo(table_name) => {
                            self.session.get_table(table_name).unwrap_or_else(|e| e.to_string())
                        }
                    };

                    // 发送执行结果
                    if let Err(e) = lines.send(response.as_str()).await {
                        println!("error on sending response; error = {e:?}");
                    }
                    if let Err(e) = lines.send(RESPONSE_END).await {
                        println!("error on sending response; error = {e:?}");
                    }
                }
                Err(e) => {
                    println!("error on decoding from socket; error = {e:?}");
                }
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> LegendDBResult<()> {
    // 启动 TCP 服务
    // todo 从配置中读取bind_address和port, 启动tcp服务
    let mut addr = String::new();
    let mut port = String::new();
    let mut endpoint = String::from("0.0.0.0:8080");
    if fs::metadata(CURRENT_DB_FILE).is_err() {
        panic!("no config file")
    }
    let config_file = File::open(DB_CONFIG)?;
    let reader = io::BufReader::new(config_file);
    for line in reader.lines() {
        match line { 
            Ok(line) => {
                if line.starts_with("bind_address") {
                    addr = line.clone()
                        .split('=')
                        .nth(1)
                        .unwrap()
                        .trim()
                        .to_string();
                }
                if line.starts_with("port") {
                    port = line.clone()
                        .split('=')
                        .nth(1)
                        .unwrap()
                        .trim()
                        .to_string();
                }
            }
            Err(e) => {
                println!("error reading line; error = {e:?}");
            }
        }
    }
    if !addr.is_empty() && !port.is_empty() {
        endpoint = addr.clone() + ":" + &port;
    }

    let listener = TcpListener::bind(&endpoint).await?;
    println!("legend_db server starts, listening on: {addr}:{port}");

    // 初始化 DB
    let p = PathBuf::from(DB_PATH);
    let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
    let shared_engine = Arc::new(Mutex::new(kvengine));

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let db = shared_engine.clone();
                let mut ss = ServerSession::new(db.lock()?)?;

                tokio::spawn(async move {
                    match ss.handle_request(socket).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("internal server error {:?}", e);
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {e:?}"),
        }
    }
}
