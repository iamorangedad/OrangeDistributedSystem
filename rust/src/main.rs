//! OrangeOrder 的 Rust 实现。一个程序，用子命令区分进程，方便和 Python 包对照着读。

mod common;
mod gateway;
mod http_util;
mod order;
mod recommender;
mod store;
mod supervisor;
mod worker;

use std::env;

fn main() {
    let mut args: Vec<String> = env::args().skip(1).collect();
    let command = match args.first() {
        Some(arg) if !arg.starts_with('-') => args.remove(0),
        _ => "supervisor".to_string(),
    };
    match command.as_str() {
        "supervisor" => supervisor::main(&args),
        "store" => store::main(&args),
        "gateway" => gateway::main(&args),
        "order" => order::main(&args),
        "recommender" => recommender::main(&args),
        "worker" => worker::main(&args),
        other => {
            eprintln!("未知子命令 {other}。可用：supervisor、store、gateway、order、recommender、worker");
            std::process::exit(2);
        }
    }
}
