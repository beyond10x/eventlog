//! Repository gate, with shell retained only as a compatibility entry point.
use clap::Parser;
use std::process::{Command, ExitCode};
#[derive(Parser)]
struct Args {
    #[arg(long)]
    production_proof: bool,
}
fn main() -> ExitCode {
    let args = Args::parse();
    let tests = if args.production_proof {
        vec![
            "run",
            "--locked",
            "-p",
            "eventlog-postgres",
            "--example",
            "production-proof",
        ]
    } else {
        vec!["test", "--workspace", "--locked"]
    };
    for arguments in [
        tests,
        vec!["fmt", "--all", "--check"],
        vec![
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--",
            "-D",
            "warnings",
        ],
    ] {
        println!("gate: cargo {}", arguments.join(" "));
        match Command::new("cargo").args(arguments).status() {
            Ok(status) if status.success() => {}
            Ok(status) => {
                eprintln!("gate: refused with exit {:?}", status.code());
                return ExitCode::FAILURE;
            }
            Err(error) => {
                eprintln!("gate: cannot start cargo: {error}");
                return ExitCode::FAILURE;
            }
        }
    }
    println!("gate: green");
    ExitCode::SUCCESS
}
