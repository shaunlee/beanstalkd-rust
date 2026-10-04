//! `beanstalkd-rs cluster <command>`: the operator's tool for cluster
//! membership (docs/DESIGN.md §8 "Cluster protocol v4 and the admin channel
//! (P6-T2)" and "Membership changes (P6-T4)").
//!
//! The tool speaks the cluster port's admin channel. Every change is based
//! on the membership it just read (the compare-and-set `expect`); a node
//! that is not the leader names the leader and the tool follows it; a change
//! the leader runs in the background is waited for by polling the
//! membership. Nothing is retried after a `Conflict`: the operator looks at
//! the new membership and decides again.
//!
//! Exit status: 0 done, 1 refused (a guardrail, a conflict, a rejected
//! identity), 2 usage error, 3 no node reachable, 4 a change accepted or
//! sent but not confirmed in time (it may still complete).

mod args;
mod drive;
mod link;
mod render;
#[cfg(test)]
mod tests;

use std::process::ExitCode;

pub use args::{ClusterArgs, ClusterCmd};

use drive::Operator;

/// Usage error (unreadable certificate or configuration files included),
/// as clap's own.
const EXIT_USAGE: u8 = 2;

pub fn run(args: &ClusterArgs) -> ExitCode {
    let settings = match args.common.resolve() {
        Ok(s) => s,
        Err(e) => {
            if args.common.json {
                println!(
                    "{}",
                    serde_json::json!({"ok": false, "exit": EXIT_USAGE, "kind": "usage", "error": e})
                );
            } else {
                eprintln!("beanstalkd-rs cluster: {e}");
            }
            return ExitCode::from(EXIT_USAGE);
        }
    };
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("beanstalkd-rs cluster: cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut op = Operator::new(&settings);
    let json = settings.json;
    let outcome = rt.block_on(async {
        match &args.command {
            ClusterCmd::Status => op.status().await.map(|r| {
                if json {
                    render::status_json(&r).to_string()
                } else {
                    render::status_text(&r)
                }
            }),
            cmd => op.change(cmd).await.map(|c| {
                if json {
                    render::changed_json(&c).to_string()
                } else {
                    render::changed_text(&c)
                }
            }),
        }
    });
    match outcome {
        Ok(out) => {
            if json {
                println!("{out}");
            } else {
                print!("{out}");
            }
            ExitCode::SUCCESS
        }
        Err(f) => {
            if json {
                println!("{}", render::fail_json(&f));
            } else {
                eprint!("{}", render::fail_text(&f));
            }
            ExitCode::from(f.exit_code())
        }
    }
}
