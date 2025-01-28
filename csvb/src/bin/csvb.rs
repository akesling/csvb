use std::collections::BTreeSet;

use anyhow::{bail, Context as _, Result};
use clap::Parser;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None, arg_required_else_help = true)]
struct Args {
    #[command(subcommand)]
    command: Command,

    /// Log level pairs of the form <MODULE>:<LEVEL>.
    #[arg(long)]
    log_levels: Option<Vec<String>>,

    /// Max number of bytes available for memory pool, defaults to 10GiB
    #[arg(long, default_value_t = 10737418240)]
    memory_pool_bytes: usize,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Print a random haiku
    Haiku(HaikuOptions),
    /// Print a random haiku
    Cmd(CmdOptions),
}

#[derive(clap::Parser, Debug)]
struct HaikuOptions {
    /// Print all haikus
    #[arg(long)]
    all: bool,
}

#[derive(clap::Parser, Debug)]
struct CmdOptions {
    /// Source CSV files for command
    #[arg(long)]
    csv: Vec<String>,
}

/// Convert a series of <MODULE>:<LEVEL> pairs into actionable `(module, LevelFilter)` pairs
fn as_level_pairs(config: &[String]) -> Result<Vec<(&str, simplelog::LevelFilter)>> {
    let mut pairs = Vec::with_capacity(config.len());
    for c in config {
        let tokens: Vec<&str> = c.split(":").collect();
        if tokens.len() != 2 {
            bail!("Flag config pair was not of the form <MODULE>:<LEVEL>: '{c}'");
        }
        pairs.push((
            tokens[0],
            match tokens[1].to_lowercase().as_str() {
                "trace" => simplelog::LevelFilter::Trace,
                "debug" => simplelog::LevelFilter::Debug,
                "info" => simplelog::LevelFilter::Info,
                "warn" => simplelog::LevelFilter::Warn,
                "error" => simplelog::LevelFilter::Error,
                _ => bail!("Unrecognized level name in '{c}'"),
            },
        ))
    }

    Ok(pairs)
}

fn initialize_logging(
    module_path_filters: &[(&str, simplelog::LevelFilter)],
) -> anyhow::Result<()> {
    simplelog::CombinedLogger::init(
        module_path_filters
            .iter()
            .map(|(module_path_filter, level)| {
                simplelog::TermLogger::new(
                    *level,
                    simplelog::ConfigBuilder::new()
                        .add_filter_allow(module_path_filter.to_string())
                        .build(),
                    simplelog::TerminalMode::Mixed,
                    simplelog::ColorChoice::Auto,
                ) as Box<dyn simplelog::SharedLogger>
            })
            .collect(),
    )
    .map_err(|e| e.into())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let levels_arg: Option<Vec<String>> = args.log_levels;

    let log_levels: Vec<(&str, simplelog::LevelFilter)> = {
        let mut log_levels = BTreeSet::from([
            ("csvb", simplelog::LevelFilter::Debug),
            // If compiled via Bazel, "csvb" binary crate module will be named "bin"
            ("bin", simplelog::LevelFilter::Debug),
        ]);

        for level in levels_arg
            .as_deref()
            .map(as_level_pairs)
            .unwrap_or(Ok(vec![]))
            .context("Log level override parsing failed")?
        {
            log_levels.insert(level);
        }
        log_levels.into_iter().collect()
    };
    let _ = initialize_logging(&log_levels[..]);
    log::trace!("Logging initialized, commands parsed...");

    match args.command {
        Command::Haiku(options) => csvb::print_haiku(options.all),
        Command::Cmd(options) => {
            csvb::run_cmd(
                &csvb::CmdOptions {
                    memory_limit_bytes: args.memory_pool_bytes,
                },
                options.csv,
            )
            .await?
        }
    }
    Ok(())
}
