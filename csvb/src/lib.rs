use anyhow::bail;

pub use csvb_engine as engine;

pub static HAIKUS: [[&str; 3]; 10] = [
    [
        "Columns fall like rain,",
        "Rows unroll in endless streams —",
        "Order from chaos.",
    ],
    [
        "Comma on the loose,",
        "Sharp precision, splitting words —",
        "Language chopped to bits.",
    ],
    [
        "Rows become columns,",
        "Twisting, spinning, transforming —",
        "New perspectives bloom.",
    ],
    [
        "Malformed CSV,",
        "Spaces, quotes, and hidden tabs —",
        "Parser's nightmare realm.",
    ],
    [
        "Count, sum, group by joy,",
        "Data folds to tell its tale —",
        "Metrics rule the world.",
    ],
    [
        "Columns never named,",
        "Yet their whispers fill the air —",
        "Untold truths await.",
    ],
    [
        "Files collide with glee,",
        "VLOOKUP and JOIN entwine —",
        "One table reborn.",
    ],
    [
        "CSV’s raw heart,",
        "Plotted as a vivid bloom —",
        "Insight takes its root.",
    ],
    [
        "Scroll forevermore,",
        "Endless sheets of raw data —",
        "Time lost in numbers.",
    ],
    [
        "Data distilled pure;",
        "Saved, .csv treasure —",
        "A programmer’s peace.",
    ],
];

/// Print a random CSV-related haiku
pub fn print_haiku(print_all: bool) {
    use rand::seq::SliceRandom as _;

    println!("line 1: line 2: line 3");
    if print_all {
        for h in HAIKUS {
            println!("{}", h.join(":"))
        }
    } else {
        let mut rng = rand::thread_rng();
        println!(
            "{}",
            HAIKUS
                .choose(&mut rng)
                .expect("at least one haiku")
                .join(":")
        )
    }
}

pub struct CmdOptions {
    /// The number of bytes the command memory pool should be limited to
    pub memory_limit_bytes: usize,
}

pub async fn run_cmd(options: &CmdOptions, sources: &[String], sql: &str) -> anyhow::Result<()> {
    use futures::stream::StreamExt as _;

    if sources.is_empty() {
        bail!("No sources provided when running command")
    }

    // TODO(alex): Create UDF to print haiku
    let mut engine = engine::CsvbCore::new(sources, options.memory_limit_bytes).await?;
    let mut stream = engine.execute(sql).await?;
    let mut batches = Vec::new();
    while let Some(items) = stream.next().await {
        batches.push(items?);
    }

    //while let Some(batch) = stream.next().await {
    //    let pretty_results = arrow::util::pretty::pretty_format_batches(&[items?])?.to_string();
    //    println!("Results:\n{}", pretty_results);
    //}

    let pretty_results = arrow::util::pretty::pretty_format_batches(&batches[..])?.to_string();
    println!("Results:\n{}", pretty_results);
    Ok(())
}
