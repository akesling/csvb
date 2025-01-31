use csvb_engine as engine;
pub use engine::{run_cmd, CmdOptions};

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

// TODO(alex): Create UDF to print haiku
