use tracing::{error, info};

mod composers;
use composers::get_composers;

mod works;
use works::get_works;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    info!("Assuming we already retrieved list of composers.");
    // To output jsonl with composers
    // let _ = get_composers(); // outputs a composers.json file in jsonl format.

    info!("Let's retrieve 1 composer");
    // works
    let composer_name = "Igor Stravinsky"; // "Wolfgang_Amadeus_Mozart"; // "Ludwig_van_Beethoven"; // "Johann_Sebastian_Bach"; // "Giuseppe_Verdi";
    let _ = get_works(&composer_name).await;
}
