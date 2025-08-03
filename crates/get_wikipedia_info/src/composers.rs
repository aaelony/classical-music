// use reqwest;
use anyhow::Result;
use regex;
use scraper::{Html, Selector};
use serde::Serialize;
use std::fmt;
use tracing::{error, info};
// use tracing_subscriber::fmt::init;

use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
};

// -----
#[derive(Debug, PartialEq)]
struct ParsedYears {
    birth_year: Option<i32>,
    death_year: Option<i32>,
    approximate: bool,
    flourished: bool,
}

#[derive(Debug, PartialEq, Serialize, Clone)]
enum QualityOfYearInfo {
    Exact,
    Approximate,
    Flourished,
    AliveToday,
}
impl fmt::Display for QualityOfYearInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            QualityOfYearInfo::Exact => "Exact",
            QualityOfYearInfo::Approximate => "Approximate",
            QualityOfYearInfo::Flourished => "Flourished",
            QualityOfYearInfo::AliveToday => "Alive Today",
        };
        write!(f, "{}", s)
    }
}
// -----

#[derive(Serialize, Clone)]
struct Composer {
    pub url: String,
    pub full_name: String,
    // pub years_info: Option<String>,
    pub list_of_compositions_url: String,
    //pub last_name: Option<String>,
    //pub first_name: Option<String>,
    pub birth_year: Option<i32>,
    pub death_year: Option<i32>,
    pub years_qualifier: QualityOfYearInfo,
}

async fn composer_writer_task(
    mut receiver: mpsc::Receiver<Composer>,
    filename: &str,
) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(filename)
        .await?;

    let mut writer = BufWriter::new(file);

    while let Some(composer) = receiver.recv().await {
        let json_line = serde_json::to_string(&composer)?;
        writer.write_all(json_line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }

    writer.flush().await?;
    Ok(())
}

async fn write_composers_via_channel(
    composers: Vec<Composer>,
    filename: &str,
) -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel::<Composer>(100);

    let filename_owned = filename.to_string();
    let writer_handle =
        tokio::spawn(async move { composer_writer_task(rx, &filename_owned).await });

    // Send composers through the channel
    for composer in composers {
        tx.send(composer)
            .await
            .map_err(|_| anyhow::anyhow!("Error trying to send Composer through Channel"))?;
    }

    drop(tx);

    writer_handle.await??;
    Ok(())
}

// fn extract_years_from_parentheses(text: &str) -> Option<String> {
//     if let Some(start) = text.find('(') {
//         if let Some(end) = text[start..].find(')') {
//             let years = &text[start + 1..start + end];
//             if !years.trim().is_empty() {
//                 return Some(years.trim().to_string());
//             }
//         }
//     }
//     None
// }

fn parse_year_range(s: &str, approximate: bool, flourished: bool) -> Option<ParsedYears> {
    let re = regex::Regex::new(r"(?i)(\d{3,4})\s*[-–]\s*(\d{3,4})").ok()?;
    if let Some(caps) = re.captures(s) {
        let birth = caps.get(1)?.as_str().parse::<i32>().ok()?;
        let death = caps.get(2)?.as_str().parse::<i32>().ok()?;
        Some(ParsedYears {
            birth_year: Some(birth),
            death_year: Some(death),
            approximate,
            flourished,
        })
    } else {
        // Try single year
        let re_single = regex::Regex::new(r"(?i)(\d{3,4})").ok()?;
        if let Some(cap) = re_single.captures(s) {
            let birth = cap.get(1)?.as_str().parse::<i32>().ok()?;
            Some(ParsedYears {
                birth_year: Some(birth),
                death_year: None,
                approximate,
                flourished,
            })
        } else {
            None
        }
    }
}

fn extract_years_from_parentheses(text: &str) -> Option<ParsedYears> {
    if let Some(start) = text.find('(') {
        if let Some(end) = text[start..].find(')') {
            let years = &text[start + 1..start + end];
            let trimmed = years.trim();

            // Normalize and lowercase for easier matching
            let normalized = trimmed.to_lowercase();

            match normalized.as_str() {
                s if s.starts_with("c.") || s.starts_with("c ") => {
                    parse_year_range(&normalized, true, false)
                }
                s if s.starts_with("fl.") || s.starts_with("fl ") => {
                    parse_year_range(&normalized, false, true)
                }
                s if s.starts_with("born ") => {
                    let year = s[5..].trim().parse::<i32>().ok()?;
                    Some(ParsedYears {
                        birth_year: Some(year),
                        death_year: None,
                        approximate: false,
                        flourished: false,
                    })
                }
                s => parse_year_range(s, false, false),
            }
        } else {
            None
        }
    } else {
        None
    }
}

async fn read_parse(url: &str) -> Result<Vec<Composer>> {
    let response = reqwest::get(url).await?;
    let html = response.text().await?;
    let document = Html::parse_document(&html);
    let li_selector = Selector::parse("li").unwrap();
    let a_selector = Selector::parse("a[href^=\"/wiki\"][title]").unwrap();

    let composers: Vec<Composer> = document
        .select(&li_selector)
        .filter_map(|li_element| {
            // Find the anchor tag with href starting with "/wiki" and title attribute
            if let Some(anchor) = li_element.select(&a_selector).next() {
                if let (Some(title), Some(href)) =
                    (anchor.value().attr("title"), anchor.value().attr("href"))
                {
                    // let anchor_text = anchor.text().collect::<String>();

                    // // Check if title matches the anchor text content
                    // if title == anchor_text {
                    //     // Extract years info from parentheses in the li element
                    //     let li_text = li_element.text().collect::<String>();
                    //     let years_info = extract_years_from_parentheses(&li_text).unwrap();

                    //     let birth_year = years_info.birth_year;
                    //     let death_year = years_info.death_year;

                    //     let years_qualifier = if years_info.approximate {
                    //         QualityOfYearInfo::Approximate
                    //     } else if years_info.flourished {
                    //         QualityOfYearInfo::Flourished
                    //     } else if years_info.death_year.is_none() {
                    //         QualityOfYearInfo::AliveToday
                    //     } else {
                    //         QualityOfYearInfo::Exact
                    //     };

                    //     let list_of_compositions_url =
                    //         format!("/wiki/List_of_compositions_by_{}", title.to_string())
                    //             .replace(" ", "_");

                    //     return Some(Composer {
                    //         full_name: title.to_string(),
                    //         birth_year,
                    //         death_year,
                    //         years_qualifier,
                    //         url: href.to_string(),
                    //         list_of_compositions_url,
                    //     });
                    // }

                    let anchor_text = anchor.text().collect::<String>();

                    // Check if title matches the anchor text content
                    if title == anchor_text {
                        // Extract years info from parentheses in the li element
                        let li_text = li_element.text().collect::<String>();

                        if let Some(years_info) = extract_years_from_parentheses(&li_text) {
                            let birth_year = years_info.birth_year;
                            let death_year = years_info.death_year;

                            let years_qualifier = if years_info.approximate {
                                QualityOfYearInfo::Approximate
                            } else if years_info.flourished {
                                QualityOfYearInfo::Flourished
                            } else if years_info.death_year.is_none() {
                                QualityOfYearInfo::AliveToday
                            } else {
                                QualityOfYearInfo::Exact
                            };

                            let list_of_compositions_url =
                                format!("/wiki/List_of_compositions_by_{}", title.to_string())
                                    .replace(" ", "_");

                            return Some(Composer {
                                full_name: title.to_string(),
                                birth_year,
                                death_year,
                                years_qualifier,
                                url: href.to_string(),
                                list_of_compositions_url,
                            });
                        } else {
                            // Handle composers without year information
                            let list_of_compositions_url =
                                format!("/wiki/List_of_compositions_by_{}", title.to_string())
                                    .replace(" ", "_");

                            return Some(Composer {
                                full_name: title.to_string(),
                                birth_year: None,
                                death_year: None,
                                years_qualifier: QualityOfYearInfo::AliveToday, // Default assumption
                                url: href.to_string(),
                                list_of_compositions_url,
                            });
                        }
                    }
                }
            }
            None
        })
        .collect();

    // <li><a href="/wiki/Clamor_Heinrich_Abel" title="Clamor Heinrich Abel">Clamor Heinrich Abel</a> (1634–1696)</li>

    Ok(composers)
}

pub async fn get_composers() {
    let url = "https://en.wikipedia.org/wiki/List_of_composers_by_name";
    let jsonl_output_filename = "composers.json";

    match read_parse(url).await {
        Ok(composers) => {
            info!("Found {} <li> elements:", composers.len());

            if let Err(e) =
                write_composers_via_channel(composers.clone(), jsonl_output_filename).await
            {
                error!(
                    "Error writing composers to file ({}): {}",
                    jsonl_output_filename, e
                );
            }

            info!("... There are {} composers", composers.len());
            // let n = 20;

            // for (index, composer) in composers.iter().take(n).enumerate() {
            //     info!(
            //         "{}. {} - {} ({} to {}, {})",
            //         index + 1,
            //         composer.full_name,
            //         composer.url,
            //         composer.birth_year.unwrap(),
            //         composer.death_year,
            //         composer.years_qualifier,
            //         //composer.years_info.as_deref().unwrap_or("no years")
            //     );

            //     if composers.len() > n {
            //         info!("... and {} more elements", composers.len() - n);
            //     }
            // }
        }
        Err(e) => error!("Error fetching li elements: {}", e),
    }
}
