use anyhow::Result;
use regex::Regex;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc,
};
use tracing::{error, info, warn};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RawCompositionData {
    pub composer_name: String,
    pub composer_url: String,
    pub source_url: String,
    pub table_index: usize,
    pub row_index: usize,
    pub headers: Vec<String>,
    pub cell_data: Vec<String>,
    pub cell_links: Vec<Option<String>>,
    pub raw_html_snippet: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Composition {
    pub composer_name: String,
    pub composer_url: String,
    pub source_url: String,
    pub title: String,
    pub work_url: Option<String>,
    pub year: Option<String>,
    pub key: Option<String>,
    pub opus: Option<String>,
    pub genre: Option<String>,
    pub catalog_number: Option<String>,
    pub instrumentation: Option<String>,
    pub duration: Option<String>,
    pub additional_info: HashMap<String, String>,
    pub raw_data: RawCompositionData, // Preserve original raw data
}

async fn raw_data_writer_task(
    mut receiver: mpsc::Receiver<RawCompositionData>,
    filename: &str,
) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true) // Start fresh for each composer
        .open(filename)
        .await?;

    let mut writer = BufWriter::new(file);

    while let Some(raw_data) = receiver.recv().await {
        let json_line = serde_json::to_string(&raw_data)?;
        writer.write_all(json_line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }

    writer.flush().await?;
    Ok(())
}

async fn composition_writer_task(
    mut receiver: mpsc::Receiver<Composition>,
    filename: &str,
) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(filename)
        .await?;

    let mut writer = BufWriter::new(file);

    while let Some(composition) = receiver.recv().await {
        let json_line = serde_json::to_string(&composition)?;
        writer.write_all(json_line.as_bytes()).await?;
        writer.write_all(b"\n").await?;
    }

    writer.flush().await?;
    Ok(())
}

fn determine_source_url(
    cells: &[scraper::ElementRef],
    a_selector: &Selector,
    page_url: &str,
    base_url: &str,
) -> String {
    // Strategy 1: Look for the most relevant link in the row
    // Priority order:
    // 1. Link in first cell (usually the title/work name)
    // 2. Any link that looks like it goes to a composition page
    // 3. Fallback to the page we're scraping from

    // Check first cell for links (highest priority)
    if let Some(first_cell) = cells.first() {
        if let Some(link) = first_cell.select(a_selector).next() {
            if let Some(href) = link.value().attr("href") {
                let full_url = if href.starts_with("/wiki/") {
                    format!("{}{}", base_url, href)
                } else {
                    href.to_string()
                };

                // Verify this looks like a composition/work page
                if is_likely_composition_url(&full_url) {
                    return full_url;
                }
            }
        }
    }

    // Strategy 2: Look through all cells for composition-related links
    for cell in cells {
        for link in cell.select(a_selector) {
            if let Some(href) = link.value().attr("href") {
                let full_url = if href.starts_with("/wiki/") {
                    format!("{}{}", base_url, href)
                } else {
                    href.to_string()
                };

                if is_likely_composition_url(&full_url) {
                    return full_url;
                }
            }
        }
    }

    // Strategy 3: Look for any Wikipedia link that's not obviously non-musical
    for cell in cells {
        for link in cell.select(a_selector) {
            if let Some(href) = link.value().attr("href") {
                let full_url = if href.starts_with("/wiki/") {
                    format!("{}{}", base_url, href)
                } else {
                    href.to_string()
                };

                // Avoid obviously non-musical links
                if !is_likely_non_composition_url(&full_url) {
                    return full_url;
                }
            }
        }
    }

    // Fallback: use the page we're scraping from
    page_url.to_string()
}

fn is_likely_composition_url(url: &str) -> bool {
    let url_lower = url.to_lowercase();

    // Positive indicators for composition URLs
    let composition_indicators = [
        "symphony",
        "sonata",
        "concerto",
        "quartet",
        "quintet",
        "trio",
        "prelude",
        "fugue",
        "etude",
        "nocturne",
        "waltz",
        "mazurka",
        "overture",
        "suite",
        "variation",
        "fantasia",
        "rhapsody",
        "mass",
        "requiem",
        "cantata",
        "oratorio",
        "opera",
        "song",
        "lied",
        "chanson",
        "aria",
        "duet",
        "movement",
        "piece",
        "work",
        "composition",
        "op.",
        "opus",
        "no.",
        "number",
        "k.",
        "bwv",
        "hob.",
        "woo",
    ];

    composition_indicators
        .iter()
        .any(|indicator| url_lower.contains(indicator))
}

fn is_likely_non_composition_url(url: &str) -> bool {
    let url_lower = url.to_lowercase();

    // Negative indicators (things that are definitely not compositions)
    let non_composition_indicators = [
        "category:",
        "file:",
        "template:",
        "user:",
        "talk:",
        "wikipedia:",
        "portal:",
        "help:",
        "special:",
        "list_of",
        "discography",
        "biography",
        "chronology",
        "timeline",
        "genre",
        "style",
        "period",
        "era",
        "instrument",
        "orchestra",
        "ensemble",
        "conservatory",
        "music_school",
        "university",
        "college",
    ];

    non_composition_indicators
        .iter()
        .any(|indicator| url_lower.contains(indicator))
}

fn extract_raw_table_data(
    table: scraper::ElementRef,
    composer_name: &str,
    composer_url: &str,
    page_url: &str,
    table_index: usize,
) -> Vec<RawCompositionData> {
    let th_selector = Selector::parse("th").unwrap();
    let tr_selector = Selector::parse("tr").unwrap();
    let td_selector = Selector::parse("td").unwrap();
    let a_selector = Selector::parse("a[href^=\"/wiki\"]").unwrap();
    let base_url = "https://en.wikipedia.org";

    let mut headers = Vec::new();
    let mut raw_data_list = Vec::new();

    // Find headers
    for row in table.select(&tr_selector) {
        let header_cells: Vec<_> = row.select(&th_selector).collect();
        if !header_cells.is_empty() {
            headers = header_cells
                .iter()
                .map(|cell| cell.text().collect::<String>().trim().to_string())
                .collect();
            break;
        }
    }

    // If no headers found, create generic ones
    if headers.is_empty() {
        // Count max columns in any row
        let max_cols = table
            .select(&tr_selector)
            .map(|row| row.select(&td_selector).count())
            .max()
            .unwrap_or(0);

        headers = (0..max_cols).map(|i| format!("column_{}", i)).collect();
    }

    // Extract data rows
    let mut row_index = 0;
    for row in table.select(&tr_selector) {
        let cells: Vec<_> = row.select(&td_selector).collect();
        if cells.is_empty() {
            continue;
        }

        let cell_data: Vec<String> = cells
            .iter()
            .map(|cell| cell.text().collect::<String>().trim().to_string())
            .collect();

        let cell_links: Vec<Option<String>> = cells
            .iter()
            .map(|cell| {
                cell.select(&a_selector)
                    .next()
                    .and_then(|a| a.value().attr("href"))
                    .map(|href| {
                        // Convert relative URLs to absolute URLs
                        if href.starts_with("/wiki/") {
                            format!("{}{}", base_url, href)
                        } else {
                            href.to_string()
                        }
                    })
            })
            .collect();

        let source_url = determine_source_url(&cells, &a_selector, page_url, base_url);

        // Get raw HTML snippet for debugging
        let raw_html_snippet = format!(
            "<tr>{}</tr>",
            cells
                .iter()
                .map(|cell| cell.html())
                .collect::<Vec<_>>()
                .join("")
        );

        let raw_data = RawCompositionData {
            composer_name: composer_name.to_string(),
            composer_url: composer_url.to_string(),
            source_url: source_url.to_string(),
            table_index,
            row_index,
            headers: headers.clone(),
            cell_data,
            cell_links,
            raw_html_snippet,
        };

        raw_data_list.push(raw_data);
        row_index += 1;
    }

    raw_data_list
}

// ------
struct FieldCanonicalizer {
    title_patterns: Vec<Regex>,
    year_patterns: Vec<Regex>,
    key_patterns: Vec<Regex>,
    opus_patterns: Vec<Regex>,
    genre_patterns: Vec<Regex>,
    catalog_patterns: Vec<Regex>,
    instrumentation_patterns: Vec<Regex>,
    duration_patterns: Vec<Regex>,
}

impl FieldCanonicalizer {
    fn new() -> Self {
        Self {
            title_patterns: vec![Regex::new(r"(?i)title|work|composition|piece|name").unwrap()],
            year_patterns: vec![Regex::new(r"(?i)year|date|composed|written|created").unwrap()],
            key_patterns: vec![Regex::new(r"(?i)key|tonality").unwrap()],
            opus_patterns: vec![
                Regex::new(r"(?i)opus|op\.?|work number|woo|bwv|k\.?|hob\.?").unwrap(),
            ],
            genre_patterns: vec![Regex::new(r"(?i)genre|type|form|category").unwrap()],
            catalog_patterns: vec![
                Regex::new(r"(?i)catalog|catalogue|cat\.?|thematic|index").unwrap(),
            ],
            instrumentation_patterns: vec![
                Regex::new(r"(?i)instrumentation|scoring|forces|ensemble|for").unwrap(),
            ],
            duration_patterns: vec![Regex::new(r"(?i)duration|length|time|minutes|mins").unwrap()],
        }
    }

    fn categorize_header(&self, header: &str) -> Option<&'static str> {
        let header_lower = header.to_lowercase();

        if self
            .title_patterns
            .iter()
            .any(|p| p.is_match(&header_lower))
        {
            Some("title")
        } else if self.year_patterns.iter().any(|p| p.is_match(&header_lower)) {
            Some("year")
        } else if self.key_patterns.iter().any(|p| p.is_match(&header_lower)) {
            Some("key")
        } else if self.opus_patterns.iter().any(|p| p.is_match(&header_lower)) {
            Some("opus")
        } else if self
            .genre_patterns
            .iter()
            .any(|p| p.is_match(&header_lower))
        {
            Some("genre")
        } else if self
            .catalog_patterns
            .iter()
            .any(|p| p.is_match(&header_lower))
        {
            Some("catalog_number")
        } else if self
            .instrumentation_patterns
            .iter()
            .any(|p| p.is_match(&header_lower))
        {
            Some("instrumentation")
        } else if self
            .duration_patterns
            .iter()
            .any(|p| p.is_match(&header_lower))
        {
            Some("duration")
        } else {
            None
        }
    }

    fn extract_year_from_text(&self, text: &str) -> Option<String> {
        let year_regex = Regex::new(r"\b(1[5-9]\d{2}|20[0-2]\d)\b").unwrap();
        year_regex.find(text).map(|m| m.as_str().to_string())
    }

    fn extract_key_from_text(&self, text: &str) -> Option<String> {
        let key_regex =
            Regex::new(r"\b([A-G](?:\s*(?:flat|sharp|♭|♯))?\s*(?:major|minor|Major|Minor))\b")
                .unwrap();
        key_regex.find(text).map(|m| m.as_str().to_string())
    }

    fn extract_opus_from_text(&self, text: &str) -> Option<String> {
        let opus_regex = Regex::new(r"\b(?:Op\.|Opus|op\.)\s*(\d+(?:\s*[a-z])?)\b").unwrap();
        opus_regex
            .captures(text)
            .and_then(|caps| caps.get(1))
            .map(|m| m.as_str().to_string())
    }
}

fn canonicalize_raw_data(raw_data: RawCompositionData) -> Composition {
    let canonicalizer = FieldCanonicalizer::new();

    let mut composition = Composition {
        composer_name: raw_data.composer_name.clone(),
        composer_url: raw_data.composer_url.clone(),
        source_url: raw_data.source_url.clone(),
        title: String::new(),
        work_url: None,
        year: None,
        key: None,
        opus: None,
        genre: None,
        catalog_number: None,
        instrumentation: None,
        duration: None,
        additional_info: HashMap::new(),
        raw_data: raw_data.clone(),
    };

    // Map headers to canonical fields
    let mut field_mappings: HashMap<&str, Vec<usize>> = HashMap::new();

    for (idx, header) in raw_data.headers.iter().enumerate() {
        if let Some(field) = canonicalizer.categorize_header(header) {
            field_mappings
                .entry(field)
                .or_insert_with(Vec::new)
                .push(idx);
        }
    }

    // Extract data based on mappings
    for (field, indices) in field_mappings {
        for &idx in &indices {
            if idx < raw_data.cell_data.len() {
                let cell_data = &raw_data.cell_data[idx];
                let cell_link = raw_data.cell_links.get(idx).and_then(|l| l.as_ref());

                match field {
                    "title" => {
                        if composition.title.is_empty() && !cell_data.is_empty() {
                            composition.title = cell_data.clone();
                            composition.work_url = cell_link.map(|s| s.to_string());
                        }
                    }
                    "year" => {
                        if composition.year.is_none() {
                            if let Some(year) = canonicalizer.extract_year_from_text(cell_data) {
                                composition.year = Some(year);
                            } else if !cell_data.is_empty() {
                                composition.year = Some(cell_data.clone());
                            }
                        }
                    }
                    "key" => {
                        if composition.key.is_none() {
                            if let Some(key) = canonicalizer.extract_key_from_text(cell_data) {
                                composition.key = Some(key);
                            } else if !cell_data.is_empty() {
                                composition.key = Some(cell_data.clone());
                            }
                        }
                    }
                    "opus" => {
                        if composition.opus.is_none() {
                            if let Some(opus) = canonicalizer.extract_opus_from_text(cell_data) {
                                composition.opus = Some(opus);
                            } else if !cell_data.is_empty() {
                                composition.opus = Some(cell_data.clone());
                            }
                        }
                    }
                    "genre" => {
                        if composition.genre.is_none() && !cell_data.is_empty() {
                            composition.genre = Some(cell_data.clone());
                        }
                    }
                    "catalog_number" => {
                        if composition.catalog_number.is_none() && !cell_data.is_empty() {
                            composition.catalog_number = Some(cell_data.clone());
                        }
                    }
                    "instrumentation" => {
                        if composition.instrumentation.is_none() && !cell_data.is_empty() {
                            composition.instrumentation = Some(cell_data.clone());
                        }
                    }
                    "duration" => {
                        if composition.duration.is_none() && !cell_data.is_empty() {
                            composition.duration = Some(cell_data.clone());
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // Fallback: if no title found, use first non-empty cell with a link
    if composition.title.is_empty() {
        for (idx, cell_data) in raw_data.cell_data.iter().enumerate() {
            if !cell_data.is_empty()
                && raw_data
                    .cell_links
                    .get(idx)
                    .and_then(|l| l.as_ref())
                    .is_some()
            {
                composition.title = cell_data.clone();
                composition.work_url = raw_data.cell_links[idx].clone();
                break;
            }
        }
    }

    // Store unmapped data in additional_info
    for (idx, (header, cell_data)) in raw_data
        .headers
        .iter()
        .zip(raw_data.cell_data.iter())
        .enumerate()
    {
        if canonicalizer.categorize_header(header).is_none() && !cell_data.is_empty() {
            composition
                .additional_info
                .insert(header.clone(), cell_data.clone());
        }
    }

    composition
}

pub async fn get_works(composer_name: &str) -> Result<()> {
    let base_wiki_url = "https://en.wikipedia.org";
    let compositions_url = format!(
        "{}/wiki/List_of_compositions_by_{}",
        base_wiki_url,
        composer_name.replace(" ", "_")
    );

    let composer_url = format!("{}/wiki/{}", base_wiki_url, composer_name.replace(" ", "_"));

    info!(
        "Fetching works for {} from {}",
        composer_name, compositions_url
    );

    let response = reqwest::get(&compositions_url).await?;
    let html = response.text().await?;
    let document = Html::parse_document(&html);

    let table_selector = Selector::parse("table").unwrap();

    // Stage 1: Extract and save raw data
    let raw_filename = format!("raw-info-{}.json", composer_name.replace(" ", "_"));
    let (raw_tx, raw_rx) = mpsc::channel::<RawCompositionData>(100);

    let raw_filename_clone = raw_filename.clone();
    let raw_writer_handle =
        tokio::spawn(async move { raw_data_writer_task(raw_rx, &raw_filename_clone).await });

    let mut all_raw_data = Vec::new();

    // Process all tables on the page
    for (table_index, table) in document.select(&table_selector).enumerate() {
        let raw_data_list = extract_raw_table_data(
            table,
            composer_name,
            &composer_url,
            &compositions_url,
            table_index,
        );

        for raw_data in raw_data_list {
            // Send to raw data writer
            if let Err(e) = raw_tx.send(raw_data.clone()).await {
                error!("Error sending raw data through channel: {}", e);
            }
            all_raw_data.push(raw_data);
        }
    }

    drop(raw_tx);
    raw_writer_handle.await??;

    info!(
        "Saved {} raw composition records to {}",
        all_raw_data.len(),
        raw_filename
    );

    // Stage 2: Canonicalize and save processed compositions
    let (comp_tx, comp_rx) = mpsc::channel::<Composition>(100);
    let comp_writer_handle =
        tokio::spawn(async move { composition_writer_task(comp_rx, "compositions.json").await });

    let mut canonicalized_count = 0;
    for raw_data in all_raw_data {
        let composition = canonicalize_raw_data(raw_data);

        // Only save compositions with meaningful titles
        if !composition.title.is_empty() && composition.title.len() > 2 {
            if let Err(e) = comp_tx.send(composition).await {
                error!("Error sending composition through channel: {}", e);
            } else {
                canonicalized_count += 1;
            }
        }
    }

    drop(comp_tx);
    comp_writer_handle.await??;

    info!(
        "Canonicalized and saved {} compositions to compositions.json",
        canonicalized_count
    );

    Ok(())
}

// Helper function to process raw data files later if needed
pub async fn reprocess_raw_data(raw_filename: &str) -> Result<Vec<Composition>> {
    use tokio::fs::File;
    use tokio::io::{AsyncBufReadExt, BufReader};

    let file = File::open(raw_filename).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut compositions = Vec::new();

    while let Some(line) = lines.next_line().await? {
        if let Ok(raw_data) = serde_json::from_str::<RawCompositionData>(&line) {
            let composition = canonicalize_raw_data(raw_data);
            if !composition.title.is_empty() {
                compositions.push(composition);
            }
        }
    }

    Ok(compositions)
}
