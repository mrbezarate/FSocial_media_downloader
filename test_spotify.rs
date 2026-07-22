use regex::Regex;

fn main() {
    let html = r#"<meta property="og:title" content="Never Gonna Give You Up"/><meta property="og:description" content="Rick Astley · Whenever You Need Somebody · Song · 1987"/>"#;
    let title_re = Regex::new(r#"<meta property="og:title" content="([^"]+)"\s*/?>"#).unwrap();
    let desc_re = Regex::new(r#"<meta property="og:description" content="([^"]+)"\s*/?>"#).unwrap();

    let title = title_re.captures(&html).and_then(|c| c.get(1)).map(|m| m.as_str().to_string()).unwrap_or_else(|| "Unknown".to_string());
    let desc = desc_re.captures(&html).and_then(|c| c.get(1)).map(|m| m.as_str().to_string()).unwrap_or_default();
    println!("Title: {}", title);
    println!("Desc: {}", desc);
}
