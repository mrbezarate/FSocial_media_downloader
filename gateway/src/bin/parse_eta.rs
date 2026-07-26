fn format_eta(eta: &str) -> Option<String> {
    let cleaned = eta.replace("~", "").replace("?", "");
    if cleaned.is_empty() {
        return None;
    }

    let parts: Vec<&str> = cleaned.split(':').collect();
    let mut result = String::new();

    if parts.len() == 3 {
        // HH:MM:SS
        let h = parts[0].parse::<u32>().unwrap_or(0);
        let m = parts[1].parse::<u32>().unwrap_or(0);
        let s = parts[2].parse::<u32>().unwrap_or(0);
        if h > 0 {
            result.push_str(&format!("{} ч ", h));
        }
        if m > 0 {
            result.push_str(&format!("{} мин ", m));
        }
        if s > 0 || (h == 0 && m == 0) {
            result.push_str(&format!("{} сек", s));
        }
    } else if parts.len() == 2 {
        // MM:SS
        let m = parts[0].parse::<u32>().unwrap_or(0);
        let s = parts[1].parse::<u32>().unwrap_or(0);
        if m > 0 {
            result.push_str(&format!("{} мин ", m));
        }
        if s > 0 || m == 0 {
            result.push_str(&format!("{} сек", s));
        }
    } else {
        return None;
    }

    Some(result.trim().to_string())
}

fn main() {
    println!("{:?}", format_eta("01:33"));
    println!("{:?}", format_eta("00:45"));
    println!("{:?}", format_eta("01:00:05"));
    println!("{:?}", format_eta("?"));
    println!("{:?}", format_eta("~02:00"));
}
