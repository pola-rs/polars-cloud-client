use crate::MAX_SUBTITLE_LENGTH;

/// Truncate a list of elements with lazy evaluation.
/// The closure is only called for elements that might fit in the result.
pub fn truncate_elements_lazy<T>(
    elements: impl Iterator<Item = T>,
    to_string: impl Fn(T) -> String,
) -> Option<String> {
    let mut result = String::new();
    let separator = ", ";
    let suffix = "...";

    for item in elements {
        let elem = to_string(item);
        if result.is_empty() {
            if elem.len() + separator.len() + suffix.len() > MAX_SUBTITLE_LENGTH {
                return truncate_subtitle(&elem);
            }
            result.push_str(&elem);
        } else {
            let needed =
                result.len() + separator.len() + elem.len() + separator.len() + suffix.len();
            if needed <= MAX_SUBTITLE_LENGTH {
                result.push_str(separator);
                result.push_str(&elem);
            } else {
                result.push_str(separator);
                result.push_str(suffix);
                return Some(result);
            }
        }
    }

    if result.is_empty() {
        None
    } else {
        Some(result)
    }
}

pub fn truncate_subtitle(subtitle: &str) -> Option<String> {
    if subtitle.is_empty() {
        return None;
    }
    if subtitle.len() > MAX_SUBTITLE_LENGTH {
        Some(format!("{}...", &subtitle[..MAX_SUBTITLE_LENGTH - 3]))
    } else {
        Some(subtitle.to_string())
    }
}
