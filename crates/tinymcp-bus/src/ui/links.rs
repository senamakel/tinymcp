//! Links a tool result offers: payment pages, deep links, sign-in URLs.
//!
//! Found in the structured content and the text, classified, and capped. A
//! scheme that could run script or reach the host is blocked, and an image
//! asset is not an action.

use serde_json::Value;

use super::web_url::WebUrl;
use super::{LinkClass, UiLink, UiLinkKind};

/// The most links one result surfaces.
pub const MAX_LINKS: usize = 5;

const MAX_URL_LEN: usize = 2048;
const MAX_WALK_DEPTH: usize = 8;
const MAX_WALK_STRINGS: usize = 512;

const BLOCKED_SCHEMES: &[&str] = &[
    "javascript",
    "data",
    "vbscript",
    "file",
    "blob",
    "about",
    "tauri",
    "ipc",
    "asset",
    "ohwidget",
    "filesystem",
    "chrome",
    "chrome-extension",
    "view-source",
    "ws",
    "wss",
    "ftp",
    "openhuman",
];

const IMAGE_EXTENSIONS: &[&str] = &[
    "jpg", "jpeg", "png", "gif", "webp", "avif", "svg", "bmp", "ico", "heic", "heif", "tif", "tiff",
];

/// The longest scheme the scanner recognizes, in bytes.
const MAX_SCHEME_LEN: usize = 32;
/// Characters that end a URL in free text, besides whitespace.
const URL_TERMINATORS: &str = "\"'<>`{}|\\^[]";

/// How `url` is classified, by scheme and, for `http(s)`, by path.
#[must_use]
pub fn classify_link(url: &str) -> LinkClass {
    let Some((scheme, rest)) = url.split_once(':') else {
        return LinkClass::Blocked;
    };
    let scheme = scheme.to_ascii_lowercase();
    if scheme.is_empty()
        || !scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-'))
        || !scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        || BLOCKED_SCHEMES.contains(&scheme.as_str())
        || rest.trim().is_empty()
        || url.chars().any(char::is_control)
        || url.len() > MAX_URL_LEN
    {
        return LinkClass::Blocked;
    }
    match scheme.as_str() {
        "http" | "https" => match WebUrl::split(url) {
            Some(web) if image_path(web.path) => LinkClass::Image,
            Some(_) => LinkClass::Web,
            None => LinkClass::Blocked,
        },
        _ => LinkClass::Handoff,
    }
}

/// How a link of class `class` may be opened, or `None` when it is not an
/// action.
#[must_use]
pub fn link_kind(class: LinkClass) -> Option<UiLinkKind> {
    match class {
        LinkClass::Web => Some(UiLinkKind::External),
        LinkClass::Handoff => Some(UiLinkKind::Handoff),
        LinkClass::Image | LinkClass::Blocked => None,
    }
}

/// Whether an `http(s)` URL points at an image asset rather than a page.
#[must_use]
pub fn is_image_url(url: &str) -> bool {
    WebUrl::split(url).is_some_and(|web| image_path(web.path))
}

pub(super) fn image_path(path: &str) -> bool {
    let path = path.to_ascii_lowercase();
    if path.contains("/image/upload/") {
        return true;
    }
    path.rsplit_once('.')
        .is_some_and(|(_, ext)| !ext.contains('/') && IMAGE_EXTENSIONS.contains(&ext))
}

/// Every actionable link in `structured` and `text`, deduplicated, at most
/// [`MAX_LINKS`], structured content first.
#[must_use]
pub fn extract_links(structured: Option<&Value>, text: &str) -> Vec<UiLink> {
    let mut out: Vec<UiLink> = Vec::new();
    let mut strings = Vec::new();
    if let Some(value) = structured {
        collect_strings(value, 0, &mut strings);
    }
    strings.push(text);
    for haystack in strings {
        for found in url_spans(haystack) {
            if out.len() >= MAX_LINKS {
                return out;
            }
            let url = trim_trailing(found);
            let Some(kind) = link_kind(classify_link(url)) else {
                continue;
            };
            if out.iter().any(|link| link.url == url) {
                continue;
            }
            out.push(UiLink {
                url: url.to_string(),
                kind,
            });
        }
    }
    out
}

/// Every `scheme://rest` run in `haystack`, leftmost first and
/// non-overlapping: a scheme of 2 to [`MAX_SCHEME_LEN`] ASCII letters, digits,
/// `+`, `.` or `-` that starts with a letter and a word boundary, then `://`,
/// then everything up to whitespace or a [`URL_TERMINATORS`] character.
///
/// Only finds candidates; [`classify_link`] decides what each one is.
fn url_spans(haystack: &str) -> Vec<&str> {
    let mut spans = Vec::new();
    let mut at = 0;
    while let Some(next) = haystack[at..].chars().next() {
        let at_boundary = haystack[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !is_word(c));
        if at_boundary {
            if let Some(len) = url_len(&haystack[at..]) {
                spans.push(&haystack[at..at + len]);
                at += len;
                continue;
            }
        }
        at += next.len_utf8();
    }
    spans
}

/// The length of the URL `text` starts with, if it starts with one.
fn url_len(text: &str) -> Option<usize> {
    let scheme = text
        .bytes()
        .take_while(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'.' | b'-'))
        // Bounded, so text like `a.a.a.…` (a boundary every other byte) stays
        // linear rather than rescanning the run from every boundary.
        .take(MAX_SCHEME_LEN + 1)
        .count();
    if !(2..=MAX_SCHEME_LEN).contains(&scheme)
        || !text.as_bytes()[0].is_ascii_alphabetic()
        || !text[scheme..].starts_with("://")
    {
        return None;
    }
    let start = scheme + "://".len();
    let body: usize = text[start..]
        .chars()
        .take_while(|c| !c.is_whitespace() && !URL_TERMINATORS.contains(*c))
        .map(char::len_utf8)
        .sum();
    (body > 0).then_some(start + body)
}

/// A word character, for the boundary before a scheme.
fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn collect_strings<'a>(value: &'a Value, depth: usize, out: &mut Vec<&'a str>) {
    if depth > MAX_WALK_DEPTH || out.len() >= MAX_WALK_STRINGS {
        return;
    }
    match value {
        Value::String(text) => out.push(text),
        Value::Array(items) => items
            .iter()
            .for_each(|item| collect_strings(item, depth + 1, out)),
        Value::Object(map) => map
            .values()
            .for_each(|item| collect_strings(item, depth + 1, out)),
        _ => {}
    }
}

fn trim_trailing(url: &str) -> &str {
    let mut end = url.len();
    loop {
        let trimmed = &url[..end];
        let Some(last) = trimmed.chars().last() else {
            return trimmed;
        };
        let unbalanced_paren =
            last == ')' && trimmed.matches('(').count() < trimmed.matches(')').count();
        if matches!(last, '.' | ',' | ';' | ':' | '!' | '?' | '*') || unbalanced_paren {
            end -= last.len_utf8();
        } else {
            return trimmed;
        }
    }
}

#[cfg(test)]
#[path = "links_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "links_scan_tests.rs"]
mod scan_tests;
