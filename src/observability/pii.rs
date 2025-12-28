use regex::Regex;
use lazy_static::lazy_static;

lazy_static! {
    static ref EMAIL_REGEX: Regex = Regex::new(r"(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}").unwrap();
}

pub fn mask_pii(input: &str) -> String {
    EMAIL_REGEX.replace_all(input, "***@***.***").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_email() {
        let input = "Contact user@example.com for details";
        let masked = mask_pii(input);
        assert_eq!(masked, "Contact ***@***.*** for details");
    }

    #[test]
    fn test_no_pii() {
        let input = "System started normally";
        let masked = mask_pii(input);
        assert_eq!(masked, input);
    }
}
