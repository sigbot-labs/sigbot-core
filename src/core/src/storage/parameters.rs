//! Shared named bindings; quoted SQL, comments and PostgreSQL casts remain literal.
use anyhow::{bail, ensure, Result};
use sigbot_utils::types::GenericValue;
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq)]
pub(super) enum ParameterStyle {
    Postgres,
    Sqlite,
}

pub(super) struct SqlParameters<'a> {
    pub statement: String,
    pub values: Vec<&'a GenericValue>,
}

impl<'a> SqlParameters<'a> {
    pub fn compile(
        template: &str,
        parameters: &'a HashMap<String, GenericValue>,
        style: ParameterStyle,
    ) -> Result<Self> {
        let input = template.as_bytes();
        let mut result = Self {
            statement: String::with_capacity(template.len()),
            values: Vec::new(),
        };
        let (mut cursor, mut copied) = (0, 0);
        while cursor < input.len() {
            match input[cursor] {
                b'\'' | b'"' | b'`' => {
                    let escaped = style == ParameterStyle::Postgres
                        && input[cursor] == b'\''
                        && cursor > 0
                        && matches!(input[cursor - 1], b'e' | b'E')
                        && (cursor < 2 || !Self::identifier(input[cursor - 2]));
                    cursor = Self::quoted(input, cursor, input[cursor], escaped)?;
                }
                b'[' if style == ParameterStyle::Sqlite => {
                    cursor = template[cursor + 1..]
                        .find(']')
                        .map(|end| cursor + end + 2)
                        .ok_or_else(|| anyhow::anyhow!("unterminated SQL identifier"))?;
                }
                b'-' if input.get(cursor + 1) == Some(&b'-') => {
                    cursor = template[cursor..]
                        .find('\n')
                        .map(|end| cursor + end)
                        .unwrap_or(input.len());
                }
                b'/' if input.get(cursor + 1) == Some(&b'*') => {
                    let mut depth = 1;
                    cursor += 2;
                    while cursor + 1 < input.len() && depth > 0 {
                        match &input[cursor..cursor + 2] {
                            b"/*" => {
                                depth += 1;
                                cursor += 2;
                            }
                            b"*/" => {
                                depth -= 1;
                                cursor += 2;
                            }
                            _ => cursor += 1,
                        }
                    }
                    ensure!(depth == 0, "unterminated SQL comment");
                }
                b':' if input.get(cursor + 1) == Some(&b':') => cursor += 2,
                b':' | b'$' => {
                    let start = cursor;
                    if input[cursor] == b'$' && style == ParameterStyle::Postgres {
                        let mut end = cursor + 1;
                        if input.get(end).is_some_and(|value| Self::initial(*value)) {
                            while input.get(end).is_some_and(|value| Self::identifier(*value)) {
                                end += 1;
                            }
                        }
                        if input.get(end) == Some(&b'$') && (start == 0 || !Self::identifier(input[start - 1])) {
                            let delimiter = &template[start..=end];
                            cursor = template[end + 1..]
                                .find(delimiter)
                                .map(|offset| end + 1 + offset + delimiter.len())
                                .ok_or_else(|| anyhow::anyhow!("unterminated dollar-quoted SQL"))?;
                            continue;
                        }
                    }
                    cursor += 1;
                    if !input.get(cursor).is_some_and(|value| Self::initial(*value))
                        || (input[start] == b'$' && start > 0 && Self::identifier(input[start - 1]))
                    {
                        continue;
                    }
                    while input.get(cursor).is_some_and(|value| Self::identifier(*value)) {
                        cursor += 1;
                    }
                    let name = &template[start + 1..cursor];
                    let Some(value) = parameters.get(name) else {
                        bail!("missing named SQL parameter: {name}");
                    };
                    result.statement.push_str(&template[copied..start]);
                    result.values.push(value);
                    match style {
                        ParameterStyle::Postgres => result.statement.push_str(&format!("${}", result.values.len())),
                        ParameterStyle::Sqlite => result.statement.push('?'),
                    }
                    copied = cursor;
                }
                _ => cursor += 1,
            }
        }
        result.statement.push_str(&template[copied..]);
        Ok(result)
    }

    fn initial(value: u8) -> bool {
        value.is_ascii_alphabetic() || value == b'_'
    }
    fn identifier(value: u8) -> bool {
        Self::initial(value) || value.is_ascii_digit()
    }

    fn quoted(input: &[u8], start: usize, quote: u8, escaped: bool) -> Result<usize> {
        let mut cursor = start + 1;
        while cursor < input.len() {
            if escaped && input[cursor] == b'\\' {
                cursor += 2;
                continue;
            }
            if input[cursor] == quote {
                if input.get(cursor + 1) == Some(&quote) {
                    cursor += 2;
                    continue;
                }
                return Ok(cursor + 1);
            }
            cursor += 1;
        }
        bail!("unterminated SQL quote")
    }
}

#[cfg(test)]
#[path = "../../tests/storage/parameters_test.rs"]
mod tests;
