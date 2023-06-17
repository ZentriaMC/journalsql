use nom::{
    branch::alt,
    bytes::streaming::{tag, take_till, take_until},
    error::context,
    multi::length_data,
    number::complete::le_u64,
    sequence::pair,
    IResult,
};

#[cfg(feature = "bytes-as-base64")]
use base64::{engine::general_purpose::STANDARD as b64, Engine};

#[derive(Clone, Debug)]
pub struct JournalField {
    pub key: String,
    pub value: JournalFieldValue,
}

#[derive(Clone, Debug)]
pub enum JournalFieldValue {
    UTF8(String),
    Bytes(Vec<u8>),
}

impl From<&JournalFieldValue> for String {
    fn from(value: &JournalFieldValue) -> Self {
        match value {
            JournalFieldValue::UTF8(value) => value.clone(),

            #[cfg(feature = "bytes-as-base64")]
            JournalFieldValue::Bytes(value) => {
                let mut v = String::from("base64:");
                v += b64.encode(value).as_str();
                v
            }

            #[cfg(not(feature = "bytes-as-base64"))]
            JournalFieldValue::Bytes(value) => {
                String::from_utf8_lossy(&strip_ansi_escapes::strip(value)).to_string()
            }
        }
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for JournalFieldValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::UTF8(value) => serializer.serialize_str(value),

            #[cfg(feature = "bytes-as-base64")]
            Self::Bytes(value) => {
                let encoded = b64.encode(value);
                serializer.serialize_str(&encoded)
            }

            #[cfg(not(feature = "bytes-as-base64"))]
            Self::Bytes(value) => serializer.collect_seq(value.iter().map(|b| i32::from(*b))),
        }
    }
}

fn parse_utf8_value(input: &[u8]) -> IResult<&[u8], JournalFieldValue> {
    let (input, (_, line)) = pair(
        context("equals sign separator", tag(b"=")),
        context("contents until terminating newline", take_until("\n")),
    )(input)?;
    let utf8_line = unsafe { std::str::from_utf8_unchecked(line) }.to_string();

    Ok((input, JournalFieldValue::UTF8(utf8_line)))
}

fn parse_bytes_value(input: &[u8]) -> IResult<&[u8], JournalFieldValue> {
    let (input, (_, data)) = pair(
        context("newline separator", tag(b"\n")),
        context("binary data with size prefix", length_data(le_u64)),
    )(input)?;

    Ok((input, JournalFieldValue::Bytes(Vec::from(data))))
}

pub fn parse_journal_field(input: &[u8]) -> IResult<&[u8], JournalField> {
    let (input, raw_key) = context("field key", take_till(|b| b == b'=' || b == b'\n'))(input)?;
    let key = unsafe { std::str::from_utf8_unchecked(raw_key) }.to_string();

    let parse_either = alt((parse_utf8_value, parse_bytes_value));
    let mut parse_all = pair(parse_either, tag(b"\n"));

    let (input, (value, _)) = parse_all(input)?;

    Ok((input, JournalField { key, value }))
}
