use nom::{
    branch::alt,
    bytes::streaming::{tag, take_till, take_until},
    error::context,
    multi::length_data,
    number::complete::le_u64,
    sequence::pair,
    IResult,
};

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
