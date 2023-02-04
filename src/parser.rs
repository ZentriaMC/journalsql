use std::io::Read;

use crossbeam_channel::Sender;
use log::{debug, trace};
use nom::{
    branch::alt,
    bytes::streaming::{tag, take_till, take_until},
    error::context,
    multi::length_data,
    number::complete::le_u64,
    sequence::pair,
    IResult,
};

use crate::{
    journal::{JournalEntry, JournalField, JournalFieldData},
    metrics,
    util::measure,
};

#[derive(Debug, thiserror::Error)]
pub enum JournalReadError {
    #[error("I/O error")]
    IOError(std::io::Error),

    #[error("Parse error")]
    ParseError(nom::error::ErrorKind, Vec<u8>),
}

fn parse_utf8_value(input: &[u8]) -> IResult<&[u8], JournalFieldData> {
    let (input, (_, line)) = pair(
        context("equals sign separator", tag(b"=")),
        context("contents until terminating newline", take_until("\n")),
    )(input)?;
    let utf8_line = unsafe { std::str::from_utf8_unchecked(line) }.to_string();

    Ok((input, JournalFieldData::UTF8(utf8_line)))
}

fn parse_bytes_value(input: &[u8]) -> IResult<&[u8], JournalFieldData> {
    let (input, (_, data)) = pair(
        context("newline separator", tag(b"\n")),
        context("binary data with size prefix", length_data(le_u64)),
    )(input)?;

    Ok((input, JournalFieldData::Bytes(Vec::from(data))))
}

pub fn parse_journal_field(input: &[u8]) -> IResult<&[u8], JournalField> {
    let (input, raw_key) = context("field key", take_till(|b| b == b'=' || b == b'\n'))(input)?;
    let key = unsafe { std::str::from_utf8_unchecked(raw_key) }.to_string();

    let parse_either = alt((parse_utf8_value, parse_bytes_value));
    let mut parse_all = pair(parse_either, tag(b"\n"));

    let (input, (value, _)) = parse_all(input)?;

    Ok((input, JournalField { key, value }))
}

pub fn read_journal_entries(
    reader: &mut (dyn std::io::Read),
    sender: Sender<JournalEntry>,
) -> Result<(), JournalReadError> {
    let mut current_entry = JournalEntry::default();
    let mut input = Vec::with_capacity(8192);

    const READ_STEP: usize = 1;

    loop {
        let (elapsed, parse_result) = measure(|| parse_journal_field(&input));
        metrics::set_last_entry_parse_time(elapsed).unwrap();

        let to_read = match parse_result {
            Ok((remaining, parsed)) => {
                trace!("processed={:?}", parsed);

                let remaining = Vec::from(remaining);
                input.truncate(remaining.len());
                input.extend(&remaining);

                current_entry.put(parsed.key, parsed.value);

                READ_STEP
            }
            Err(nom::Err::Incomplete(nom::Needed::Unknown)) => READ_STEP,
            Err(nom::Err::Incomplete(nom::Needed::Size(sz))) => sz.get(),
            Err(nom::Err::Error(e)) => {
                if e.code == nom::error::ErrorKind::Eof {
                    // If we've hit an eof and have only newline in the buffer, then it's end of the journal entry
                    if input.len() == 1 && input[0] == b'\n' {
                        if let Err(err) = sender.send(current_entry) {
                            debug!("producer channel closed: {:?}", err);
                            break;
                        }

                        current_entry = JournalEntry::default();
                        input.truncate(0);
                    }

                    READ_STEP
                } else {
                    return Err(JournalReadError::ParseError(e.code, e.input.to_owned()));
                }
            }
            Err(nom::Err::Failure(e)) => {
                return Err(JournalReadError::ParseError(e.code, e.input.to_owned()));
            }
        };

        reader
            .take(to_read as u64)
            .read_to_end(&mut input)
            .map_err(JournalReadError::IOError)?;

        if input.is_empty() {
            break;
        }
    }

    Ok(())
}
