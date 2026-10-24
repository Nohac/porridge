//! Gate Linux perf around measured work, excluding setup and verification.
use std::{env, fs::{File, OpenOptions}, io::{BufRead, BufReader, Write}};

pub struct PerfControl {
    control: File,
    acknowledgement: BufReader<File>,
}

impl PerfControl {
    pub fn open() -> std::io::Result<Option<Self>> {
        match (
            env::var_os("PROFILE_PERF_CONTROL"),
            env::var_os("PROFILE_PERF_ACK"),
        ) {
            (Some(control), Some(ack)) => Ok(Some(Self {
                control: OpenOptions::new().write(true).open(control)?,
                acknowledgement: BufReader::new(File::open(ack)?),
            })),
            (None, None) => Ok(None),
            _ => Err(std::io::Error::other(
                "set both PROFILE_PERF_CONTROL and PROFILE_PERF_ACK",
            )),
        }
    }

    pub fn command(&mut self, command: &str) -> std::io::Result<()> {
        writeln!(self.control, "{command}")?;
        self.control.flush()?;
        let mut response = String::new();
        self.acknowledgement.read_line(&mut response)?;
        // Some perf versions include a trailing NUL after the newline.
        if response.trim_matches(|ch: char| ch.is_whitespace() || ch == '\0') != "ack" {
            return Err(std::io::Error::other(format!(
                "unexpected perf acknowledgement: {response:?}"
            )));
        }
        Ok(())
    }
}
