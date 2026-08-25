use chrono::{DateTime, Datelike, Days, NaiveTime, TimeZone, Utc};
use chrono_tz::Tz;

use crate::error::ApiFailure;

pub fn parse_local_time(value: &str) -> Result<NaiveTime, ApiFailure> {
    NaiveTime::parse_from_str(value, "%H:%M:%S")
        .or_else(|_| NaiveTime::parse_from_str(value, "%H:%M"))
        .map_err(|_| ApiFailure::Invalid("local_time must be HH:MM or HH:MM:SS".into()))
}

pub fn next_occurrence(
    now: DateTime<Utc>,
    local_time: NaiveTime,
    time_zone: &str,
    weekdays: &[u8],
) -> Result<DateTime<Utc>, ApiFailure> {
    if weekdays.is_empty() || weekdays.iter().any(|day| *day > 6) {
        return Err(ApiFailure::Invalid(
            "weekdays must contain values from 0 (Sunday) through 6".into(),
        ));
    }
    let zone: Tz = time_zone
        .parse()
        .map_err(|_| ApiFailure::Invalid("time_zone must be an IANA zone".into()))?;
    let local_now = now.with_timezone(&zone);
    for offset in 0..=8 {
        let date = local_now
            .date_naive()
            .checked_add_days(Days::new(offset))
            .ok_or_else(|| ApiFailure::Invalid("date exceeds supported range".into()))?;
        if !weekdays.contains(&(date.weekday().num_days_from_sunday() as u8)) {
            continue;
        }
        let candidate = match zone.from_local_datetime(&date.and_time(local_time)) {
            chrono::LocalResult::Single(value) => value,
            chrono::LocalResult::Ambiguous(first, second) => first.min(second),
            chrono::LocalResult::None => continue,
        }
        .with_timezone(&Utc);
        if candidate > now {
            return Ok(candidate);
        }
    }
    Err(ApiFailure::Invalid(
        "no valid occurrence found in the next eight days".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selects_matching_weekday_after_now() {
        let now = "2026-08-24T12:00:00Z".parse().unwrap();
        let next = next_occurrence(
            now,
            parse_local_time("08:30").unwrap(),
            "America/Chicago",
            &[2],
        )
        .unwrap();
        assert_eq!(next.to_rfc3339(), "2026-08-25T13:30:00+00:00");
    }

    #[test]
    fn rejects_unknown_zone_and_invalid_weekday() {
        let now = Utc::now();
        assert!(
            next_occurrence(now, parse_local_time("08:30").unwrap(), "Mars/Base", &[1]).is_err()
        );
        assert!(next_occurrence(now, parse_local_time("08:30").unwrap(), "UTC", &[7]).is_err());
    }
}
