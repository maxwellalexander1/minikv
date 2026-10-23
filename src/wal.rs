use crc32fast::Hasher;

pub const OP_SET: u8 = 1;
pub const OP_DEL: u8 = 2;

pub enum Record {
    Set { key: String, value: Vec<u8>, expires_at_ms: Option<u64> },
    Del { key: String },
}

fn crc_of(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}

// payload layout:
// SET: op(1) | key_len(4) | key | expires_at(8, 0 = none) | val_len(4) | value
// DEL: op(1) | key_len(4) | key
fn build_payload(op: u8, key: &str, value: Option<(&[u8], Option<u64>)>) -> Vec<u8> {
    let mut p = Vec::new();
    p.push(op);
    p.extend_from_slice(&(key.len() as u32).to_le_bytes());
    p.extend_from_slice(key.as_bytes());
    if let Some((v, exp)) = value {
        p.extend_from_slice(&exp.unwrap_or(0).to_le_bytes());
        p.extend_from_slice(&(v.len() as u32).to_le_bytes());
        p.extend_from_slice(v);
    }
    p
}

fn frame(payload: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc_of(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

pub fn encode_set(key: &str, value: &[u8], expires_at_ms: Option<u64>) -> Vec<u8> {
    frame(build_payload(OP_SET, key, Some((value, expires_at_ms))))
}

pub fn encode_del(key: &str) -> Vec<u8> {
    frame(build_payload(OP_DEL, key, None))
}

pub fn parse_payload(payload: &[u8]) -> Option<Record> {
    if payload.len() < 5 {
        return None;
    }
    let op = payload[0];
    let key_len = u32::from_le_bytes(payload[1..5].try_into().ok()?) as usize;
    if payload.len() < 5 + key_len {
        return None;
    }
    let key = String::from_utf8(payload[5..5 + key_len].to_vec()).ok()?;
    if op == OP_DEL {
        if payload.len() != 5 + key_len {
            return None;
        }
        return Some(Record::Del { key });
    }
    if op != OP_SET {
        return None;
    }
    let mut off = 5 + key_len;
    if payload.len() < off + 8 + 4 {
        return None;
    }
    let exp_raw = u64::from_le_bytes(payload[off..off + 8].try_into().ok()?);
    off += 8;
    let val_len = u32::from_le_bytes(payload[off..off + 4].try_into().ok()?) as usize;
    off += 4;
    if payload.len() != off + val_len {
        return None;
    }
    let value = payload[off..].to_vec();
    let expires_at_ms = if exp_raw == 0 { None } else { Some(exp_raw) };
    Some(Record::Set { key, value, expires_at_ms })
}

// returns (records, valid_bytes). stops at first short read or crc fail.
pub fn read_valid(bytes: &[u8]) -> (Vec<Record>, usize) {
    let mut out = Vec::new();
    let mut off = 0;
    loop {
        if bytes.len() - off < 8 {
            break;
        }
        let len = u32::from_le_bytes(bytes[off..off + 4].try_into().unwrap()) as usize;
        let want_crc = u32::from_le_bytes(bytes[off + 4..off + 8].try_into().unwrap());
        if bytes.len() - off - 8 < len {
            break;
        }
        let payload = &bytes[off + 8..off + 8 + len];
        if crc_of(payload) != want_crc {
            break;
        }
        match parse_payload(payload) {
            Some(r) => out.push(r),
            None => break,
        }
        off += 8 + len;
    }
    (out, off)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let b = encode_set("hello", b"world", Some(12345));
        let (recs, used) = read_valid(&b);
        assert_eq!(used, b.len());
        assert_eq!(recs.len(), 1);
        match &recs[0] {
            Record::Set { key, value, expires_at_ms } => {
                assert_eq!(key, "hello");
                assert_eq!(value, b"world");
                assert_eq!(*expires_at_ms, Some(12345));
            }
            _ => panic!("wrong record"),
        }

        let b2 = encode_del("hello");
        let (recs2, used2) = read_valid(&b2);
        assert_eq!(used2, b2.len());
        assert!(matches!(recs2[0], Record::Del { .. }));
    }

    #[test]
    fn bad_crc_stops() {
        let mut b = encode_set("k", b"v", None);
        let last = b.len() - 1;
        b[last] ^= 0xff;
        let (recs, used) = read_valid(&b);
        assert_eq!(recs.len(), 0);
        assert_eq!(used, 0);
    }

    #[test]
    fn torn_tail_stops() {
        let mut b = encode_set("a", b"1", None);
        b.extend_from_slice(&[1, 2, 3]);
        let (recs, used) = read_valid(&b);
        assert_eq!(recs.len(), 1);
        assert!(used < b.len());
    }
}
