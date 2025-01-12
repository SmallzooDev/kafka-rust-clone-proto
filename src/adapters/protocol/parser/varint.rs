pub trait PutVarint {
    fn put_uvarint(&mut self, num: i64);
}

impl PutVarint for Vec<u8> {
    fn put_uvarint(&mut self, mut num: i64) {
        while (num & !0x7F) != 0 {
            self.push(((num & 0x7F) | 0x80) as u8);
            num >>= 7;
        }
        self.push(num as u8);
    }
}

pub fn decode_varint(buf: &[u8]) -> u64 {
    let mut result: u64 = 0;
    let mut shift = 0;

    for &byte in buf {
        result |= ((byte & 0x7f) as u64) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            break;
        }
    }

    result
} 