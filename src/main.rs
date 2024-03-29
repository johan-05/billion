use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Read;
use std::str;

#[derive(Debug)]
struct City {
    min: i16,
    max: i16,
    sum: i32,
    count: i32,
}

trait First {
    type Item;
    fn first_e(&self, token: Self::Item) -> usize;

    fn last_e(&self, token: Self::Item) -> usize;
}

impl<T> First for Vec<T>
where
    T: PartialEq,
{
    type Item = T;

    fn first_e(&self, token: T) -> usize {
        let mut idx = 0;
        while self[idx] != token {
            idx += 1;
        }
        return idx;
    }

    fn last_e(&self, token: T) -> usize {
        let mut idx = self.len() - 1;
        while self[idx] != token {
            idx -= 1;
        }
        return idx;
    }
}

impl<T> First for [T]
where
    T: PartialEq,
{
    type Item = T;

    fn first_e(&self, token: T) -> usize {
        let mut idx = 0;
        while self[idx] != token {
            idx += 1;
        }
        return idx;
    }

    fn last_e(&self, token: T) -> usize {
        let mut idx = self.len() - 1;
        while self[idx] != token {
            idx -= 1;
        }
        return idx;
    }
}

trait Extend {
    type Item;
    fn write(&mut self, slice: &[Self::Item]);

    fn extend(&mut self, slice: &[Self::Item], first: Self::Item);
}

impl<T> Extend for [T]
where
    T: PartialEq + Copy,
{
    type Item = T;
    fn write(&mut self, slice: &[T]) {
        for (i, n) in slice.iter().enumerate() {
            self[i] = *n;
        }
    }

    fn extend(&mut self, slice: &[T], first: T) {
        let offset = self.first_e(first);
        for (i, n) in slice.iter().enumerate() {
            self[offset + i] = *n;
        }
    }
}

trait DecodeCity {
    fn decode_slice(&mut self, slice: &[u8]);
}

impl DecodeCity for HashMap<[u8; 10], City> {
    fn decode_slice(&mut self, slice: &[u8]) {
        let mut city_buf = [0; 10];
        let mut counter = 0;
        let mut found_semi = 0;
        while counter < 10 {
            if found_semi == 0 {
                city_buf[counter] = slice[counter];
            } else {
                city_buf[counter] = 0;
                counter += 1;
                continue;
            }
            if slice[counter] == ';' as u8 {
                found_semi = counter
            }
            counter += 1;
        }

        let num: &[u8];
        if found_semi != 0 {
            num = &slice[found_semi + 1..];
        } else {
            loop {
                if slice[counter] == ';' as u8 {
                    break;
                }
                counter += 1;
            }
            num = &slice[counter + 1..];
        }

        let temp = parse_num(num);

        match self.get_mut(&city_buf) {
            Some(city) => {
                city.count += 1;
                city.sum += temp as i32;
                if temp > city.max {
                    city.max = temp;
                }
                if temp < city.min {
                    city.min = temp;
                }
            }
            None => {
                let city = City {
                    min: temp,
                    max: temp,
                    sum: temp as i32,
                    count: 1,
                };
                self.insert(city_buf, city);
            }
        }
    }
}

fn parse_num(input: &[u8]) -> i16 {
    let negative = input[0] == 0x2D;
    println!("{:?}", input);
    let len = input.len();

    let (d1, d2, d3) = match (negative, len) {
        (false, 3) => (0, input[0] - b'0', input[2] - b'0'),
        (false, 4) => (input[0] - b'0', input[1] - b'0', input[3] - b'0'),
        (true, 4) => (0, input[1] - b'0', input[3] - b'0'),
        (true, 5) => (input[1] - b'0', input[2] - b'0', input[4] - b'0'),
        _ => unreachable!(),
    };
    let int = (d1 as i16 * 100) + (d2 as i16 * 10) + d3 as i16;
    let int = if negative { -int } else { int };
    int
}

const BUF_SIZE: usize = 512 * 512;

fn main() -> io::Result<()> {
    let mut f = fs::File::open("measurements.txt")?;
    let mut buff = vec![0; BUF_SIZE];
    let mut overflow_buffer = [0u8; 60];
    let start = std::time::Instant::now();
    let mut city_map: HashMap<[u8; 10], City> = HashMap::new();

    'main: loop {
        match f.read(&mut buff) {
            Ok(ref n) if *n == BUF_SIZE => {
                let first_n = buff.first_e(0xA);
                let last_n = buff.last_e(0xA);

                let overflow_end = buff[0..first_n].as_ref();
                overflow_buffer.extend(overflow_end, 0);
                let semi = overflow_buffer.first_e(0);
                let overflow_slice = &overflow_buffer[1..semi];
                let main_buffer = buff[first_n + 1..last_n - 1].as_mut();

                city_map.decode_slice(&overflow_slice[0..overflow_slice.len() - 1]);
                for l in main_buffer.split_mut(|b| *b == 0xA) {
                    city_map.decode_slice(&l[0..l.len() - 1]);
                }

                let overflow_start = buff[last_n + 1..].as_ref();
                overflow_buffer.fill(0);
                overflow_buffer.write(overflow_start);
            }
            Ok(n) => {
                let first_n = buff.first_e(0xA);

                let overflow_end = buff[0..first_n].as_ref();
                overflow_buffer.extend(overflow_end, 0);
                let semi = overflow_buffer.first_e(0);
                let overflow_slice = &overflow_buffer[1..semi];
                let main_buffer = buff[first_n + 1..n].as_mut();

                city_map.decode_slice(overflow_slice);
                for l in main_buffer.split_mut(|b| *b == 0xA) {
                    if l.len() == 0 {
                        continue;
                    }
                    city_map.decode_slice(l);
                }

                break 'main;
            }
            Err(e) => {
                panic!("{}", e)
            }
        }
    }

    for key in city_map.keys() {
        let city = city_map.get(key).unwrap();
        println!("{:?}, {:?}", str::from_utf8(key), city);
    }

    let after = std::time::Instant::now();
    let time = after - start;
    println!("duration: {}", time.as_millis());

    Ok(())
}
