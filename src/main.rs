use std::boxed::Box;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io::Read;
use std::mem;
use std::str;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{self, Sender};
use std::sync::Mutex;
use std::thread::{self, JoinHandle};

#[derive(Debug, Copy, Clone)]
struct City {
    min: i16,
    max: i16,
    sum: i32,
    count: i32,
}

struct MainBuffer {
    data: Vec<u8>,
    beginning: usize,
    end: usize,
}

struct Thread {
    handle: JoinHandle<HashMap<[u8; 10], City>>,
    sender: Sender<Option<MainBuffer>>,
    mutex: &'static Mutex<bool>,
}

impl Thread {
    fn new(mutex_ref: &'static Mutex<bool>, city_map: HashMap<[u8; 10], City>) -> Self {
        let (tx, rx) = mpsc::channel::<Option<MainBuffer>>();

        let handle = thread::spawn(move || {
            let city_map = thread_loop(rx, mutex_ref, city_map);
            return city_map;
        });

        return Thread {
            handle: handle,
            sender: tx,
            mutex: mutex_ref,
        };
    }

    fn send(&self, data: MainBuffer) {
        self.sender.send(Some(data)).unwrap();
    }

    fn is_ready(&self) -> bool {
        let ready = self.mutex.lock().unwrap();
        return *ready;
    }

    fn close(&self) {
        self.sender.send(None).unwrap();
    }

    fn join(self) -> HashMap<[u8; 10], City> {
        self.handle.join().unwrap()
    }
}


trait First {
    type Item;
    fn first_occur(&self, token: Self::Item) -> usize;

    fn last_occur(&self, token: Self::Item) -> usize;
}

impl<T> First for Vec<T>
where
    T: PartialEq,
{
    type Item = T;
    fn first_occur(&self, token: T) -> usize {
        let mut idx = 0;
        while self[idx] != token {
            idx += 1;
        }
        return idx;
    }

    fn last_occur(&self, token: T) -> usize {
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

    fn first_occur(&self, token: T) -> usize {
        let mut idx = 0;
        while self[idx] != token {
            idx += 1;
        }
        return idx;
    }

    fn last_occur(&self, token: T) -> usize {
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
        let offset = self.first_occur(first);
        for (i, n) in slice.iter().enumerate() {
            self[offset + i] = *n;
        }
    }
}

trait DecodeCity {
    fn decode_slice(&mut self, slice: &[u8]);

    fn merge(&mut self, thread_map: Self);
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

    fn merge(&mut self, thread_map: HashMap<[u8; 10], City>) {
        for key in thread_map.keys(){
            let merge_city = thread_map.get(key).unwrap();
            match self.get_mut(key) {
                Some(city) => {
                    city.count += merge_city.count;
                    city.sum += merge_city.sum;
                    if merge_city.max > city.max {
                        city.max = merge_city.max;
                    }
                    if merge_city.min < city.min {
                        city.min = merge_city.min;
                    }
                }
                None => {
                    self.insert(*key, merge_city.clone());
                }
            }
        }
    }
}

fn thread_loop(
    rx: Receiver<Option<MainBuffer>>,
    mutex_ref: &'static Mutex<bool>,
    mut city_map: HashMap<[u8; 10], City>,
) -> HashMap<[u8; 10], City> {
    'thread_loop: loop {
        let main_buffer_option = rx.recv().unwrap();
        match main_buffer_option {
            Some(main_buffer) => {
                let data = main_buffer.data[main_buffer.beginning..main_buffer.end].as_ref();
                let mut counter = 0;
                let mut ready = mutex_ref.lock().unwrap();
                *ready = false;
                mem::drop(ready);

                for l in data.split(|b| *b == b'\r') {
                    counter += 1;
                    if l.len() <= 1 {
                        println!("cancelled, {}, {:?}", counter, l);
                        panic!();
                        //continue;
                    }
                    city_map.decode_slice(&l[1..]);
                }
                let mut ready = mutex_ref.lock().unwrap();
                *ready = true;
                mem::drop(ready);
            }
            None => break 'thread_loop,
        }
    }
    return city_map;
}

fn parse_num(input: &[u8]) -> i16 {
    let negative = input[0] == 0x2D;
    let len = input.len();

    let (d1, d2, d3) = match (negative, len) {
        (false, 3) => (0, input[0] - b'0', input[2] - b'0'),
        (false, 4) => (input[0] - b'0', input[1] - b'0', input[3] - b'0'),
        (true, 4) => (0, input[1] - b'0', input[3] - b'0'),
        (true, 5) => (input[1] - b'0', input[2] - b'0', input[4] - b'0'),
        _ => {
            println!("{:?}", str::from_utf8(input));
            panic!()
        }
    };
    let int = (d1 as i16 * 100) + (d2 as i16 * 10) + d3 as i16;
    let int = if negative { -int } else { int };
    int
}

const BUF_SIZE: usize = 512 * 512;
const THREAD_COUNT: usize = 5;

fn main() -> Result<(), Box<dyn Error>> {
    let mut f = fs::File::open("measurements.txt")?;
    let mut overflow_buffer = [0u8; 60];
    overflow_buffer[0] = b'\r';

    let mut city_map: HashMap<[u8; 10], City> = HashMap::new();
    let start = std::time::Instant::now();

    let mut thread_pool = Vec::new();

    for _ in 0..THREAD_COUNT {
        let ready_mutex = Box::leak(Box::new(Mutex::new(true)));
        let citymap_ref: HashMap<[u8; 10], City> = HashMap::new();
        let thread = Thread::new(ready_mutex, citymap_ref);
        thread_pool.push(thread);
    }

    'main: loop {
        let mut buff = vec![0; BUF_SIZE];

        match f.read(&mut buff) {
            Ok(ref n) if *n == BUF_SIZE => {
                let first_r = buff.first_occur(b'\r');
                let last_r = buff.last_occur(b'\r');

                let overflow_end = buff[0..first_r].as_ref();
                overflow_buffer.extend(overflow_end, 0);
                let semi = overflow_buffer.first_occur(0);
                let overflow_slice = &overflow_buffer[0..semi];

                city_map.decode_slice(&overflow_slice[1..]);

                let overflow_start = buff[last_r + 1..].as_ref();
                overflow_buffer.fill(0);
                overflow_buffer.write(overflow_start);

                let main_buffer = MainBuffer {
                    data: buff,
                    beginning: first_r + 1,
                    end: last_r,
                };

                for thread in &thread_pool {
                    if thread.is_ready() {
                        thread.send(main_buffer);
                        break;
                    }
                }
            }
            Ok(_) => {
                println!("last batch");
                let first_r = buff.first_occur(b'\r');
                let last_r = buff.last_occur(b'\r');


                let overflow_end = buff[0..first_r].as_ref();
                overflow_buffer.extend(overflow_end, 0);
                let semi = overflow_buffer.first_occur(0);
                let overflow_slice = &overflow_buffer[0..semi];
                let main_buffer = MainBuffer {
                    data: buff,
                    beginning: first_r + 1,
                    end: last_r
                };

                if overflow_slice.len() != 0 {
                    city_map.decode_slice(&overflow_slice[1..]);
                }

                for thread in &thread_pool {
                    if thread.is_ready() {
                        thread.send(main_buffer);
                        break;
                    }
                }

                break 'main;
            }
            Err(e) => {
                panic!("{}", e)
            }
        }
    }

    for thread in thread_pool {
        thread.close();
        let thread_map = thread.join();
        city_map.merge(thread_map);
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
