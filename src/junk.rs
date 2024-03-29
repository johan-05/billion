use std::fs;
use std::io;
use std::str;
use std::io::Read;
use std::collections::HashMap;


#[derive(Debug)]
struct City{
    min:f32,
    max:f32,
    sum:f32,
    count:i32
}

trait First{
    type Item;
    fn first_e(&self, token:Self::Item)->usize;

    fn last_e(&self, token:Self::Item)->usize;
}

impl<T> First for Vec<T>
where T:PartialEq{
    type Item = T;

    fn first_e(&self, token:T)->usize {
        let mut idx = 0;
        while self[idx] != token {
            idx += 1;
        }
        return idx;
    }

    fn last_e(&self, token:T)->usize {
        let mut idx = self.len()-1;
        while self[idx] != token {
            idx -= 1;
        }
        return idx;
    }
}

impl<T> First for [T]
where T:PartialEq{
    type Item = T;

    fn first_e(&self, token:T)->usize {
        let mut idx = 0;
        while self[idx] != token {
            idx += 1;
        }
        return idx;
    }

    fn last_e(&self, token:T)->usize {
        let mut idx = self.len()-1;
        while self[idx] != token {
            idx -= 1;
        }
        return idx;
    }
}

trait Extend{
    type Item;
    fn write(&mut self, slice:&[Self::Item]);

    fn extend(&mut self, slice:&[Self::Item], first: Self::Item);
}

impl<T> Extend for [T]
where T:PartialEq + Copy{
    type Item = T;
    fn write(&mut self, slice:&[T]) {
        for (i, n) in slice.iter().enumerate(){
            self[i] = *n;
        }
    }
    
    fn extend(&mut self, slice:&[T], first:T) {
        let offset = self.first_e(first);
        for (i, n) in slice.iter().enumerate(){
            self[offset+i] = *n;
        }
    }
}


trait DecodeCity {
    fn decode_slice(&mut self, slice:&[u8]);
}

impl DecodeCity for HashMap<[u8; 10], City>{

    fn decode_slice(&mut self, slice:&[u8]) {
        //println!("{:?}", str::from_utf8(slice));
        //println!("{:?}", slice);

        let mut iter = slice.split(|b| *b == ';' as u8);
        let city_name = iter.next().unwrap();
        let mut city_buf = [0; 10];
        for (i, n) in city_name.iter().enumerate(){
            if i<10{
                city_buf[i] = *n;
            }else{
                break;
            }
        }
        //println!("city name {},", str::from_utf8(city_name).unwrap());
        let temp = str::
            from_utf8(
                iter.next()
                .unwrap())
            .unwrap()
            .trim()
            .parse::<f32>()
            .unwrap();


        match self.get_mut(&city_buf){
            Some(city)=>{
                city.count += 1;
                city.sum += temp;
                if temp > city.max{
                    city.max = temp;
                }
                if temp < city.min{
                    city.min = temp;
                }
            } 
            None => {
                let city = City{
                    min:temp,
                    max:temp,
                    sum:temp,
                    count:1
                };
                self.insert(city_buf, city);
            }
        }
    }
}

const BUF_SIZE: usize = 120000;


fn main() -> io::Result<()> {
    let mut f = fs::File::open("measurements.txt")?;
    let mut buff = vec![0; BUF_SIZE];
    let mut overflow_buffer = [0u8; 60];
    let mut counter:i64 = 0;
    let start = std::time::Instant::now();
    let mut city_map:HashMap<[u8; 10], City> = HashMap::new();

    'main: loop {
        match f.read(&mut buff) {
            Ok(ref n) if *n == BUF_SIZE => {
                //println!("new packet");
                let first_n = buff.first_e(0xA);
                let last_n = buff.last_e(0xA);

                let overflow_end = buff[0..first_n].as_ref();
                overflow_buffer.extend(overflow_end, 0);
                let semi = overflow_buffer.first_e(0);
                let overflow_slice = &overflow_buffer[1..semi];

                let main_buffer = buff[first_n+1..last_n-1].as_mut();

                city_map.decode_slice(overflow_slice);
                main_buffer
                    .split_mut(|b| *b==0xA)
                    .for_each(|l| city_map.decode_slice(l));
                
                let overflow_start = buff[last_n..].as_ref();
                overflow_buffer.fill(0);
                overflow_buffer.write(overflow_start);
            }
            Ok(n) => {
                let first_n = buff.first_e(0xA);

                let overflow_end = buff[0..first_n].as_ref();
                overflow_buffer.extend(overflow_end, 0);
                let semi = overflow_buffer.first_e(0);
                let overflow_slice = &overflow_buffer[1..semi];

                let main_buffer = buff[first_n+1..n].as_mut();

                city_map.decode_slice(overflow_slice);
                main_buffer
                    .split_mut(|b| *b==0xA)
                    .for_each(|l| city_map.decode_slice(l));
                
                break 'main;
            }

            Err(e) => {
                panic!("paniced with {}", e);
            }
        };
    }

    let after = std::time::Instant::now();
    let time = after - start;
    println!("duration: {}, count: {}", time.as_millis(), counter);

    for key in city_map.keys(){
        let city = city_map.get(key).unwrap();
        println!("{}, {:?}", str::from_utf8(key).unwrap(),  city);
    }

    Ok(())
}
