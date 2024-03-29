use std::fs;
use std::io;
use std::str;
use std::io::Read;
use std::io::BufRead;
use std::collections::HashMap;

const BUF_SIZE: usize = 10;
const SLIC:[u8;5] = [4,3,2,64,3];

#[derive(Debug)]
struct City{
    min:i16,
    max:i16,
    sum:i32,
    count:i32
}

fn main() -> io::Result<()> {
    let mut f = fs::File::open("test.txt")?;
    let mut buff = [0; BUF_SIZE];

    //let city_map:HashMap<&[u8], City> = HashMap::new();


    'main: loop {
        match f.read(&mut buff) {
            Ok(ref n) if *n == BUF_SIZE => {
                let last_n = buff.len() - buff.as_slice().iter().rev().position(|b| *b==0xA).unwrap();
                let slico = buff[0..last_n].as_ref();
                slico.split(|b| *b == 0xA)
                    .for_each(|b| {
                        let mut iter = b.split(|b| *b == ';' as u8);
                        let city_name = iter.next().unwrap();
                        println!("city name {}", str::from_utf8(city_name).unwrap());
                        let temp:f32 = str::
                            from_utf8(
                                iter.next()
                                .unwrap())
                            .unwrap()
                            .parse()
                            .unwrap();
                        println!("city {}, temp {}", str::from_utf8(city_name).unwrap(), temp);
                    });

            }
            Ok(n) => {
                let thing:&[u8] = buff[0..n].as_ref();

                thing.split(|b| *b == 0xA)
                .for_each(|b| {
                    let mut iter = b.split(|b| *b == ';' as u8);
                    let city_name = iter.next().unwrap();
                    let temp:f32 = str::
                        from_utf8(
                            iter.next()
                            .unwrap())
                        .unwrap()
                        .trim()
                        .parse::<f32>()
                        .unwrap();

                    println!("city name {} temp {}", str::from_utf8(city_name).unwrap(), temp);

                });

            break 'main;
            }

            Err(e) => {
                panic!("paniced with {}", e);
            }
        };
    }

    Ok(())
}
