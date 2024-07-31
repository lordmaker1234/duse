
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::fmt::Display;
use std::{env, path};

use size::Size;
use crossbeam::crossbeam_channel::{unbounded, Receiver, Sender};

#[derive(Default)]
struct Stats {
    size: u64,
    count: i32,
}

impl Display for Stats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f, 
            "{} bytes ({}) across {} items",
            self.size,
            Size::from_bytes(self.size as f64),
            self.count
        )
    }
}

impl<'a> std::iter::Sum<&'a Stats> for Stats {
    fn sum<I: Iterator<Item = &'a Stats>>(Iter: I) -> Self{
        let mut result = Self::default();
        for stat in Iter {
            result.count += stat.count;
            result.size += stat.size;
        }
        result
    }
}

impl std::ops::AddAssign for Stats {
    fn add_assign(&mut self, rhs: Self) {
        self.count += rhs.count;
        self.size += rhs.size;
    }
}

impl Stats {
    fn from_file(p: &Path) -> Self {
        Self {
            size: p.metadata().unwrap().len(),
            count: 1,
        }
    }

    fn add_file(&mut self, p: &Path) -> Result<(), std::io::Error>{
        let size = p.metadata()?.len();
        self.count += 1;
        self.size += size;

        Ok(())
    }

}

fn main() {
    let current_path = env::current_dir()
        .expect("")
        .to_str()
        .expect("")
        .to_string();
    let args: Vec<String> = env::args().collect();
    let target = args.get(1).unwrap_or(&current_path);
    let path = path::Path::new(target);

    if !path.exists() {
        eprintln!("Invalid path: {}", &target);
    } else if path.is_file() {
        todo!();
    } else if path.is_dir() {
//        let size: f64 = get_dir_size(path) as f64;
//        println!("Total size is {} bytes ({})", size, Size::from_bytes(size));
        let cores = num_cpus::get().to_string();
        let cores = std::env::var("WORKERS").unwrap_or(cores).parse().unwrap();
        let stat = size_of_dir(path, cores);

        println!("Total size is {}", stat);
    } else {
        eprintln!("Unknown type {}", target);
    }
}

fn size_of_dir(path: &path::Path, num_threads: usize) -> Stats {
    let mut stats = Vec::new();
    let mut consumers = Vec::new();
    {
        let (producer, rx) = unbounded();

        for idx in 0..num_threads {
            let producer = producer.clone();
            let rx = rx.clone();

            consumers.push(std::thread::spawn(move || worker(idx, rx, &producer)));
        }

        stats.push(walk(path, &producer));
    }

    for c in consumers {
        let stat = c.join().unwrap();
        stats.push(stat);
    }
    stats.iter().sum()
}

fn worker(_idx: usize, receiver: Receiver<PathBuf>, sender: &Sender<PathBuf>) -> Stats {
    let mut stat = Stats::default();
    while let Ok(path) = receiver.recv_timeout(Duration::from_millis(50)){
        let newstat = walk(&path, sender);
        stat += newstat;
    }
    stat
}

fn walk(path: &path::Path, sender: &Sender<PathBuf>) -> Stats {
    let mut stat = Stats::default();

    if let Err(e) = path.read_dir() {
        eprintln!("Error {} ({})", e, path.to_str().unwrap());
        return stat;
    } else if let Ok(dir_items) = path.read_dir() {
        for entry in dir_items.flatten() {
            let path = entry.path ();
            if path.is_file() {
                stat.add_file(&path).unwrap();    
            } else if path.is_dir() {
                sender.try_send(path).unwrap();
            }
        }
    }
    stat
}
