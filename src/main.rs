extern crate hyper;
extern crate rustc_serialize;
extern crate time;
use hyper::client::*;
use rustc_serialize::json::{Json, encode};
use std::thread::sleep;
use std::time::Duration;

use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::Path;

use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::btree_map::BTreeMap;

fn print_to_file(file: &Arc<Mutex<File>>, string: &str) -> Result<usize, std::io::Error> {
    file.lock().unwrap().write(string.as_bytes())
}

fn get(bufdeposit: Arc<Mutex<String>>, semaphor: Arc<Mutex<bool>>) {
	let now = time::now();
    let client = Client::new();
    let site_transel = "http://www.transelectrica.ro/sen-filter";
    let mut res = client.get(site_transel).send().unwrap();
    assert_eq!(res.status, hyper::Ok);
    let mut buf = Vec::new();
    let _ = res.read_to_end(&mut buf).unwrap();
    let mut json = Json::from_str(&unsafe{String::from_utf8_unchecked(buf)}[..]).unwrap();
	let mut btm = BTreeMap::new();
	btm.insert(".REQUEST_TIME".to_string(), Json::String(format!("{}", now.strftime("%y/%m/%d %T").unwrap()).to_string()));
	json.as_array_mut().unwrap().push(Json::Object(btm));
    bufdeposit.lock().unwrap().clone_from(&encode(&json).unwrap());
    *semaphor.lock().unwrap() = true;
}

fn print(bufdeposit: Arc<Mutex<String>>, file: &Arc<Mutex<File>>, print_header: bool) {
    let buf = bufdeposit.lock().unwrap().clone();
    let json = Json::from_str(&buf[..]).unwrap();
    let mut array = json.as_array().unwrap().clone();
    array.retain(|t| !(t.as_object().unwrap().contains_key("row1_HARTASEN_DATA")));
    array.sort_by_key(|t| t.as_object().unwrap().iter().next().unwrap().0.clone());
    if print_header {
        print_to_file(file, "COMPUTER_TIME\tSERVER_TIME\tLOG MS").unwrap();
        for elem in &array {
            let (name, _) = elem.as_object().unwrap().iter().next().unwrap();
            print_to_file(file, &format!("\t{}", name)[..]).unwrap();
        }
        print_to_file(file, "\n").unwrap();
    }
    let mut array2 = json.as_array().unwrap().clone();
    array2.retain(|t| t.as_object().unwrap().contains_key("row1_HARTASEN_DATA"));
    let now = time::now();
    print_to_file(file, &format!("{} {:03}\t{}\t{}", now.strftime("%y/%m/%d %T").unwrap(), now.tm_nsec / 1000_000, array2.iter().next().unwrap().as_object().unwrap().iter().next().unwrap().1.as_string().unwrap(), now.tm_nsec / 1000)[..]).unwrap();
    for elem in &array {
        let (_, value) = elem.as_object().unwrap().iter().next().unwrap();
        print_to_file(file, &format!("\t{}", value.as_string().unwrap())[..]).unwrap();
    }
    print_to_file(file, "\n").unwrap();
}

fn get_timer(bufdeposit: Arc<Mutex<String>>, semaphor: Arc<Mutex<bool>>) {
    let now = time::now();
    sleep(Duration::new(0, 1000_000_000 - now.tm_nsec as u32));
    loop {
        let bufdeposit_clone = bufdeposit.clone();
        let semaphor_clone = semaphor.clone();
        thread::spawn(move || get(bufdeposit_clone, semaphor_clone));
		let now = time::now();
		let sec_sleep = 10 - ((now.tm_sec + 5) % 10) as u64;
        if now.tm_nsec < 500_000_000 && now.tm_nsec > 0 {
            sleep(Duration::new(sec_sleep - 1, 1000_000_000 - now.tm_nsec as u32));
        } else {
            sleep(Duration::new(sec_sleep, 1000_000_000 - now.tm_nsec as u32));
        }
    }
}

fn print_timer(bufdeposit: Arc<Mutex<String>>, semaphor: Arc<Mutex<bool>>) {
    let now = time::now();
    let mut old_path_str = format!("transel{}.txt", now.strftime("20%y%m%d").unwrap());
    let path_str = format!("transel{}.txt", now.strftime("20%y%m%d").unwrap());
    let path = Path::new(&path_str[..]);
    let mut print_header = !path.exists();
    let mut file = Arc::new(Mutex::new(OpenOptions::new().write(true).append(true).create(true).open(path).unwrap()));
    let now = time::now();
    sleep(Duration::new(0, 1000_000_000 - now.tm_nsec as u32));
    loop {
        let now = time::now();
        let path_str2 = format!("transel{}.txt", now.strftime("20%y%m%d").unwrap());
        if path_str2 != old_path_str {
            file.lock().unwrap().flush().unwrap();
            print_header = true;
            old_path_str = path_str2;
            let path_str = format!("transel{}.txt", now.strftime("20%y%m%d").unwrap());
            let path = Path::new(&path_str[..]);
            file = Arc::new(Mutex::new(OpenOptions::new().write(true).append(true).create(true).open(path).unwrap()));
        }
        let filelock2 = file.clone();
        let bufdeposit_clone = bufdeposit.clone();
        if *semaphor.lock().unwrap() {
            thread::spawn(move || print(bufdeposit_clone, &filelock2, print_header));
            print_header = false;
        }
        let now = time::now();
		let sec_sleep = 10 - (now.tm_sec % 10) as u64;
        if now.tm_nsec < 500_000_000 && now.tm_nsec > 0 {
            sleep(Duration::new(sec_sleep - 1, 1000_000_000 - now.tm_nsec as u32));
        } else {
            sleep(Duration::new(sec_sleep, 1000_000_000 - now.tm_nsec as u32));
        }
    }
}

fn main() {
    let bufdeposit = Arc::new(Mutex::new(String::new()));
    let bufdeposit_get = bufdeposit.clone();
    let semaphor = Arc::new(Mutex::new(false));
    let semaphor_get = semaphor.clone();
    let get_child = thread::spawn(move || get_timer(bufdeposit_get, semaphor_get));
    let bufdeposit_print = bufdeposit.clone();
    let semaphor_print = semaphor.clone();
    let print_child = thread::spawn(move || print_timer(bufdeposit_print, semaphor_print));
    let _ = get_child.join();
    let _ = print_child.join();
}
