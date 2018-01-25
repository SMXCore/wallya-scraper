extern crate hyper;
extern crate rustc_serialize;
extern crate time;
extern crate docopt;
extern crate xmlJSON;
extern crate mqtt3;
use hyper::client::*;
use rustc_serialize::json::{Json, encode, ToJson};
use std::thread::sleep;
use std::time::Duration;

use std::fs::{File, OpenOptions};
use std::io::prelude::*;
use std::path::Path;

use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::btree_map::BTreeMap;

use xmlJSON::XmlDocument;
use std::str::FromStr;

use docopt::Docopt;

use std::net::TcpStream;
use std::io::{BufReader, BufWriter};
use mqtt3::{MqttRead, MqttWrite, Packet, Connect, Publish, Protocol, QoS, PacketIdentifier, Subscribe};

const USAGE: &'static str = "
wallya-scraper
Usage:
  wallya-scraper [--interval=<s>] [-f | --full] <address> <topic>
  wallya-scraper (-h | --help)
Options:
  -h --help         Show this screen.
  --interval=<s>    Interval between queries and prints. Must be divisible by 2 and must divide 60. [default: 10].
  -f --full         Use the address as a full address and do not append the default extension (/cgi-bin/wallyabcgi?req=3&lang=en)
";

#[derive(Debug, RustcDecodable, Clone)]
struct Args {
	flag_interval: isize,
	arg_address: String,
	arg_topic: String,
	flag_full: bool,
}

fn print_to_file(file: &Arc<Mutex<File>>, string: &str) -> Result<(), std::io::Error> {
	try!(file.lock().unwrap().write(string.as_bytes()));
	try!(file.lock().unwrap().flush());
	Ok(())
}

fn get(bufdeposit: Arc<Mutex<String>>, semaphor: Arc<Mutex<bool>>, address: String) {
	let now = time::now();
	let client = Client::new();
	let site_transel = address;
	let mut res = client.get(site_transel.as_str()).send().unwrap();
	assert_eq!(res.status, hyper::Ok);
	let mut buf = Vec::new();
	let _ = res.read_to_end(&mut buf).unwrap();
	let doc : XmlDocument = XmlDocument::from_str(&unsafe{String::from_utf8_unchecked(buf)}[..]).unwrap();
	let json_ini = doc.to_json().as_object().unwrap().get("measures").unwrap().as_object().unwrap().get("measure").unwrap().clone();//.as_array().unwrap()[0].clone();
	let mut json = Json::Array(vec![]);
	let array = json_ini.as_array().unwrap().clone();

	let mut btmo = BTreeMap::new();

	for elem in &array {
		let mut btm = BTreeMap::new();
		let obj = elem.as_object().unwrap();
		let name = obj.get("$").unwrap().as_object().unwrap().get("name").unwrap().as_string().unwrap().to_string();
		if obj.get("unit").unwrap().as_object().unwrap().contains_key("_") {
			btm.insert("unit".to_string(), Json::String(obj.get("unit").unwrap().as_object().unwrap().get("_").unwrap().as_string().unwrap().to_string()));
		}
		let value = obj.get("value").unwrap().as_object().unwrap().get("_").unwrap().as_string().unwrap().to_string();
		btm.insert("value".to_string(), Json::String(value));
		btmo.insert(name, Json::Object(btm));
	}
//	let mut btm = BTreeMap::new();
	btmo.insert(".REQUEST_TIME".to_string(), Json::String(format!("{}", now.strftime("%F %T").unwrap()).to_string()));
//	json.as_array_mut().unwrap().push(Json::Object(btm));
	// În mod implicit, nu primim în json și data la care a răspuns
//	let mut btm = BTreeMap::new();
	let hyper::header::Date(hyper::header::HttpDate(sv_date)) = res.headers.get::<hyper::header::Date>().unwrap().clone();
	btmo.insert(".SERVER_TIME".to_string(), Json::String(format!("{}", sv_date.strftime("%F %T").unwrap()).to_string()));
//	json.as_array_mut().unwrap().push(Json::Object(btm));
	let jso = Json::Object(btmo);
	bufdeposit.lock().unwrap().clone_from(&encode(&jso).unwrap());
	*semaphor.lock().unwrap() = true;
}

fn print(bufdeposit: Arc<Mutex<String>>, file: &Arc<Mutex<File>>, print_header: bool, topic: String) {
	let buf = bufdeposit.lock().unwrap().clone();
	let json = Json::from_str(&buf[..]).unwrap();
	//println!("{:?}", json);
	let mut obj = json.as_object().unwrap().clone();
//	array.retain(|t| !(t.as_object().unwrap().contains_key(".SERVER_TIME")));
//	array.sort_by_key(|t| t.as_object().unwrap().iter().next().unwrap().0.clone());
	if print_header {
		print_to_file(file, "COMPUTER_TIME\tLOG MS").unwrap();
		for (name, _) in &obj {
//			let (name, _) = elem.as_object().unwrap().iter().next().unwrap();
			print_to_file(file, &format!("\t{}", name)[..]).unwrap();
		}
		print_to_file(file, "\n").unwrap();
	}
	let mut obj2 = json.as_object().unwrap().clone();
//	array2.retain(|t| t.as_object().unwrap().contains_key(".SERVER_TIME"));
	let now = time::now();
	print_to_file(file, &format!("{} {:03}\t{}", now.strftime("%F %T").unwrap(), now.tm_nsec / 1000_000, now.tm_nsec / 1000)[..]).unwrap();
	for (_, elem) in &obj2 {
//		let (_, value) = elem.as_object().unwrap().iter().next().unwrap();
		let value = if let Some(elemo) = elem.as_object() {
			elemo.get("value").unwrap()
		} else {
			elem
		};
		print_to_file(file, &format!("\t{}", value.as_string().unwrap())[..]).unwrap();
	}
	print_to_file(file, "\n").unwrap();

	// MQTT
//	println!("-");
	let stream = TcpStream::connect("127.0.0.1:1883").unwrap();
	let mut reader = BufReader::new(stream.try_clone().unwrap());
	let mut writer = BufWriter::new(stream.try_clone().unwrap());
	let connect =
		Packet::Connect(Box::new(Connect {
			protocol: Protocol::MQTT(4),
			keep_alive: 30,
			client_id: "wallya-scraper-pub".to_owned(),
			clean_session: true,
			last_will: None,
			username: None,
			password: None
		}));

	println!(".");
	let _ = writer.write_packet(&connect);
	let _ = writer.flush();
	let packet = reader.read_packet().unwrap();
	// PUBLISH
	let publish = Packet::Publish(Box::new(Publish {
		dup: false,
		qos: QoS::AtLeastOnce,
		retain: false,
		topic_name: topic,
		pid: Some(PacketIdentifier(10)),
		payload: Arc::new(buf.into_bytes())
	}));
	let _ = writer.write_packet(&publish);
	let _ = writer.flush();
	let packet = reader.read_packet().unwrap();
	println!("{:?}", packet);
}

fn get_timer(bufdeposit: Arc<Mutex<String>>, semaphor: Arc<Mutex<bool>>, args: Args) {
	let now = time::now();
	sleep(Duration::new(0, 1000_000_000 - now.tm_nsec as u32));
	loop {
		let bufdeposit_clone = bufdeposit.clone();
		let semaphor_clone = semaphor.clone();
		let addr = if args.flag_full {
			args.clone().arg_address
		} else {
			let addr = args.clone().arg_address;
			if addr.ends_with("/") {
				addr + "cgi-bin/wallyabcgi?req=3&lang=en"
			} else {
				addr + "/cgi-bin/wallyabcgi?req=3&lang=en"
			}
		};
		thread::spawn(move || get(bufdeposit_clone, semaphor_clone, addr));
		let now = time::now();
		let sec_sleep = args.flag_interval as u64 - ((now.tm_sec + args.flag_interval as i32 / 2) % args.flag_interval as i32) as u64;
		if now.tm_nsec < 500_000_000 && now.tm_nsec > 0 {
			sleep(Duration::new(sec_sleep - 1, 1000_000_000 - now.tm_nsec as u32));
		} else {
			sleep(Duration::new(sec_sleep, 1000_000_000 - now.tm_nsec as u32));
		}
	}
}

fn print_timer(bufdeposit: Arc<Mutex<String>>, semaphor: Arc<Mutex<bool>>, args: Args) {
	let now = time::now();
	let mut old_path_str = format!("wallya{}.txt", now.strftime("%Y%m%d").unwrap());
	let path_str = format!("wallya{}.txt", now.strftime("%Y%m%d").unwrap());
	let path = Path::new(&path_str[..]);
	let mut print_header = !path.exists();
	let mut file = Arc::new(Mutex::new(OpenOptions::new().write(true).append(true).create(true).open(path).unwrap()));
	let now = time::now();
	sleep(Duration::new(0, 1000_000_000 - now.tm_nsec as u32));
	loop {
		let now = time::now();
		let path_str2 = format!("wallya{}.txt", now.strftime("%Y%m%d").unwrap());
		if path_str2 != old_path_str {
			file.lock().unwrap().flush().unwrap();
			print_header = true;
			old_path_str = path_str2;
			let path_str = format!("wallya{}.txt", now.strftime("$Y%m%d").unwrap());
			let path = Path::new(&path_str[..]);
			file = Arc::new(Mutex::new(OpenOptions::new().write(true).append(true).create(true).open(path).unwrap()));
		}
		let filelock2 = file.clone();
		let bufdeposit_clone = bufdeposit.clone();
		let arg_topic = args.arg_topic.clone();
		if *semaphor.lock().unwrap() {
			thread::spawn(move || print(bufdeposit_clone, &filelock2, print_header, arg_topic));
			print_header = false;
		}
		let now = time::now();
		let sec_sleep = args.flag_interval as u64 - (now.tm_sec % args.flag_interval as i32) as u64;
		if now.tm_nsec < 500_000_000 && now.tm_nsec > 0 {
			sleep(Duration::new(sec_sleep - 1, 1000_000_000 - now.tm_nsec as u32));
		} else {
			sleep(Duration::new(sec_sleep, 1000_000_000 - now.tm_nsec as u32));
		}
	}
}

fn main() {
	let args: Args = Docopt::new(USAGE).and_then(|d| d.decode()).unwrap_or_else(|e| e.exit());
	if args.flag_interval % 2 != 0 || 60 % args.flag_interval != 0 {
		panic!("Interval not divisible by 2 or not a divisor of 60");
	}
	let bufdeposit = Arc::new(Mutex::new(String::new()));
	let bufdeposit_get = bufdeposit.clone();
	let semaphor = Arc::new(Mutex::new(false));
	let semaphor_get = semaphor.clone();
	let args_cp = args.clone();
	let get_child = thread::spawn(move || get_timer(bufdeposit_get, semaphor_get, args_cp));
	let bufdeposit_print = bufdeposit.clone();
	let semaphor_print = semaphor.clone();
	let print_child = thread::spawn(move || print_timer(bufdeposit_print, semaphor_print, args));
	let _ = get_child.join();
	let _ = print_child.join();
}
