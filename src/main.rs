use std::io;
use tokio::net::TcpListener;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};
use std::ops::Add;

struct Entry{
    value: String,
    expires_at: Option<Instant>
}

impl Entry{
    fn new(value: String, expires_at: Option<Instant>) -> Self{
        Self{
            value,
            expires_at
        }
    }
}

async fn handle_client(socket: &mut TcpStream, client_addr: SocketAddr, data_set: Arc<RwLock<HashMap<String, Entry>>>){
    let mut buffer = [0; 512];
    loop {
        match socket.read(&mut buffer).await{
            Ok(0) => break,
            Ok(n) => {
                let commands = match parse_resp(&buffer[0..n]){
                    Ok(res) => res,
                    Err(e) => {
                        println!("Parsing Error Code -> {}", e);
                        vec![]
                    },
                };
                
                println!("Received command {:?} from client {:?}", commands, client_addr);
                
                if commands.len() > 0{
                    match commands[0].as_str(){
                        "GET" => handle_get(socket, &commands[1], &data_set).await,
                        "SET" => handle_set(socket, &commands[1], &commands[2], &data_set).await,
                        "DEL" => handle_del(socket, &commands[1], &data_set).await,
                        "EXPIRE" => handle_exp(socket, &commands[1], &commands[2], &data_set).await,
                        _ => match socket.write(b"-ERR Unknown Command\r\n").await{
                            Ok(_) => (),
                            Err(_e) => println!("Socket write failed"),
                        }
                    }
                }
            },
            Err(_e) => println!("got some error"),
        }
    }
    
}

async fn handle_get(socket: &mut TcpStream, key: &String, data_set: &Arc<RwLock<HashMap<String, Entry>>>){
    let map = data_set.read().await;
    match map.get(key){
        Some(res) => {
                let val = &res.value;
                
                if check_expired(res){
                    drop(map);
                    let mut map = data_set.write().await;
                    map.remove(key);
                    match socket.write(b"$-1\r\n").await{
                        Ok(_) => (),
                        Err(_e) => println!("Socket write failed!"),
                    }
                }else{
                    let len = (*val).len();
                    let res_string = format!("${}\r\n{}\r\n", len, *val);
                    match socket.write(res_string.as_bytes()).await{
                    Ok(_) => (),
                    Err(_e) => println!("Socket write failed!")
                }
            }
        },
        _ => match socket.write(b"$-1\r\n").await{
            Ok(_) => (),
            Err(_e) => println!("Socket write failed!"),
        }
    }
}

async fn handle_set(socket: &mut TcpStream, key: &String, val: &String, data_set: &Arc<RwLock<HashMap<String, Entry>>>){
    let new_entry = Entry::new(val.to_string(), None);
    
    let mut map = data_set.write().await;
    
    map.insert(key.to_string(), new_entry);
    
    match socket.write(b"+OK\r\n").await{
        Ok(_) => (),
        Err(_e) => println!("Socket write failed!"),
    }
}

async fn handle_del(socket: &mut TcpStream, key: &String, data_set: &Arc<RwLock<HashMap<String, Entry>>>){
    let mut map = data_set.write().await;
    match map.remove(key){
        Some(_res) => {
            match socket.write(b"+OK\r\n").await{
                Ok(_) => (),
                Err(_e) => println!("Socket write failed!"),
            } 
        },
        _ => match socket.write(b"$-1\r\n").await{
            Ok(_) => (),
            Err(_e) => println!("Socket write failed!"),
        }
    }
}

async fn handle_exp(socket: &mut TcpStream, key: &String, expiry: &String, data_set: &Arc<RwLock<HashMap<String, Entry>>>){
    let expiry_secs: u64 = match expiry.parse(){
        Ok(sec) => sec,
        Err(_) => {
            match socket.write(b"-ERR value is not an integer\r\n").await{
                Ok(_) => (),
                Err(_e) => println!("Socket write failed"),
            }
            return;
        },
    };
    let now = Instant::now();
    let expires_at = Some(now.add(Duration::new(expiry_secs, 0)));
    
    let mut map = data_set.write().await;
    
    match map.get_mut(key){
        Some(entry) => {
            entry.expires_at = expires_at;
            match socket.write(b":1\r\n").await{
              Ok(_) => (),
              Err(_e) => println!("Socket write failed!"),  
            };
        },
        None => {
            match socket.write(b":0\r\n").await{
                Ok(_) => (),
                Err(_e) => println!("Socket write failed!"),
            }
        }
    }
}


fn check_expired(data: &Entry) -> bool{
    let now = Instant::now();
    match data.expires_at{
        Some(exp) => now > exp,
        None => false, 
    }
}

// Example -> *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n
fn parse_resp(buffer: &[u8]) -> Result<Vec<String>, String>{
    match *buffer.get(0).ok_or("PARSE_OUT_OF_BOUNDS".to_string())?{
        b'*' => (),
        _ => return Err("PARSE_ERROR_FORMAT".to_string()),
    }
    let mut i = 1;
    
    let command_length = match get_slength(buffer, &mut i, "PARSE_ERROR_LENGTH"){
        Ok(len) => len,
        Err(e) => return Err(e),
    };
    
    match flush_rn(buffer, &mut i){
        Ok(_) => (),
        Err(e) => return Err(e),
    }
    
    let mut result = vec![];
    
    for _command in 1..=command_length{
        match *buffer.get(i).ok_or("PARSE_OUT_OF_BOUNDS".to_string())?{
            b'$' => (),
            _ => return Err("PARSE_ERROR_COMMAND".to_string()),
        }
        i = i+1;
        
        let tab_length = match get_slength(buffer, &mut i, "PARSE_ERROR_COMMAND_LENGTH"){
            Ok(len) => len,
            Err(e) => return Err(e),
        };
        
        match flush_rn(buffer, &mut i){
            Ok(_) => (),
            Err(e) => return Err(e),
        }
        
        result.push(String::from_utf8_lossy(&buffer.get(i..i+tab_length).ok_or("PARSE_OUT_OF_BOUNDS".to_string())?).to_string());
        i = i+tab_length;
        
        match flush_rn(buffer, &mut i){
            Ok(_) => (),
            Err(e) => return Err(e),
        }
    }
    Ok(result)
}

fn flush_rn(buffer: &[u8], ind: &mut usize) -> Result<(), String> {
    if *buffer.get(*ind).ok_or("PARSE_OUT_OF_BOUNDS".to_string())?== b'\r' && *buffer.get(*ind+1).ok_or("PARSE_OUT_OF_BOUNDS".to_string())? == b'\n'{
        *ind = *ind+ 2;
        Ok(())
    }else{
        return Err("PARSE_ERROR_FLUSH".to_string());
    }
}

fn get_slength(buffer: &[u8], ind: &mut usize, message: &str) -> Result<usize, String> {
    let mut command_length = 0;
    while buffer.get(*ind).ok_or("PARSE_OUT_OF_BOUNDS".to_string())?.is_ascii_digit(){
        command_length = command_length*10 + (*buffer.get(*ind).ok_or("PARSE_OUT_OF_BOUNDS".to_string())? - b'0') as usize; 
        *ind = *ind+1;
    }
    
    if command_length == 0{
        return Err(message.to_string());
    }
    Ok(command_length)
}


#[tokio::main]

async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8008").await?;
    let data_set = Arc::new(RwLock::new(HashMap::new()));
    loop {
        let (mut socket, client_addr) = listener.accept().await?;
        let data_set_clone = data_set.clone();
        tokio::spawn(async move{
            println!("Connected to client - {:?}", client_addr);
            handle_client(&mut socket, client_addr, data_set_clone).await;
        });
    }
    
}
