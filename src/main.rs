use std::io;
use tokio::net::TcpListener;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::TcpStream;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn handle_client(socket: &mut TcpStream, client_addr: SocketAddr, data_set: Arc<RwLock<HashMap<String, String>>>){
    let mut buffer = [0; 512];
    loop {
        match socket.read(&mut buffer).await{
            Ok(0) => break,
            Ok(n) => {
                // match socket.write(&buffer[0..n]).await{
                //     Ok(_)=> (),
                //     Err(_e) => println!("Socket write failed"),
                // };
                
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

async fn handle_get(socket: &mut TcpStream, key: &String, data_set: &Arc<RwLock<HashMap<String, String>>>){
    let map = data_set.read().await;
    match map.get(key){
        Some(res) => {
                let len = (*res).len();
                let res_string = format!("${}\r\n{}\r\n", len, *res);
                match socket.write(res_string.as_bytes()).await{
                Ok(_) => (),
                Err(_e) => println!("Socket write failed!")
            }
        },
        _ => match socket.write(b"$-1\r\n").await{
            Ok(_) => (),
            Err(_e) => println!("Socket write failed!"),
        }
    }
}

async fn handle_set(socket: &mut TcpStream, key: &String, val: &String, data_set: &Arc<RwLock<HashMap<String, String>>>){
    let mut map = data_set.write().await;
    map.insert((*key).to_string(), (*val).to_string());
    match socket.write(b"+OK\r\n").await{
        Ok(_) => (),
        Err(_e) => println!("Socket write failed!"),
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
