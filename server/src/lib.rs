use std::{error::Error, fs};

#[derive(Debug)]
pub struct Config {
    pub server_id: u16,
    pub port_no: u16,
    pub peer_ports: Vec<String>,
}

impl Config {
    pub fn build(args: &[String]) -> Result<Config, Box<dyn Error>> {
        if args.len() < 4 {
            return Err("Usage: cargo run -p server <server_id> <server_port> <config_file with peer ports>".into())
        }
        let server_id: u16 = args[1].parse().expect("Please enter a number!");
        let port_no: u16 = args[2].parse().expect("Please enter a valid port no");
        let config_file = &args[3];

        let contents = fs::read_to_string(config_file)?;
        let peer_ports: Vec<String> = contents
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(|l| l.parse::<u16>().unwrap())
            .filter(|p| *p != port_no)
            .map(|p| format!("[::1]:{}", p))
            .collect(); 

        Ok(Self {
            server_id, 
            port_no,
            peer_ports,
        })
    } 
}
