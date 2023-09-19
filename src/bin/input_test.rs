// use tokio::io::{AsyncRead};

use std::io::{self, Write};



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 
    let mut buffer = String::new();
    let stdin = io::stdin();
    // let res = stdin.read_line(&mut buffer)?;
    
    
    // println!("{:?}",  str::from_utf8(res));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Ok(res) = async {
                    stdin.read_line(&mut buffer)
                } => {
                    print!("{:?}",  res);
                    match io::stdout().flush() {
                        Ok(_) => print!("\n"),
                        Err(error) => panic!("Dumb bitch error: {}", error),
                    }
                }
                else => {}
            };
        }

    });
    println!("Listening socket ready");

    loop {}
    Ok(())
}