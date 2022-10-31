use postgres::config::SslMode;
use postgres::{Client, Config, Error, NoTls, SimpleQueryMessage};
use std::fmt::Write;

fn main() -> Result<(), Error> {
    let host = "localhost";
    // let port = 5432;
    let port = 6543;
    let user = "postgres";
    let dbname = "test";
    let mut client = Config::new()
        .host(host)
        .port(port)
        .user(user)
        .dbname(dbname)
        .password("123456")
        .connect(NoTls)?;

    let mut need_header = true;
    for rsp in client.simple_query("SELECT * FROM t1;")? {
        match rsp {
            SimpleQueryMessage::Row(simple_query_row) => {
                let columns = simple_query_row.columns();
                if need_header {
                    let mut row = "".to_string();
                    for c in columns {
                        write!(row, "{} |\n", c.name());
                    }
                    need_header = false;
                }

                let mut row = "".to_string();
                for i in 0..simple_query_row.len() {
                    let r = simple_query_row.get(i);
                    write!(row, "{:?} |", r);
                }
                println!("{}", row);
            }
            SimpleQueryMessage::CommandComplete(n) => {
                println!("rows completed: {}", n)
            }
            _ => {
                println!("others unknown")
            }
        }
    }

    Ok(())
}
