use postgres::{Client, Error, NoTls};

fn main() -> Result<(), Error> {
    let mut client = Client::connect(
        "host=localhost port=6432 user=ming",
        NoTls,
    )?;
    for row in client.query("SELECT * FROM test;", &[])? {
        let c1: i32 = row.get("c1");
        let c2: i32 = row.get("c2");
        println!("row c1 = {}, c2 = {}", c1, c2);
    }
    Ok(())
}
