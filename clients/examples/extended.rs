use postgres::{Client, Config, Error, NoTls};

fn main() -> Result<(), Error> {
    let host = "localhost";
    let port = 5432;
    // let port = 6543;
    let user = "postgres";
    let dbname = "test";
    let mut client = Config::new()
        .host(host)
        .port(port)
        .user(user)
        .dbname(dbname)
        .password("123456")
        .connect(NoTls)?;

    for row in client.query("SELECT * FROM t1;SELECT 1 + 2;", &[])? {
        let c1: i32 = row.get("c1");
        let c2: i32 = row.get("c2");
        println!("row c1 = {}, c2 = {}", c1, c2);
    }
    Ok(())
}
