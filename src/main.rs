extern crate rpassword;

use clap::{Arg, App};
use postgres::{Client, NoTls, Config};

fn main() {
    const DEFAULT_HOST: &str = "localhost";
    const DEFAULT_PORT: &str = "5432";
    const DEFAULT_USER: &str = "postgres";

    let matches = App::new("csv_to_postgresql")
        .author("Matthew Scheffel <matt@dataheck.com>")
        .about("Inserts market data into a PostgreSQL database from CSV-formatted files. TLS not supported.")
        .arg(
            Arg::with_name("directory")
                .short("d")
                .long("directory")
                .takes_value(true)
                .help("A directory containing CSV-formatted files.")
                .required(true)
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .takes_value(true)
                .default_value(DEFAULT_HOST)
                .help("The hostname of the PostgreSQL server to connect to.")
        )
        .arg(
            Arg::with_name("database")
                .short("b")
                .long("database")
                .takes_value(true)
                .required(true)
                .help("The database to USE on the PostgreSQL server.")
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .takes_value(true)
                .default_value(DEFAULT_PORT)
                .help("The port to connect to the PostgreSQL server on.")
        )
        .arg(
            Arg::with_name("user")
                .short("u")
                .long("user")
                .takes_value(true)
                .default_value(DEFAULT_USER)
                .help("The user to connect to the PostgreSQL server with.")
        )
        .get_matches();
    
    let postgresql_host = matches.value_of("host").unwrap();
    let postgresql_user = matches.value_of("user").unwrap();
    let postgresql_port:u16 = matches.value_of("port").unwrap().parse::<u16>().expect(&format!("Invalid port specified: '{}.'", matches.value_of("port").unwrap()));

    println!("Connecting to PostgreSQL {}:{} as user '{}'.", postgresql_host, postgresql_port, postgresql_user);

    let postgresql_pass = rpassword::prompt_password_stdout("Password: ").unwrap();

    let client = Config::new()
                    .host(postgresql_host)
                    .port(postgresql_port)
                    .user(postgresql_user)
                    .password(postgresql_pass)
                    .connect(NoTls).unwrap();
    
    println!("stuff");

}
