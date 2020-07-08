use std::collections::HashMap;
use std::convert::TryInto;

use clap::{Arg, App};
use chrono::prelude::*;
use postgres::{Config, NoTls};
use regex::Regex;
use rpassword::prompt_password_stdout;
use walkdir::{WalkDir, DirEntry};

//extern crate chrono;

fn csv_filter(entry: &DirEntry) -> bool {
    let is_folder = entry.file_type().is_dir();
    let file_name = entry.file_name().to_str().unwrap();
    let lowercase_file_name = file_name.to_lowercase();
    let file_ext = lowercase_file_name.split('.').last();

    match file_ext {
        Some(ext) => {
            ext == "csv" || is_folder
        },
        None => {
            false
        },
    }
}

fn cme_month_letter_to_number(letter: &str) -> Result<usize, String> {
    match letter {
        "f" => Ok(1),
        "g" => Ok(2),
        "j" => Ok(4),
        "k" => Ok(5),
        "h" => Ok(3),
        "m" => Ok(6),
        "n" => Ok(7),
        "q" => Ok(8),
        "u" => Ok(9),
        "v" => Ok(10),
        "x" => Ok(11),
        "z" => Ok(12),
        _ => Err(format!("Invalid contract month: '{}'", letter))
    }
}

fn complete_short_year(year: &usize) -> usize {
    if *year >= 40 {
        *year + 1900
    } else if *year < 40 {
        *year + 2000
    } else {
        *year
    } 
}

fn command_usage<'a, 'b>() -> App<'a, 'b> {
    const DEFAULT_HOST: &str = "localhost";
    const DEFAULT_PORT: &str = "5432";
    const DEFAULT_USER: &str = "postgres";

    App::new("csv_to_postgresql")
    .author("Matthew Scheffel <matt@dataheck.com>")
    .about("Inserts market data into a PostgreSQL database from CSV-formatted files. TLS not supported.")
    .long_about("Designed for the insertion of market data exported from MultiCharts QuoteManager.
    Expects a QuoteManager-standard naming convention: SYMBOL-DATASOURCE-EXCHANGE-TYPE-TIMEFRAME-FIELD.csv.
    SYMBOL, EXCHANGE, TYPE, FIELD and TIMEFRAME are used to select the appropriate table to insert to (concatenated with '_'). 
    SYMBOLs are deconstructed if they appear to be Futures, and will be converted to the base symbol with a new CONTRACT field added.
    ")
    .arg(
        Arg::with_name("directory")
            .short("d")
            .long("directory")
            .takes_value(true)
            .help("A directory containing CSV-formatted files.")
            .required(true)
    )
    .arg(
        Arg::with_name("create")
            .short("c")
            .long("create")
            .takes_value(false)
            .help("Create table structure required for insertion")
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
}

fn create_tables(client: &mut postgres::Client) -> Result<usize, postgres::Error> {
    client.batch_execute(r#"
        CREATE TABLE bars (
            "timestamp" timestamp with time zone not null, 
            contract date,
            symbol text collate pg_catalog."default" not null,
            open numeric,
            high numeric,
            low numeric,
            close numeric,
            volume numeric,
            open_interest numeric,
            barsize text not null,
            constraint bars_daily_pkey primary key (symbol, "timestamp")
        )
    "#)?;
    Ok(0)
}

fn main() {
    let matches = command_usage().get_matches();
    
    let postgresql_host = matches.value_of("host").unwrap();
    let postgresql_user = matches.value_of("user").unwrap();
    let postgresql_dbname = matches.value_of("database").unwrap();
    let postgresql_port = matches.value_of("port").unwrap().parse::<u16>().expect(&format!("Invalid port specified: '{}.'", matches.value_of("port").unwrap()));

    println!("Connecting to PostgreSQL {}:{} as user '{}'.", postgresql_host, postgresql_port, postgresql_user);

    let postgresql_pass = prompt_password_stdout("Password: ").unwrap();

    let mut client = Config::new()
                    .host(postgresql_host)
                    .port(postgresql_port)
                    .user(postgresql_user)
                    .dbname(postgresql_dbname)
                    .password(postgresql_pass)
                    .connect(NoTls).unwrap();
    
    if matches.is_present("create") {
        println!("Creating tables.");
        create_tables(&mut client).unwrap();
    }

    let insert_statement = client.prepare(r#"
        INSERT INTO bars ("timestamp", symbol, contract, open, high, low, close, volume, open_interest, barsize) 
        VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"#
    ).unwrap();    

    let target_path = matches.value_of("directory").unwrap();
    println!("Transversing path '{}'", target_path);
    
    let futures_regex = Regex::new(r"^(?i)(?P<root>[@A-Z]+)(?P<month>[FGHJKMNQUVXZ])(?P<year>\d+)$").unwrap();

    struct file_metadata<'a> {
        symbol: &'a str,
        datasource: &'a str,
        exchange: &'a str,
        market: &'a str,
        timeframe: &'a str,
        field: &'a str,
    }

    type Record = HashMap<String, String>;

    for entry in WalkDir::new(target_path).into_iter().filter_entry(|e| csv_filter(e)) {
        let file_path = match entry.as_ref() {
            Ok(e) => {
                if e.file_type().is_file() {
                    e.path()
                } else {
                    continue; // no message required for skipping folders
                }
            },
            Err(e) => {
                println!("Forced to skip entry: {}", e); // file system error?
                continue;
            }
        };

        let lowercase_file_name = file_path.file_name().unwrap().to_str().unwrap().to_lowercase();
        let name_segments: Vec<&str> = lowercase_file_name.split('.').nth(0).unwrap().split('-').collect();
        // 0=symbol, 1=datasource, 2=exchange, 3=type, 4=time, 5=field
        
        if name_segments.len() != 6 {
            println!("Filename does not meet expected pattern ('symbol-datasource-exchange-type-time-field.csv'), skipping. File: {}", lowercase_file_name);
            continue;
        }

        let metadata = file_metadata{
            symbol: name_segments[0], 
            datasource: name_segments[1], 
            exchange: name_segments[2], 
            market: name_segments[3], 
            timeframe: name_segments[4], 
            field: name_segments[5]
        };

        // deconstruct CME futures short contract names, ex: @VXJ20 -> @VX, April, 2020.
        let (symbol_root, contract_month, contract_year) = match futures_regex.captures(name_segments[0]) {
            Some(x) => (
                x.name("root").unwrap().as_str(), // i.e., root of @VXJ20 is @VX
                x.name("month").unwrap().as_str(), 
                x.name("year").unwrap().as_str().parse::<usize>().unwrap()
            ),
            None => (metadata.symbol, "", 0)
        };

        // create a date object, if relevant
        let contract_date:Option<chrono::Date<Utc>> = match (contract_month, contract_year) {
            ("", 0) => None,
            (month, year) => {
                let year_number = complete_short_year(&year);
                let month_number = cme_month_letter_to_number(&month).unwrap();

                Some(Utc.ymd(year_number.try_into().unwrap(), month_number.try_into().unwrap(), 1))
            },
        };

        let table_name = format!(
            "{symbol}_{exchange}_{type}_{field}_{timeframe}",
            symbol=symbol_root,
            exchange=metadata.exchange,
            type=metadata.market,
            field=metadata.field,
            timeframe=metadata.timeframe
        );

        let mut reader = csv::Reader::from_path(file_path);

        println!("Error with file, skipped: {}", lowercase_file_name);

        match reader.as_mut() {
            Ok(r) => {
                let headers = r.headers().unwrap();
                println!("{} {:#?}", table_name, &headers);

                for row_result in r.deserialize() {
                    let row: Record = row_result.unwrap();

                    println!("{:?}", row);
                    break;
                }                
            },
            Err(_) => {
                println!("Error with file, skipped: {}", lowercase_file_name)
            }
        }
    }
}
