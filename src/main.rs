use std::collections::HashMap;

extern crate spmc;

use clap::{Arg, App};
use postgres::{Config, NoTls};
use postgres::types::Type;
use regex::Regex;
use rpassword::prompt_password_stdout;
use walkdir::{WalkDir, DirEntry};

use std::thread;
use std::sync::Arc;

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
    const DEFAULT_THREADS: &str = "10";

    App::new("csv_to_postgresql")
    .author("Matthew Scheffel <matt@dataheck.com>")
    .about("Inserts market data into a PostgreSQL database from CSV-formatted files. TLS not supported.")
    .long_about("Designed for the insertion of market data exported from MultiCharts QuoteManager.
    Expects a QuoteManager-standard naming convention: SYMBOL-DATASOURCE-EXCHANGE-TYPE-TIMEFRAME-FIELD.csv.
    SYMBOLs are deconstructed if they appear to be Futures, and will be converted to the base symbol with a new CONTRACT field added.
    Will create its own table, 'bars', if executed with --create.
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
    .arg(
        Arg::with_name("threads")
            .short("t")
            .long("threads")
            .takes_value(true)
            .default_value(DEFAULT_THREADS)
            .help("The number of threads (and PostgreSQL connections) to use for insertion.")
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
            constraint bars_daily_pkey primary key (symbol, barsize, contract, "timestamp")
        );
        CREATE INDEX symbol_idx ON bars (symbol);
        CREATE INDEX symbol_contract_idx ON bars (symbol, contract);
    "#)?;
    Ok(0)
}

fn prepare_client(host: Arc<String>, port: Arc<u16>, user: Arc<String>, dbname: Arc<String>, password: Arc<String>) -> postgres::Client {
    let client = Config::new()
        .host(&host)
        .port(*port)
        .user(&user)
        .dbname(&dbname)
        .password(password.to_string())
        .connect(NoTls).unwrap();

    client
}

type Record = HashMap<String, String>;

struct FileMetadata<'a> {
    symbol: &'a str,
    datasource: &'a str,
    exchange: &'a str,
    market: &'a str,
    timeframe: &'a str,
    field: &'a str,
}

fn process_file(entry_value: DirEntry, futures_regex: Arc<regex::Regex>, client: &mut postgres::Client) {
    let lowercase_file_name = entry_value.path().file_stem().unwrap().to_str().unwrap().to_lowercase();
    let name_segments: Vec<&str> = lowercase_file_name.split('-').collect();
    // 0=symbol, 1=datasource, 2=exchange, 3=type, 4=time, 5=field
    
    if name_segments.len() != 6 {
        println!("Filename does not meet expected pattern ('symbol-datasource-exchange-type-time-field.csv'), skipping. File: {}", lowercase_file_name);
        return;
    }

    let metadata = FileMetadata{
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

    let contract_date:String = match (contract_month, contract_year) {
        ("", 0) => String::from(""),
        (month, year) => {
            let year_number = complete_short_year(&year);
            let month_number = cme_month_letter_to_number(&month).unwrap();

            format!("{year}-{month:02}-{day:02}", year=year_number, month=month_number, day=1)
        },
    };

    if metadata.timeframe != "day" && metadata.timeframe != "minute" {
        println!("Timeframe not supported, skipped: {}", lowercase_file_name);
        return;
    }

    let mut reader = csv::Reader::from_path(entry_value.path());

    match reader.as_mut() {
        Ok(r) => {
            let insert_day_statement = client.prepare_typed(
                r#"
                INSERT INTO bars ("timestamp", symbol, contract, open, high, low, close, volume, barsize) 
                VALUES(
                TO_TIMESTAMP($1, 'YYYY-MM-DD'), $2, TO_TIMESTAMP($3, 'YYYY-MM-DD'), CAST($4 AS numeric), CAST($5 AS numeric),
                CAST($6 AS numeric), CAST($7 AS numeric), CAST($8 AS numeric), $9
                )
                ON CONFLICT ON CONSTRAINT bars_daily_pkey DO NOTHING"#,
                &[Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT]
            ).unwrap();    
        
            let insert_minute_statement = client.prepare_typed(
                r#"
                INSERT INTO bars ("timestamp", symbol, contract, open, high, low, close, volume, barsize) 
                VALUES(
                TO_TIMESTAMP(CONCAT($1, ' ', $2), 'YYYY-MM-DD HH24:MI:SS'), $3, TO_TIMESTAMP($4, 'YYYY-MM-DD'), CAST($5 AS numeric), CAST($6 AS numeric),
                CAST($7 AS numeric), CAST($8 AS numeric), CAST($9 AS numeric), $10
                )
                ON CONFLICT ON CONSTRAINT bars_daily_pkey DO NOTHING"#,
                &[Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT, Type::TEXT]
            ).unwrap();

            for row_result in r.deserialize() {
                let row: Record = row_result.unwrap();

                match metadata.timeframe {
                    "day" => {
                        client.execute(
                            &insert_day_statement, 
                            &[
                                &row["Date"], &symbol_root, &contract_date, 
                                &row["Open"], &row["High"], &row["Low"], &row["Close"], &row["TotalVolume"],
                                &metadata.timeframe
                            ]
                        ).unwrap();
                    },
                    "minute" => {
                        client.execute(
                            &insert_minute_statement,
                            &[
                                &row["Date"], &row["Time"], &symbol_root, &contract_date, 
                                &row["Open"], &row["High"], &row["Low"], &row["Close"], &row["TotalVolume"],
                                &metadata.timeframe
                            ]
                        ).unwrap();
                    },
                    _ => {
                        break; // should be impossible, we checked earlier
                    }
                }
            }                
        },
        Err(_) => {
            println!("Error with file, skipped: {}", lowercase_file_name)
        }
    }
}

fn main() {
    let matches = command_usage().get_matches();
    
    let postgresql_host = Arc::new(matches.value_of("host").unwrap().to_string());
    let postgresql_user = Arc::new(matches.value_of("user").unwrap().to_string());
    let postgresql_dbname = Arc::new(matches.value_of("database").unwrap().to_string());
    let postgresql_port = Arc::new(matches.value_of("port").unwrap().parse::<u16>().expect(&format!("Invalid port specified: '{}.'", matches.value_of("port").unwrap())));
    let max_threads = matches.value_of("threads").unwrap().parse::<usize>().expect(&format!("Invalid thread count specified: '{}.'", matches.value_of("threads").unwrap()));

    println!("Connecting to PostgreSQL {}:{} as user '{}'.", postgresql_host, postgresql_port, postgresql_user);

    let postgresql_pass = Arc::new(prompt_password_stdout("Password: ").unwrap());

    if matches.is_present("create") {
        println!("Creating tables.");
        
        let postgresql_host = postgresql_host.clone();
        let postgresql_port = postgresql_port.clone();
        let postgresql_user = postgresql_user.clone();
        let postgresql_dbname = postgresql_dbname.clone();
        let postgresql_pass = postgresql_pass.clone();

        let mut client = prepare_client(
            postgresql_host, 
            postgresql_port, 
            postgresql_user, 
            postgresql_dbname, 
            postgresql_pass
        );
        create_tables(&mut client).unwrap();
    }

    let futures_regex = std::sync::Arc::new(Regex::new(r"^(?i)(?P<root>[@A-Z]+)(?P<month>[FGHJKMNQUVXZ])(?P<year>\d+)$").unwrap());

    let target_path = matches.value_of("directory").unwrap();
    println!("Transversing path '{}'", target_path);

    let (mut tx, rx) = spmc::channel();
    let mut thread_handles = Vec::new();

    for _n in 0..max_threads {
        let rx = rx.clone();

        let futures_regex = futures_regex.clone();
        let postgresql_host = postgresql_host.clone();
        let postgresql_port = postgresql_port.clone();
        let postgresql_user = postgresql_user.clone();
        let postgresql_dbname = postgresql_dbname.clone();
        let postgresql_pass = postgresql_pass.clone();        

        thread_handles.push(thread::spawn(move || {
            let postgresql_host = postgresql_host.clone();
            let postgresql_port = postgresql_port.clone();
            let postgresql_user = postgresql_user.clone();
            let postgresql_dbname = postgresql_dbname.clone();
            let postgresql_pass = postgresql_pass.clone();

            let mut client = prepare_client(
                postgresql_host, 
                postgresql_port, 
                postgresql_user, 
                postgresql_dbname, 
                postgresql_pass
            );

            loop {
                let futures_regex = futures_regex.clone();

                let entry_result = rx.recv();
                match entry_result {
                    Ok(entry_value) => {
                        process_file(
                            entry_value, 
                            futures_regex, 
                            &mut client
                        );
                    },
                    Err(_) => {
                        println!("All work complete, thread shutdown.");
                        break;
                    }
                }
            }
        }));
    }

    for entry in WalkDir::new(target_path).into_iter().filter_entry(|e| csv_filter(e)) {
        match entry {
            Ok(e) => {
                if e.file_type().is_file() {
                    tx.send(e).unwrap();
                } else {
                    continue; // no message required for skipping folders
                }
            },
            Err(e) => {
                println!("Forced to skip entry: {}", e); // file system error?
                continue;
            }
        };  
    }

    for handle in thread_handles {
        handle.join().unwrap();
    }
}
