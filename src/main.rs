use rldb::storage::StorageOptions;
use rldb::Database;
use rustyline::error::ReadlineError;
use rustyline::Editor;

fn main() {
    env_logger::init();

    let db = Database::new(StorageOptions {
        base_path: "risinglight.db".into(),
    });
    let mut rl = Editor::<()>::new().expect("new line editor error");

    loop {
        match rl.readline("> ") {
            Ok(line) => {
                rl.add_history_entry(&line);

                let ret = db.run_sql(&line);
                match ret {
                    Ok(chunks) => {
                        for chunk in chunks {
                            println!("{}", chunk);
                        }
                    }
                    Err(err) => println!("{}", err),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("Bye!");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("Exited");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
}
