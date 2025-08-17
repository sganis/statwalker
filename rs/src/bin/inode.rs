// compile with: cargo build --bin index
// run: index my.res.csv my.agg.csv

use anyhow::{Context, Result};
use csv::{ReaderBuilder};
use redb::{Database, ReadableDatabase, TableDefinition};
use bincode::{encode_to_vec, config};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};     // <-- add these two imports

#[derive(Encode, Decode, Serialize, Deserialize)]
struct Row {
    inode: String,    
    atime: i64,    
    mtime: i64,
    uid: i64,
    gid: i64,
    mode: i64,
    size: u64,
    disk: u64,
    cat: String,
    hash: String,
}
#[derive(Encode, Decode, Serialize, Deserialize)]
struct HashRow {    
    mtime: i64,
    size: u64,
    hash: String,
}

const PATH: TableDefinition<&str, &[u8]>   = TableDefinition::new("path");
const HASH: TableDefinition<&str, &[u8]>   = TableDefinition::new("hash");

fn main() -> Result<()> {
    let res = std::env::args().nth(1).expect("stats.csv required");
    let stem = PathBuf::from(&res).file_stem().unwrap().to_string_lossy().to_string();
    let redb = format!("{}.redb", stem);

    //
    // 1) build PATH index from <stem>.csv
    //
    {
        let db = Database::create(&redb)?;
        let write = db.begin_write()?;
        let mut tbl = write.open_table(PATH)?;

        let mut rdr = ReaderBuilder::new().has_headers(true).from_path(&res)?;
        let cfg = config::standard();

        for row in rdr.records() {
            let r = row?;
            let inode   = r.get(0).unwrap_or("").to_string();
            let atime   = r.get(1).unwrap().parse::<i64>()?;
            let mtime   = r.get(2).unwrap().parse::<i64>()?;
            let uid   = r.get(3).unwrap().parse::<i64>()?;
            let gid   = r.get(4).unwrap().parse::<i64>()?;
            let mode   = r.get(5).unwrap().parse::<i64>()?;
            let size   = r.get(6).unwrap().parse::<u64>()?;
            let disk   = r.get(7).unwrap().parse::<u64>()?;
            let path = r.get(8).unwrap();
            let cat = r.get(10).unwrap_or("").to_string();
            let hash = r.get(10).unwrap_or("").to_string();

            let rec = Row { inode, atime, mtime, uid, gid, mode, size, disk, cat, hash };
            let bytes = encode_to_vec(rec, cfg)?;
            tbl.insert(path, bytes.as_slice())?;
        }
        drop(tbl);
        write.commit()?;
        println!("Wrote -> {}", redb);
    }

    //
    // 1) build HASH index from <stem>.csv
    //
    // {
    //     let db = Database::open(&redb)?;     
    //     let write = db.begin_write()?;
    //     let mut tbl = write.open_table(HASH)?;

    //     let mut rdr = ReaderBuilder::new().has_headers(true).from_path(&res)?;
    //     let cfg = config::standard();

    //     for row in rdr.records() {
    //         let r = row?;
    //         let inode   = r.get(0).unwrap();
    //         let mtime   = r.get(2).unwrap().parse::<i64>()?;
    //         let sizeb   = r.get(7).unwrap().parse::<u64>()?;
    //         let hashstr = r.get(11).unwrap_or("").to_string();
            
    //         if !hashstr.is_empty() {        
    //             let rec = HashRow { mtime, size: sizeb, hash: hashstr };
    //             let bytes = encode_to_vec(rec, cfg)?;
    //             tbl.insert(inode, bytes.as_slice())?;            
    //         }
    //     }

    //     drop(tbl);
    //     write.commit()?;
    //     println!("Wrote -> {}", redb);
    // }

    Ok(())
}
