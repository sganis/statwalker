// compile with: cargo build --bin index
// run: index my.res.csv my.agg.csv

use anyhow::{Context, Result};
use csv::{ReaderBuilder};
use redb::{Database, TableDefinition};
use bincode::{encode_to_vec, config};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};     // <-- add these two imports

#[derive(Encode, Decode, Serialize, Deserialize)]
struct AggRowBin {
    file_count: u64,
    disk_usage: u128,
    latest_mtime: i64,
    users: Vec<String>,
}

#[derive(Encode, Decode, Serialize, Deserialize)]
struct HashRow {
    mtime: i64,
    size: u64,
    hash: String,
}


const AGG: TableDefinition<&str, &[u8]>   = TableDefinition::new("agg");
const HASH: TableDefinition<&str, &[u8]>  = TableDefinition::new("hash");

fn main() -> Result<()> {
    let res = std::env::args().nth(1).expect("res.csv required");
    let agg = std::env::args().nth(2).expect("agg.csv required");

    let stem = PathBuf::from(&res).file_stem().unwrap().to_string_lossy().to_string();
    let agg_redb  = format!("{}.agg.redb",  stem);
    let hash_redb = format!("{}.hash.redb", stem);

    //
    // 1) build HASH index from <stem>.res.csv
    //
    {
        let db = Database::create(&hash_redb)?;
        let write = db.begin_write()?;
        let mut tbl = write.open_table(HASH)?;

        let mut rdr = ReaderBuilder::new().has_headers(true).from_path(&res)?;
        let cfg = config::standard();

        for row in rdr.records() {
            let r = row?;
            let inode   = r.get(0).unwrap();
            let mtime   = r.get(2).unwrap().parse::<i64>()?;
            //let mtime = r.get(2).unwrap().parse::<i64>()
            //    .with_context(|| format!("bad mtime at row {:?} -> {:?}", r.get(8), r.get(2)))?;

            let sizeb   = r.get(7).unwrap().parse::<u64>()?;
            let hashstr = r.get(11).unwrap_or("").to_string();

            if !hashstr.is_empty() {
                let rec = HashRow { mtime, size: sizeb, hash: hashstr };
                let bytes = encode_to_vec(rec, cfg)?;
                tbl.insert(inode, bytes.as_slice())?;
            }
        }
        drop(tbl);
        write.commit()?;
        println!("Wrote hash -> {}", hash_redb);
    }

    //
    // 2) build AGG index from <stem>.agg.csv
    //
    {
        let db = Database::create(&agg_redb)?;
        let write = db.begin_write()?;
        let mut tbl = write.open_table(AGG)?;

        let mut rdr = ReaderBuilder::new().has_headers(false).from_path(&agg)?;
        let cfg = config::standard();

        for rec in rdr.records() {
            let r = rec?;
            let path    = r.get(0).unwrap().to_string();
            let fcount  = r.get(1).unwrap().parse().unwrap();
            let dusage  = r.get(2).unwrap().parse().unwrap();
            let lm      = r.get(3).unwrap().parse().unwrap();
            let users   = r.get(4).unwrap().split('|').filter(|x| !x.is_empty()).map(|s| s.to_string()).collect();

            let row = AggRowBin { file_count: fcount, disk_usage: dusage, latest_mtime: lm, users };
            let bytes = encode_to_vec(row, cfg)?;
            tbl.insert(path.as_str(), bytes.as_slice())?;
        }
        drop(tbl);
        write.commit()?;
        println!("Wrote agg  -> {}", agg_redb);
    }
    Ok(())
}
