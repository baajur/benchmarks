use std::fs::File;
use std::io::{BufReader, BufRead, BufWriter, Error, Write};

fn main() -> Result<(), Error> {
    let file = File::open("/mnt/tpch/100/lineitem.tbl")?;
    let lines = BufReader::new(file).lines();
    let rows_per_partition = 6250400;
    let mut counter = 0;
    let mut partition = 0;
    let mut writer: Option<BufWriter<_>> = None;
    for line in lines {
        let line = line?;
        if counter % rows_per_partition == 0 {
            let filename = format!("part-{}.tbl", partition);
            println!("Writing {} ...", filename);
            writer = Some(BufWriter::new(File::create(filename)?));
            partition += 1;
        }
        if let Some(writer) = writer.as_mut() {
            writer.write(line.as_bytes())?;
            writer.write("\n".as_bytes())?;
        }
        counter += 1;
    }
    println!("{}", counter);
    Ok(())
}
