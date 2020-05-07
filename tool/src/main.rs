use std::io::Result;

use plotlib::page::Page;
use plotlib::repr::Plot;
use plotlib::style::{LineJoin, LineStyle};
use plotlib::view::ContinuousView;

fn main() -> Result<()> {

    let benches = vec!["rust", "spark", "jvm"];
    let colors = vec!["burlywood", "green", "blue"];
    let cpus = vec![3, 6, 9, 12];

    let mut view = ContinuousView::new()
        .x_label("Number of CPU Cores")
        .x_range(3.0, 12.0)
        .y_range(0.0, 150.0)
        .y_label("Time (seconds)");

    let mut i = 0;
    for bench in &benches {
        let mut bench_points = vec![];
        for cpu in &cpus {
            let seconds = get_time_seconds(bench, *cpu)?;
            println!("{} {} = {}", bench, cpu, seconds);
            bench_points.push((*cpu as f64, seconds));
        }
        let l1 = Plot::new(bench_points).line_style(
            LineStyle::new()
                .colour(colors[i])
                .linejoin(LineJoin::Miter),
        );
        view = view.add(l1);
        i += 1;
    }

    let filename = "benchmarks.svg";
    println!("Generating {}", filename);
    Page::single(&view).save(filename).expect("saving svg");

    Ok(())
}

fn get_time_seconds(bench: &str, cpu: usize) -> Result<f64> {
    let path = format!("../results/0.2.5-SNAPSHOT/laptop/{}-{}cpu-2048mb.txt", bench, cpu);
    let mut rdr = csv::Reader::from_path(path)?;
    for result in rdr.records() {
        let record = result?;
        return Ok(record.get(1).unwrap().parse::<f64>().unwrap()/1000.0);
    }
    panic!()
}