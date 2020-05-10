use plotters::prelude::*;

struct BenchmarkData {
    rust: Vec<f32>,
    jvm: Vec<f32>,
    spark: Vec<f32>,
}

impl BenchmarkData {
    fn min_time(&self) -> f32 {
        let mut min: f32 = 1000000.0;
        for v in &self.rust {
            min = min.min(*v);
        }
        for v in &self.jvm {
            min = min.min(*v);
        }
        for v in &self.spark {
            min = min.min(*v);
        }
        min
    }
    fn max_time(&self) -> f32 {
        let mut max: f32 = 0.0;
        for v in &self.rust {
            max = max.max(*v);
        }
        for v in &self.jvm {
            max = max.max(*v);
        }
        for v in &self.spark {
            max = max.max(*v);
        }
        max
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let color_rust = RGBColor(0xFF, 0x00, 0x00);
    let color_spark = RGBColor(0x00, 0xAA, 0x33);
    let color_jvm = RGBColor(0x00, 0x00, 0xFF);

    let version = "0.2.5";
    let computers = vec!["laptop", "desktop"];
    let formats = vec!["csv", "parquet"];
    let executors = vec!["rust", "spark"]; // "jvm"
    let colors = vec![color_rust, color_spark];
    let cpus = vec![3, 6, 9, 12];

    for computer in &computers {
        for format in &formats {

            let data = read_results(computer, version, format, cpus.clone(), executors.clone());
            let y_max = (60_f32 / data.min_time());

            let filename = format!("/home/andy/git/andygrove/benchmarks/results/{}/{}/benchmarks-{}.svg", version, computer, format);
            println!("Generating {}", filename);

            let root = SVGBackend::new(&filename, (640, 480)).into_drawing_area();

            root.fill(&WHITE)?;
            let mut chart = ChartBuilder::on(&root)
                .caption("Ballista Scalability: CPU Cores", ("sans-serif", 30).into_font())
                .margin(5)
                .x_label_area_size(30)
                .y_label_area_size(30)
                .build_ranged(3f32..12f32, 0f32..y_max)?;

            chart.configure_mesh().draw()?;

            let mut i = 0;
            for bench in &executors {
                let mut bench_points: Vec<(f32, f32)> = vec![];
                let mut line_points = vec![];
                for cpu in &cpus {
                    let seconds = get_time_seconds(bench, *cpu, computer, version, format);
                    println!("{} {} = {}", bench, cpu, seconds);
                    bench_points.push((*cpu as f32, seconds));
                    line_points.push(seconds);
                }

                let data = (0..cpus.len()).map(|i| {
                    (cpus[i] as f32, 60.0 / line_points[i]) // queries per minute
                });


                let color = &colors[i];

                chart
                    .draw_series(LineSeries::new(
                        data,
                        &colors[i],
                    ))?
                    .label(bench.to_owned())
                    .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], color));


                i += 1;
            }

            chart
                .configure_series_labels()
                .background_style(&WHITE.mix(0.8))
                .border_style(&BLACK)
                .draw()?;
        }
    }

    for computer in &computers {
        for format in &formats {
            println!("## {} {}", computer, format);
            println!("|Cores|{}|", executors.join("|"));
            println!("|-----|{}|", executors.iter().map(|_| "---")
                .collect::<Vec<&str>>()
                .join("|"));
            for cpu in &cpus {
                let results: Vec<String> = executors.iter()
                    .map(|bench| format!("{}", get_time_seconds(bench, *cpu, computer, version, format)))
                    .collect::<Vec<String>>();
                println!("|{}|{}|", cpu, results.join("|"));
            }
        }
    }

    Ok(())
}

fn read_results(computer: &str, version: &str, format: &str, cpus: Vec<usize>, executors: Vec<&str>) -> BenchmarkData {

    let mut result = BenchmarkData {
        rust: vec![],
        jvm: vec![],
        spark: vec![]
    };

    for executor in executors {
        for cpu in &cpus {
            let time_s = get_time_seconds(&executor, *cpu, computer, version, format);
            match executor {
                "rust" => result.rust.push(time_s),
                "jvm" => result.jvm.push(time_s),
                "spark" => result.spark.push(time_s),
                _ => panic!()
            }
        }
    }

    result
}

fn get_time_seconds(bench: &str, cpu: usize, hardware: &str, version: &str, format: &str) -> f32 {
    let path = format!("../results/{}/{}/{}-{}-{}cpu.csv", version, hardware, bench, format, cpu);
    //println!("Reading {}", path);
    let mut rdr = csv::Reader::from_path(path).unwrap();
    for result in rdr.records() {
        let record = result.unwrap();
        return record.get(1).unwrap().parse::<f32>().unwrap()/1000.0;
    }
    panic!()
}