use plotters::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let color_rust = RGBColor(0xFF, 0x00, 0x00);
    let color_spark = RGBColor(0x00, 0xAA, 0x33);
    let color_jvm = RGBColor(0x00, 0x00, 0xFF);

    let version = "0.2.5-SNAPSHOT";
    let computers = vec!["laptop", "desktop"];
    let benches = vec!["rust", "jvm", "spark"];
    let colors = vec![color_rust, color_jvm, color_spark];
    let cpus = vec![3, 6, 9, 12];

    for computer in &computers {

        let filename = format!("bench-{}-{}.svg", computer, version);
        println!("Generating {}", filename);

        let root = SVGBackend::new(&filename, (640, 480)).into_drawing_area();

        root.fill(&WHITE)?;
        let mut chart = ChartBuilder::on(&root)
            .caption("Ballista Scalability: CPU Cores", ("sans-serif", 30).into_font())
            .margin(5)
            .x_label_area_size(30)
            .y_label_area_size(30)
            .build_ranged(3f32..12f32, 0f32..15f32)?;

        chart.configure_mesh().draw()?;

        let mut i = 0;
        for bench in &benches {
            let mut bench_points: Vec<(f32,f32)> = vec![];
            let mut line_points = vec![];
            for cpu in &cpus {
                let seconds = get_time_seconds(bench, *cpu, computer, version);
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

    for computer in &computers {
        println!("## {}", computer);
        println!("|Cores|{}|", benches.join("|"));
        println!("|-----|{}|", benches.iter().map(|_| "---")
            .collect::<Vec<&str>>()
            .join("|"));
        for cpu in &cpus {
            let results: Vec<String> = benches.iter()
                .map(|bench| format!("{}", get_time_seconds(bench, *cpu, computer, version)))
                .collect::<Vec<String>>();
            println!("|{}|{}|", cpu, results.join("|"));
        }
    }

    Ok(())
}

fn get_time_seconds(bench: &str, cpu: usize, hardware: &str, version: &str) -> f32 {
    let path = format!("../results/{}/{}/{}-{}cpu-2048mb.txt", version, hardware, bench, cpu);
    let mut rdr = csv::Reader::from_path(path).unwrap();
    for result in rdr.records() {
        let record = result.unwrap();
        return record.get(1).unwrap().parse::<f32>().unwrap()/1000.0;
    }
    panic!()
}