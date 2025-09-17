use chrono::Local;

fn main() {
    let iso_date = Local::now().date_naive();
    println!("cargo:rustc-env=BUILD_DATE={}", iso_date);
}