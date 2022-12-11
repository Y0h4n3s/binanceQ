use std::str::FromStr;


pub fn change_percent(old: f64, new: f64) -> f64 {
	if old == 0.0 {
		0.0
	} else {
		(new - old) / old * 100.0
	}
}

pub fn normalize(value: f64, min: f64, max: f64) -> f64 {
	(value - min) / (max - min) * 100.0
}

pub fn to_precision(num: f64, precision: usize) -> f64 {
	f64::from_str(format!("{:.*}", precision, num).as_str()).unwrap()
}

