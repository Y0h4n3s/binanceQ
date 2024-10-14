use crate::types::Symbol;
use rust_decimal::Decimal;

#[derive(Clone, Debug, Default)]
pub struct Spread {
    pub symbol: Symbol,
    pub spread: Decimal,
    pub time: u64,
}

impl Spread {
    pub fn new(symbol: Symbol) -> Self {
        Self {
            symbol,
            spread: Decimal::ZERO,
            time: 0,
        }
    }

    pub fn update(&mut self, price: Decimal, time: u64) {
        self.spread = price;
        self.time = time;
    }
}
