use serde::Deserialize;
use error_chain::error_chain;



error_chain! {
    errors {
     }

    foreign_links {
        BinanceError(binance::errors::Error);
    }
}
