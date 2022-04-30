-- name: CreateStockDaily :exec
INSERT INTO stock_daily ( code, trade_date, open, high,low,close,vol,amount )
VALUES
    ( $1, $2, $3, $4,$5,$6,$7,$8 ) ;
