-- name: CreateStockDaily :exec
INSERT INTO stock_daily ( code, trade_date, open, high,low,close,vol,amount )
VALUES
    ( $1, $2, $3, $4, $5, $6, $7, $8 ) ;

-- name: CreateStockBrief :exec
INSERT INTO stock_brief ( code, last_trade_date)
VALUES
    ( $1, $2 ) ;

-- name: GetStockBrief :one
SELECT * FROM stock_brief WHERE code = $1 LIMIT 1 ;

-- name: UpdateStockBrief :exec
UPDATE stock_brief SET last_trade_date = $1 WHERE code = $2;