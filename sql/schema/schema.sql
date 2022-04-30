CREATE TABLE "stock_daily" (
  "id" bigserial PRIMARY KEY,
  "code" varchar(255) NOT NULL,
  "trade_date" date NOT NULL,
  "open" numeric(32,2) NOT NULL,
  "high" numeric(32,2) NOT NULL,
  "low" numeric(32,2) NOT NULL,
  "close" numeric(32,2) NOT NULL,
  "vol" int8 NOT NULL,
  "amount" numeric(255,2) NOT NULL,
  "create_at" timestamptz(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;

CREATE TABLE "stock_brief" (
  "id" bigserial PRIMARY KEY,
  "code" varchar(255) NOT NULL,
  "last_trade_date" date NOT NULL,
  "create_at" timestamptz(6) NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;

CREATE UNIQUE INDEX idx_code ON stock_brief(code);