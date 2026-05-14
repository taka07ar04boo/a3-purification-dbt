-- api.a3_portfolios の umaban カラムが馬連形式（XX-YY）または単勝形式（XX）の正規表現に合致しているか確認する
WITH invalid_umaban AS (
    SELECT 
        portfolio_id,
        umaban
    FROM {{ source('api', 'a3_portfolios') }}
    WHERE umaban NOT SIMILAR TO '[0-9]{1,2}(-[0-9]{1,2})*'
)
SELECT * FROM invalid_umaban
