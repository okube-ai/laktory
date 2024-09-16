SELECT
    cast(data.created_at AS TIMESTAMP) AS created_at,
    data.symbol AS symbol,
    data.open AS open,
    data.close AS close
FROM
    {df}
