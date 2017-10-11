SELECT
  'Change in cell: ' || CAST(ToCellId(endLon, endLat) AS VARCHAR),
  COUNT(*) OVER (
    PARTITION BY ToCellId(endLon, endLat) ORDER BY eventTime RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
  )
FROM (
  SELECT * FROM TaxiEvents WHERE NOT isStart AND IsInNYC(endLon, endLat)
)
