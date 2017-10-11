SELECT ToCoords(cell), wstart, wend, isStart, popCnt
FROM (
	SELECT 
		cell,
		isStart,
		HOP_START(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wstart,
		HOP_END(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE) AS wend,
		COUNT(isStart) AS popCnt
	FROM (
		SELECT
			eventTime,
			isStart,
			CASE WHEN isStart THEN ToCellId(startLon, startLat) ELSE ToCellId(endLon, endLat) END AS cell
		FROM TaxiEvents
		WHERE IsInNYC(startLon, startLat) AND IsInNYC(endLon, endLat)
	)
	GROUP BY cell, isStart, HOP(eventTime, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)
)
WHERE popCnt > 20
