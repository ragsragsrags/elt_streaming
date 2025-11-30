SELECT
	MIN(VF.[valid_from])
FROM
	(
		SELECT TOP 2
			[valid_from]
		FROM
			(
				SELECT
					valid_from
				FROM
					customers

				UNION

				SELECT
					valid_from
				FROM
					products

				UNION

				SELECT
					valid_from
				FROM
					products_on_hand

				UNION

				SELECT
					valid_from
				FROM
					orders
			) VF
		ORDER BY
			valid_from desc
	) VF