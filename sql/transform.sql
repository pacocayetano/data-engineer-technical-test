MERGE `data-engineer-test-491612.INTEGRATION.integration_prueba_tecnica` AS target
USING (
  SELECT
    id,
    postId,
    name,
    email,
    body,
    CURRENT_DATE() AS load_date
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_num
    FROM `data-engineer-test-491612.SANDBOX_api_test.api_comments`
  )
  WHERE row_num = 1
) AS source
ON target.id = source.id
WHEN NOT MATCHED THEN
  INSERT (id, postId, name, email, body, load_date)
  VALUES (source.id, source.postId, source.name, source.email, source.body, source.load_date);
