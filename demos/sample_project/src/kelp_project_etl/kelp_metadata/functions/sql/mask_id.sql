CASE
  WHEN id IS NULL THEN NULL
  WHEN length(id) >= 4 THEN CONCAT('XXX-XX-', right(regexp_replace(id, '[^0-9]', ''), 4))
  ELSE 'MASKED'
END
