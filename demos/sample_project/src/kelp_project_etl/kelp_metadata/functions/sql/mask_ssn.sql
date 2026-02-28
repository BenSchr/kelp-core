CASE
  WHEN ssn IS NULL THEN NULL
  WHEN length(ssn) >= 4 THEN CONCAT('XXX-XX-', right(regexp_replace(ssn, '[^0-9]', ''), 4))
  ELSE 'MASKED'
END
