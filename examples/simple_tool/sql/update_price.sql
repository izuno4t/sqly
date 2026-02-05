UPDATE products
SET price = CAST(price * /* rate */1.0 AS INTEGER)
WHERE category = /* category */'electronics'
