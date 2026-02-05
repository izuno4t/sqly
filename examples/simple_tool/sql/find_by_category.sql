SELECT id, name, price, category
FROM products
WHERE 1 = 1
    AND category = /* $category */'electronics'
ORDER BY category, name
