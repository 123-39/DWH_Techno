# Inner-join на MapReduce (ReduceSideJoin)

---
### Задача: Написать inner-join на MapReduce (ReduceSideJoin) для двух таблиц по полю product_id:
- shop_product.csv содержит поля product_id и description
- shop_price.csv содержит поля product_id и price

В результате должен получится файл из трех колонок (product_id, description, price)

---
### Команды для запуска:
``` bash
mapred streaming -D mapred.reduce.tasks=0 -input /$Path_to_the_file/shop_product.csv -output /$Path_to_the_file/product -mapper mr3_product_mapper.py -file /$Path_to_the_file/mr3_product_mapper.py

mapred streaming -D mapred.reduce.tasks=0 -input /$Path_to_the_file/shop_price.csv -output /$Path_to_the_file/price -mapper mr3_price_mapper.py -file /$Path_to_the_file/mr3_price_mapper.py

mapred streaming -D mapred.reduce.tas=1 -input /$Path_to_the_file/product -input $Path_to_the_file/price -output /$Path_to_the_file/mr3_join -mapper mr3_join_mapper.py -reducer mr3_join_reducer.py -file /$Path_to_the_file/mr3_join_mapper.py -file /$Path_to_the_file/mr3_join_reducer.py
```