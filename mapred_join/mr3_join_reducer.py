#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_join_reducer.py"""

import sys

# Ваш код
previous_key = None
product = ""
price = ""
check_numb = 0
prices = []

# читаем строки из STDIN (standard input)
for line in sys.stdin:
    # Подготавливаем строку: удаляем пробелы в начале и конце
    prepared_line = line.strip()

    # Парсим строку: делим по первому \t
    key, value = prepared_line.split('\t', 1)
    # Т.к. все записи с одиннаковыми ключами попадут на один reducer и 
    # записи отсортированы по ключу на этапе shuffle, 
    # то можем подсчитать средню длину слов на каждую букву
    if key == previous_key:
        if value.find(",") != -1:
            product = value
        else:
            prices.append(value)
        check_numb += 1
    else:
        if (previous_key) and (check_numb >= 2):
            check_numb = 0
            for i in prices:
                print '{key}\t{product}\t{price}'.format(key=previous_key, product=product, price=i)
            prices = []
        previous_key = key
        prices = []
        if value.find(",") != -1:
            product = value
        else:
            prices.append(value)
        check_numb += 1

if (previous_key) and (check_numb >= 2):
    check_numb = 0
    for i in prices:
        print '{key}\t{product}\t{price}'.format(key=previous_key, product=product, price=i)
    prices = []

