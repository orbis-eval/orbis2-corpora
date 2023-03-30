#!/usr/bin/env python3

from zstd import loads
from csv import reader



with open("ner_dataset.csv.zst", "rb") as f:
    content = loads(f.read()).decode("latin1")

    my_sentence = ""
    quote_open = False
    last = ""
    annotations = []
    for sentence, word, pos, tag in reader(content.strip().split("\n")[1:]):
        if sentence and my_sentence:
            print(my_sentence)
            my_sentence = ""
            quote_open = False
            last = ""

        if my_sentence and (len(word) > 2 or word.isalnum()) and (last != '"' or not quote_open):
            left = " "
        elif word == '"' and not quote_open:
            left = " "
        else:
            left = ""

        if word == '"':
            quote_open = not quote_open

        my_sentence += left
        my_sentence += word
        if tag: print(tag)
        last = word

