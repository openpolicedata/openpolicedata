import re

def camel_case_split(x):
    if len(x)==0:
        return x
    words = []
    cur = x[0]
    for l in x[1:]:
        if l.isupper() and cur[-1].islower():
            words.append(cur)
            cur = l
        else:
            cur+=l

    words.append(cur)
    return words


def split_words(string):
    # Split based on spaces, punctuation, and camel case
    words = list(re.split(r"[^A-Za-z]+", string))
    k = 0
    while k < len(words):
        new_words = camel_case_split(words[k])
        words[k] = new_words[0]
        for j in range(1, len(new_words)):
            words.insert(k+1, new_words[j])
            k+=1
        k+=1

    return words