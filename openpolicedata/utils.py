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


def split_words(string, case=None):
    # Split based on spaces, punctuation, and camel case
    words = list(re.split(r"[^A-Za-z]+", string))
    k = 0
    while k < len(words):
        if len(words[k])==0:
            del words[k]
            continue
        new_words = camel_case_split(words[k])
        words[k] = new_words[0]
        for j in range(1, len(new_words)):
            words.insert(k+1, new_words[j])
            k+=1
        k+=1

    if case!=None:
        if case.lower()=='lower':
            words = [x.lower() for x in words]
        elif case.lower()=='upper':
            words = [x.upper() for x in words]
        else:
            raise ValueError("Unknown input case")

    return words

def is_str_number(x):
    is_num = isinstance(x,str)
    if is_num:
        x = x.strip()
        if not x.isdigit():
            # Try removing decimal
            dec = x.find('.')
            is_num = dec>0 and x[:dec].isdigit() and x[dec+1:].isdigit()
    return is_num