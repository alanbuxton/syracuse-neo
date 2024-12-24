import re
import string

def cache_friendly(key):
    no_punct = re.sub(rf"[{string.punctuation} ]","_",key)
    cleaned = re.sub(r"_{2,}","_",no_punct)
    if len(cleaned) > 230:
        cleaned = cleaned[:210] + str(hash(cleaned[210:]))
    return cleaned
