from collections import defaultdict
import re

def deduplicate_and_sort_by_frequency(strings, min_count=2):
    """
    Groups case-insensitive duplicates, keeps the version with most capitals,
    and sorts by frequency (most common first).
    """
    groups = defaultdict(list)
    
    # Group by lowercase version
    for s in strings:
        groups[clean_group_name(s)].append(s)
    
    # Get the canonical version and count for each group
    results = []
    for group in groups.values():
        canonical = max(group, key=lambda s: sum(1 for c in s if c.isupper()))
        count = len(group)
        results.append((canonical, count))
    
    # Sort by count (descending) and return just the strings
    results.sort(key=lambda x: x[1], reverse=True)
    return [canonical for canonical, count in results if count >= min_count]

def clean_group_name(s):
    s = s.lower()
    s = s.replace("-"," ")
    return re.sub(r"\s+"," ",s)