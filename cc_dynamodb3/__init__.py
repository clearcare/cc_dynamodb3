

# Fix a schematics py3.11 incompatibility
try:
    from collections import Iterable
except:
    import collections
    import collections.abc
    collections.Iterable = collections.abc.Iterable
    collections.Set = collections.abc.Set

