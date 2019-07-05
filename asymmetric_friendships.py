import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    person = record[0]
    friend = record[1]
    mr.emit_intermediate(person, friend)
    mr.emit_intermediate(friend, "?"+person)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    friends = []
    checks = []
    for val in list_of_values:
        if val[0] == "?":
            checks.append(val[1:])
        else:
            friends.append(val)
    for check in checks:
        if check not in friends:
            mr.emit((key,check))
            mr.emit((check,key))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
