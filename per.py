from itertools import permutations
s=input()
k=int(input())
per=sorted(permutations(s,k))
for p in per:
    print(''.join(p))
