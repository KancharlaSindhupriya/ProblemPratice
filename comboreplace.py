from itertools import combinations_with_replacement
s,k=input().split()
k=int(k)
s=sorted(s)
for c in sorted(combinations_with_replacement(s,k)):
    print(''.join(c))
 
