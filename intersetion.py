l1 = [1, 3, 6, 78, 35, 55]
l2 = [12, 24, 35, 24, 88, 120, 155]
s1 = set(l1)
s2 = set(l2)
s1 &= s2
res = list(s1)

print("Intersection:", res)
