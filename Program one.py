def f1(s,k):
    for i in range(0,len(s),k):
        substr=s[i:i+k]
        s1=""
        for char in substr:
            if char not in s1:
                s1 += char
        print(s1)
s,k=input(),int(input())
f1(s,k)
