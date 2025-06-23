class person:
     def getgender(self):
         return "unknown"
class female(person):
    def getgender(self):
        return "female"
class male(person):
     def getgender(self):
        return "male"
f=female()
m=male()
print(f.getgender())
print(m.getgender())
