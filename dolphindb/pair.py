class Pair(object):

    def __init__(self, a, b):
        if type(a) != type(b):
            raise RuntimeError("data types in pair must be the same")
        self.a = a
        self.b = b
        self.pairlist = [a, b]
        self.type = type(a)

    @classmethod
    def fromlist(cls, alist):
        if len(alist) != 2:
            raise RuntimeError("list/array must have two elements to construct a pair")
        return cls(alist[0], alist[1])

    def __str__(self):
        return str(self.a) + ":" + str(self.b)

    def __repr__(self):
        return "pair(" + str(self.a) + "," + str(self.b) + ")"