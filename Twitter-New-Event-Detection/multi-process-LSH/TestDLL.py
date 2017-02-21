from ctypes import cdll
lib = cdll.LoadLibrary('C:\\Users\\samera\\Documents\\visual studio 2015\\Projects\\TestDLLSolution\\Debug\\MatrixLib.lib')


class Foo(object):
    def __init__(self):
        self.obj = lib.Foo_new()

    def bar(self):
        lib.Foo_bar(self.obj)


f = Foo()
f.bar() #and you will see "Hello" on the screen


