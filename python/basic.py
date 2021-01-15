class Demo:
    def __init__(self):
        self.name = 'test'
        self.extra = {'test': 1}


def main():
    s = Demo()
    print(s.extra[s.name])


if __name__ == '__main__':
    main()
