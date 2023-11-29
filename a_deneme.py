def main():
    for i in range(-2, -5, -1):
        print(f"{i}")

    a = float(3.0)
    b = float(19)
    c = a / b

    d = float("{:.5f}".format(c))
    e = round(c, 5)
    f = f'{c:.5f}'

    print(d)


main()
