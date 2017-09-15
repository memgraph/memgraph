import common

expressions = ['1 + 3', '2 - 1', '2 * 5', '5 / 2', '5 % 5', '-5' + '1.4 + 3.3',
               '6.2 - 5.4', '6.5 * 1.2', '6.6 / 1.2', '8.7 % 3.2', '-6.6',
               '"Flo" + "Lasta"', 'true AND false', 'true OR false',
               'true XOR false', 'NOT true', '1 < 2', '2 = 3', '6.66 < 10.2',
               '3.14 = 3.2', '"Ana" < "Ivana"', '"Ana" = "Mmmmm"',
               'Null < Null', 'Null = Null']

print(common.generate(expressions, 30))
