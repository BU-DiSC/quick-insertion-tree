import csv
from collections import Counter

from matplotlib import pyplot as plt


def transform_unique(values: list[int]) -> list[int]:
    c = Counter(values)
    max_occur = c.most_common(1)[0][0]  # // 10  # normalize so that max(values) * max_occur < 2^32
    m = min(values)
    c = Counter()
    q = []
    for v in values:
        q.append((v - m) * max_occur + c[v])
        c[v] += 1
    return q


def process(file):
    reader = csv.reader(file)
    values = [int(float(row[0]) * 100) for row in reader]
    plt.clf()
    plt.plot(values)
    plt.savefig('values.png')

    uniques = transform_unique(values)
    print(len(uniques), len(set(uniques)))
    return uniques


def main():
    with open('SPXUSD_close.txt') as file:
        uniques = process(file)
        with open('SPXUSD_close_uniques.txt', 'w') as out:
            for v in uniques:
                print(v, file=out)
        plt.clf()
        plt.plot(uniques)
        plt.savefig('uniques.png')
