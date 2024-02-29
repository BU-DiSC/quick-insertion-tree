import csv
import matplotlib.pyplot as plt


def plot(fname):
    with open(fname) as file:
        csv_reader = csv.reader(file)
        keys = []
        mins = []
        maxs = []
        fast = []
        colors = []
        count = 0
        for row in csv_reader:
            if row[0] in ['HARD', 'SOFT']:
                continue
            key = int(row[0])
            min_val = int(row[1] or 0)
            max_val = int(row[2] or 5000001)
            keys.append(key)
            mins.append(min_val)
            maxs.append(max_val)
            if min_val <= key < max_val:
                count += 1
            fast.append(count)
            colors.append('blue' if min_val <= key < max_val else 'red')

        plt.figure(figsize=(12, 6), dpi=200)
        plt.fill_between(range(len(keys)), mins, maxs, color='green', alpha=0.3, label='range')
        plt.scatter(range(len(keys)), keys, color=colors, s=.1, label='key')
        plt.plot(range(len(keys)), fast, color='black', label='fast')
        plt.ylim(0, len(keys))
        plt.legend()
        plt.savefig(fname + '.png')


def plot2(fname):
    with open(fname) as file:
        csv_reader = csv.reader(file)
        keys = []
        fast = []
        for row in csv_reader:
            if len(row) != 2:
                print(row)
                continue
            keys.append(int(row[0]))
            fast.append(int(row[1]))

        plt.figure(figsize=(12, 6), dpi=200)
        plt.scatter(range(len(keys)), keys, s=.1, label='key')
        plt.plot(range(len(fast)), fast, label='fast')
        plt.legend()
        plt.savefig(fname + '.png')


def main():
    for f in ['simple', 'tail', 'lil', 'lol', 'lol_r', 'lol_v', 'lol_vr', 'quit']:
        plot2(f'{f}.csv')


if __name__ == '__main__':
    main()
