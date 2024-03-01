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


def main():
    plot('lol_v.csv')
    plot('quit.csv')


if __name__ == '__main__':
    main()
